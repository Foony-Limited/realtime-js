/**
 * Tests for the REST client against an in-process fake HTTP server. The fake
 * records every request (method, path, headers, body) and serves canned or
 * stateful responses, mirroring the real service's shapes: publish returns
 * `{channel, messageId, serial}`, history returns an array plus a
 * `Link: <...>; rel="next"` header, errors use `{"error": {...}}`.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { afterEach, describe, expect, it } from 'vitest';

import { generateRandomKey } from './crypto.js';
import { Rest, RestError } from './rest.js';

type RecordedRequest = {
  method: string;
  path: string;
  authorization: string | undefined;
  body: unknown;
};

type RouteHandler = (request: RecordedRequest, response: ServerResponse) => void;

/** Minimal fake REST service: records requests, dispatches to a handler. */
class FakeRestServer {
  readonly requests: RecordedRequest[] = [];
  handler: RouteHandler = (_request, response) => {
    response.writeHead(404).end();
  };
  private server: Server | null = null;

  async start(): Promise<string> {
    this.server = createServer((incoming, response) => {
      void this.dispatch(incoming, response);
    });
    await new Promise<void>((resolve) => this.server!.listen(0, '127.0.0.1', resolve));
    const address = this.server.address();
    if (address === null || typeof address === 'string') throw new Error('no port');
    return `http://127.0.0.1:${address.port}`;
  }

  async stop(): Promise<void> {
    if (!this.server) return;
    await new Promise<void>((resolve, reject) => this.server!.close((error) => (error ? reject(error) : resolve())));
    this.server = null;
  }

  private async dispatch(incoming: IncomingMessage, response: ServerResponse): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming) {
      chunks.push(chunk as Buffer);
    }
    const text = Buffer.concat(chunks).toString();
    const recorded: RecordedRequest = {
      method: incoming.method ?? '',
      path: incoming.url ?? '',
      authorization: incoming.headers.authorization,
      body: text === '' ? undefined : JSON.parse(text),
    };
    this.requests.push(recorded);
    this.handler(recorded, response);
  }
}

function json(response: ServerResponse, status: number, body: unknown, headers?: Record<string, string>): void {
  response.writeHead(status, { 'Content-Type': 'application/json', ...headers });
  response.end(JSON.stringify(body));
}

function restError(response: ServerResponse, status: number, code: number, message: string): void {
  json(response, status, { error: { message, code, statusCode: status } });
}

const fakes: FakeRestServer[] = [];

async function startFake(): Promise<{ fake: FakeRestServer; endpoint: string }> {
  const fake = new FakeRestServer();
  const endpoint = await fake.start();
  fakes.push(fake);
  return { fake, endpoint };
}

afterEach(async () => {
  await Promise.all(fakes.splice(0).map((fake) => fake.stop()));
});

describe('Rest publish', () => {
  it('publishes name+data with Basic key auth and returns the result', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => json(response, 201, { channel: 'chat:1', messageId: 'm-1', serial: 7 });

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const result = await rest.channels.get('chat:1').publish('greeting', { hi: true });

    expect(result).toEqual({ messageId: 'm-1', serial: 7 });
    const request = fake.requests[0]!;
    expect(request.method).toBe('POST');
    expect(request.path).toBe('/channels/chat%3A1/messages');
    expect(request.authorization).toBe('Basic ' + btoa('myapp.key1:s3cret'));
    expect(request.body).toEqual({ name: 'greeting', data: { hi: true } });
  });

  it('publishes a message array as one body array', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => json(response, 201, { channel: 'chat:1', messageId: 'm-2', serial: 8 });

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    await rest.channels.get('chat:1').publish([
      { name: 'a', data: 1 },
      { name: 'b', data: 2 },
    ]);

    expect(fake.requests[0]!.body).toEqual([
      { name: 'a', data: 1 },
      { name: 'b', data: 2 },
    ]);
  });

  it('stamps the client-level clientId unless the message names its own', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => json(response, 201, { channel: 'c', messageId: 'm', serial: 1 });

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret', clientId: 'user-1' });
    const channel = rest.channels.get('c');
    await channel.publish('a', 1);
    await channel.publish({ name: 'b', data: 2, clientId: 'user-2' });

    expect((fake.requests[0]!.body as { clientId?: string }).clientId).toBe('user-1');
    expect((fake.requests[1]!.body as { clientId?: string }).clientId).toBe('user-2');
  });

  it('passes id and ephemeral through', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => json(response, 201, { channel: 'c', messageId: 'my-id' });

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const result = await rest.channels.get('c').publish({ name: 'typing', data: true, id: 'my-id', ephemeral: true });

    expect(result.serial).toBeUndefined();
    expect(fake.requests[0]!.body).toEqual({ name: 'typing', data: true, id: 'my-id', ephemeral: true });
  });
});

describe('Rest history', () => {
  it('maps items and follows the Link next header for older pages', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (request, response) => {
      if (!request.path.includes('start=')) {
        json(response, 200, [{ id: 'm-3', name: 'x', data: 3, timestamp: 30, serial: 3 }], {
          Link: '</channels/c/messages?limit=1&start=m-3>; rel="next"',
        });
        return;
      }
      json(response, 200, [{ id: 'm-2', name: 'x', data: 2, timestamp: 20, serial: 2 }]);
    };

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const page = await rest.channels.get('c').history({ limit: 1 });

    expect(page.items.map((item) => item.id)).toEqual(['m-3']);
    expect(page.hasNext()).toBe(true);
    expect(page.isLast()).toBe(false);

    const older = await page.next();
    expect(older).not.toBeNull();
    expect(older!.items.map((item) => item.id)).toEqual(['m-2']);
    expect(older!.hasNext()).toBe(false);
    expect(older!.isLast()).toBe(true);
    expect(await older!.next()).toBeNull();

    expect(fake.requests[0]!.path).toBe('/channels/c/messages?limit=1');
    expect(fake.requests[1]!.path).toBe('/channels/c/messages?limit=1&start=m-3');
  });

  it('passes direction and start params through', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => json(response, 200, []);

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    await rest.channels.get('c').history({ direction: 'forwards', start: 'm-9', limit: 5 });

    expect(fake.requests[0]!.path).toBe('/channels/c/messages?limit=5&start=m-9&direction=forwards');
  });
});

describe('Rest presence', () => {
  it('returns the member snapshot and passes filters', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) =>
      json(response, 200, [
        { clientId: 'alice', connectionId: 'conn-1', action: 'present', data: { seat: 1 }, timestamp: 10 },
      ]);

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const page = await rest.channels.get('c').presence.get({ clientId: 'alice', limit: 10 });

    expect(page.items).toEqual([
      { clientId: 'alice', connectionId: 'conn-1', action: 'present', data: { seat: 1 }, timestamp: 10 },
    ]);
    expect(page.isLast()).toBe(true);
    expect(fake.requests[0]!.path).toBe('/channels/c/presence?limit=10&clientId=alice');
  });
});

describe('Rest auth', () => {
  it('requests a token from the key endpoint with Basic auth', async () => {
    const { fake, endpoint } = await startFake();
    const details = {
      token: 'jwt-here',
      keyName: 'myapp.key1',
      issued: 1000,
      expires: 4600000,
      clientId: 'user-1',
      capability: '{"chat:*":["subscribe"]}',
    };
    fake.handler = (_request, response) => json(response, 200, details);

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const token = await rest.auth.requestToken({
      clientId: 'user-1',
      ttl: 3_600_000,
      capability: { 'chat:*': ['subscribe'] },
    });

    expect(token).toEqual(details);
    const request = fake.requests[0]!;
    expect(request.path).toBe('/keys/myapp.key1/requestToken');
    expect(request.authorization).toBe('Basic ' + btoa('myapp.key1:s3cret'));
    expect(request.body).toEqual({ clientId: 'user-1', ttl: 3_600_000, capability: { 'chat:*': ['subscribe'] } });
  });

  it('rejects requestToken without an API key', async () => {
    const { endpoint } = await startFake();
    const rest = new Rest({ endpoint, token: 'some-jwt' });
    await expect(rest.auth.requestToken({ clientId: 'u' })).rejects.toThrow(/API key is required/u);
  });
});

describe('Rest time and errors', () => {
  it('returns the server time without auth', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => json(response, 200, [1719772800000]);

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    expect(await rest.time()).toBe(1719772800000);
    expect(fake.requests[0]!.authorization).toBeUndefined();
  });

  it('surfaces error bodies as RestError with code and statusCode', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => restError(response, 403, 40301, 'capability does not permit publish on this channel');

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const failure = await rest.channels.get('c').publish('a', 1).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(RestError);
    expect((failure as RestError).code).toBe(40301);
    expect((failure as RestError).statusCode).toBe(403);
    expect((failure as RestError).message).toMatch(/capability/u);
  });

  it('requires some credential', () => {
    expect(() => new Rest({ endpoint: 'http://127.0.0.1:1' })).toThrow(/key, token, or authCallback/u);
  });
});

describe('Rest authCallback', () => {
  it('sends the callback token as Bearer and refreshes once on 401', async () => {
    const { fake, endpoint } = await startFake();
    let served = 0;
    fake.handler = (request, response) => {
      served++;
      if (request.authorization === 'Bearer stale') {
        restError(response, 401, 40102, 'token expired');
        return;
      }
      json(response, 201, { channel: 'c', messageId: 'm', serial: 1 });
    };

    let minted = 0;
    const rest = new Rest({
      endpoint,
      authCallback: () => {
        minted++;
        return minted === 1 ? 'stale' : 'fresh';
      },
    });
    const result = await rest.channels.get('c').publish('a', 1);

    expect(result.messageId).toBe('m');
    expect(minted).toBe(2);
    expect(served).toBe(2);
    expect(fake.requests[1]!.authorization).toBe('Bearer fresh');
  });

  it('does not loop when the refreshed token is also rejected', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) => restError(response, 401, 40101, 'nope');

    const rest = new Rest({ endpoint, authCallback: () => 'always-bad' });
    const failure = await rest.channels.get('c').publish('a', 1).catch((error: unknown) => error);

    expect((failure as RestError).statusCode).toBe(401);
    expect(fake.requests.length).toBe(2);
  });
});

describe('Rest channel encryption', () => {
  it('encrypts publishes and decrypts history with the channel cipher', async () => {
    const { fake, endpoint } = await startFake();
    const stored: unknown[] = [];
    fake.handler = (request, response) => {
      if (request.method === 'POST') {
        stored.push(request.body);
        json(response, 201, { channel: 'c', messageId: 'm-1', serial: 1 });
        return;
      }
      const record = stored[0] as { name: string; data: string; encoding: string };
      json(response, 200, [
        { id: 'm-1', name: record.name, data: record.data, encoding: record.encoding, timestamp: 5, serial: 1 },
      ]);
    };

    const cipherKey = await generateRandomKey(256);
    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const channel = rest.channels.get('c', { cipher: { key: cipherKey } });

    await channel.publish('secret', { launch: 'codes' });
    const sent = fake.requests[0]!.body as { data: unknown; encoding?: string };
    expect(sent.encoding).toBe('cipher+aes-256-gcm/base64');
    expect(typeof sent.data).toBe('string');
    expect(sent.data).not.toContain('launch');

    const page = await channel.history();
    expect(page.items[0]!.data).toEqual({ launch: 'codes' });
    expect(page.items[0]!.encoding).toBeUndefined();
  });

  it('leaves encrypted history untouched when no cipher is configured', async () => {
    const { fake, endpoint } = await startFake();
    fake.handler = (_request, response) =>
      json(response, 200, [{ id: 'm-1', name: 'x', data: 'AAAA', encoding: 'cipher+aes-256-gcm/base64', timestamp: 5 }]);

    const rest = new Rest({ endpoint, key: 'myapp.key1:s3cret' });
    const page = await rest.channels.get('c').history();
    expect(page.items[0]!.encoding).toBe('cipher+aes-256-gcm/base64');
    expect(page.items[0]!.data).toBe('AAAA');
  });
});
