/**
 * REST client for Foony Realtime: request/response access to the same
 * service the WebSocket `Realtime` client talks to. Use it from backends and
 * integrations that publish or read without holding a connection open (cron
 * jobs, serverless functions, webhooks): `new Rest({ key })`, then
 * `rest.channels.get('chat:1').publish('greeting', { hi: true })`.
 *
 * Publishes made here are indistinguishable from WebSocket publishes to
 * subscribers, history, and billing. Channel encryption works the same way:
 * pass the shared `cipher` key to `channels.get` and payloads are encrypted
 * before they leave the process.
 */

import type { Capability } from './auth.js';
import { Cipher, isCipherEncoding, type CipherParams } from './crypto.js';

/** Default Foony Realtime endpoint, shared with the WebSocket client. */
import { DEFAULT_REALTIME_ENDPOINT } from './connection.js';

/** Options for the {@link Rest} client. */
export type RestOptions = {
  /**
   * Service host or absolute http(s) URL. Defaults to `realtime.foony.io`,
   * which resolves to `https://realtime.foony.io`.
   *
   * @defaultValue `'realtime.foony.io'`
   */
  readonly endpoint?: string;
  /**
   * A Realtime API key in `appSlug.publicKeyId:privateKey` form. This is the
   * preferred (and simplest) auth method for server-side callers. The key is
   * a long-lived secret, so keep it server-side and never ship it in browser
   * code.
   */
  readonly key?: string;
  /**
   * A static JWT, sent as a Bearer token. This is the preferred auth method
   * for client-side callers. Mutually exclusive with `authCallback`.
   */
  readonly token?: string;
  /**
   * Async callback that returns a fresh JWT. Called before the first request
   * and again whenever the service reports the current token expired.
   */
  readonly authCallback?: () => Promise<string> | string;
  /**
   * Default clientId stamped on published messages that don't set their own.
   * Only useful with key auth, to attribute a backend's publishes to a user.
   * When omitted, the service attributes each publish to the authenticated
   * identity, so token-auth callers never need to set this: the token's
   * clientId applies automatically, and a differing value is rejected.
   */
  readonly clientId?: string;
  /** Override the fetch implementation. Mostly useful in tests. */
  readonly fetch?: typeof fetch;
};

/** Per-channel options passed to `rest.channels.get(name, options)`. */
export type RestChannelOptions = {
  /**
   * Enable end-to-end payload encryption on this channel with the given {@link CipherParams}.
   * This prevents the Foony backend from seeing the plaintext `data` of messages published to this channel.
   * The `cipher` key should be kept private and never shared with the public or our backend.
   */
  readonly cipher?: CipherParams;
};

/** One message to publish over REST. */
export type RestPublishMessage = {
  /** Application-level event name. */
  readonly name: string;
  /** Arbitrary JSON-serializable payload. */
  readonly data: unknown;
  /**
   * Attribute the message to a clientId. Only useful with key auth, which may
   * name any user. Token auth needs no value here (the token's clientId
   * applies automatically) and anything else is rejected.
   */
  readonly clientId?: string;
  /**
   * Stable id reused across resends so the server can drop duplicates of the
   * same publish. Single-message publishes only.
   */
  readonly id?: string;
  /** Fire-and-forget: delivered live but excluded from history and resume. */
  readonly ephemeral?: boolean;
};

/** Result of a successful publish. */
export type PublishResult = {
  /** Server-assigned (or echoed) message id. An array publish shares one id. */
  readonly messageId: string;
  /** Contiguous per-channel serial for durable publishes. Absent for ephemeral ones. */
  readonly serial?: number;
};

/** One message returned from {@link RestChannel.history}. */
export type RestMessage = {
  /** Message id. Batch members share their batch's id. */
  readonly id: string;
  /** Application-level event name. */
  readonly name?: string;
  /** Payload (decrypted when the channel has a cipher). */
  readonly data?: unknown;
  /** Publish time, ms since epoch. */
  readonly timestamp: number;
  /** Publisher's clientId. */
  readonly clientId?: string;
  /** Remaining payload encoding, e.g. a cipher tag when no cipher is configured. */
  readonly encoding?: string;
  /** Contiguous per-channel serial (absent for unsequenced messages). */
  readonly serial?: number;
};

/** One current member returned from {@link RestPresence.get}. */
export type PresenceMember = {
  /** The member's clientId. */
  readonly clientId: string;
  /** The member's connection id (one clientId may hold several). */
  readonly connectionId: string;
  /** Always `'present'` in a snapshot. */
  readonly action: string;
  /** Presence payload (decrypted when the channel has a cipher). */
  readonly data?: unknown;
  /** Remaining payload encoding when the data could not be decoded. */
  readonly encoding?: string;
  /** When the member last entered or updated, ms since epoch. */
  readonly timestamp: number;
};

/** Query params for {@link RestChannel.history}. */
export type RestHistoryParams = {
  /**
   * Page size. The default is 100.
   *
   * @defaultValue 100
   */
  readonly limit?: number;
  /** Exclusive message-id cursor: return messages published strictly before it. */
  readonly start?: string;
  /**
   * `'backwards'` (newest first, the default) or `'forwards'` (oldest first).
   *
   * @defaultValue `'backwards'`
   */
  readonly direction?: 'backwards' | 'forwards';
};

/** Query params for {@link RestPresence.get}. */
export type RestPresenceParams = {
  /** Cap the number of members returned. */
  readonly limit?: number;
  /** Only members with this clientId. */
  readonly clientId?: string;
  /** Only the member on this connection. */
  readonly connectionId?: string;
};

/** Params for {@link RestAuth.requestToken}. */
export type TokenParams = {
  /** The clientId the token authenticates as. Required. */
  readonly clientId: string;
  /**
   * Token lifetime in ms. Defaults to one hour, and the service caps it at
   * 24 hours.
   *
   * @defaultValue 3600000
   */
  readonly ttl?: number;
  /**
   * Capability to grant. It must be a subset of the key's own capability.
   *
   * @defaultValue The key's own capability.
   */
  readonly capability?: Capability | string;
};

/** A service-issued token plus the metadata needed to cache it until expiry. */
export type TokenDetails = {
  /** The signed JWT to authenticate WebSocket or REST calls with. */
  readonly token: string;
  /** Name of the key that requested it, `appSlug.publicKeyId`. */
  readonly keyName: string;
  /** Issue time, ms since epoch. */
  readonly issued: number;
  /** Expiry time, ms since epoch. */
  readonly expires: number;
  /** The clientId the token authenticates as. */
  readonly clientId: string;
  /** The granted capability as a JSON string. */
  readonly capability: string;
};

/** Error raised for any non-2xx REST response. */
export class RestError extends Error {
  /** Machine-readable protocol code (the same table as `ErrorCode`). */
  readonly code: number;
  /** HTTP status of the response. */
  readonly statusCode: number;

  constructor(message: string, code: number, statusCode: number) {
    super(message);
    this.name = 'RestError';
    this.code = code;
    this.statusCode = statusCode;
  }
}

/**
 * One page of a paginated response. `items` is the current page, and `next()`
 * fetches the following page (older messages for a newest-first history) or
 * resolves null on the last page.
 */
export class PaginatedResult<T> {
  /** The items on this page. */
  readonly items: readonly T[];
  private readonly nextPath: string | null;
  private readonly load: (path: string) => Promise<PaginatedResult<T>>;

  constructor(items: readonly T[], nextPath: string | null, load: (path: string) => Promise<PaginatedResult<T>>) {
    this.items = items;
    this.nextPath = nextPath;
    this.load = load;
  }

  /** True when another page exists. */
  hasNext(): boolean {
    return this.nextPath !== null;
  }

  /** True when this is the final page. */
  isLast(): boolean {
    return this.nextPath === null;
  }

  /**
   * Fetch the next page. Resolves with the page, or with null when this is
   * the last one. Rejects with {@link RestError} when the fetch fails.
   */
  async next(): Promise<PaginatedResult<T> | null> {
    if (this.nextPath === null) {
      return null;
    }
    return this.load(this.nextPath);
  }
}

/**
 * REST client. Construct with an API key (or a token/authCallback) and use
 * `channels.get(name)` for publish, history, and presence reads. Use the
 * WebSocket `Realtime` client instead when you need to receive live messages.
 *
 * @example
 * ```ts
 * // Server-side: an API key is the simplest auth method here.
 * const rest = new Rest({ key: process.env.REALTIME_API_KEY });
 * const channel = rest.channels.get('chat:room-1');
 * await channel.publish('greeting', { text: 'hi' });
 * const history = await channel.history({ limit: 10 });
 * console.log(history.items);
 * ```
 */
export class Rest {
  /** Token minting against the service, authenticated by this client's key. */
  readonly auth: RestAuth;
  private readonly options: RestOptions;
  private readonly baseUrl: string;
  private readonly channelsByName = new Map<string, RestChannel>();
  /** Cached JWT from `authCallback`, replaced when the service reports it expired. */
  private cachedToken: string | null = null;

  /** Map-like accessor for channels. Stable instance per name. */
  readonly channels = {
    /**
     * Get the {@link RestChannel} named `name`, creating it on first use.
     * `options` (e.g. `cipher`) apply when the channel is first created.
     */
    get: (name: string, options?: RestChannelOptions): RestChannel => {
      let existing = this.channelsByName.get(name);
      if (!existing) {
        existing = new RestChannel(this, name, options?.cipher);
        this.channelsByName.set(name, existing);
      }
      return existing;
    },
    /** Remove the channel instance for `name`. A no-op when it doesn't exist. */
    release: (name: string): void => {
      this.channelsByName.delete(name);
    },
  };

  constructor(options: RestOptions | string) {
    this.options = typeof options === 'string' ? { key: options } : options;
    if (!this.options.key && !this.options.token && !this.options.authCallback) {
      throw new Error('Rest: one of key, token, or authCallback is required');
    }
    this.baseUrl = endpointToHttpUrl(this.options.endpoint);
    this.auth = new RestAuth(this);
  }

  /**
   * Fetch the current service time, in ms since the Unix epoch. Useful for
   * measuring clock skew. Rejects with {@link RestError} when the request
   * fails.
   */
  async time(): Promise<number> {
    const body = (await this.request('GET', '/time', undefined, { auth: false })) as { json: unknown };
    const times = body.json as number[];
    if (!Array.isArray(times) || typeof times[0] !== 'number') {
      throw new RestError('malformed /time response', 50000, 500);
    }
    return times[0];
  }

  /** The configured default clientId, if any. */
  get clientId(): string | undefined {
    return this.options.clientId;
  }

  /** The configured API key, used by {@link RestAuth.requestToken}. */
  get key(): string | undefined {
    return this.options.key;
  }

  /**
   * Perform one authenticated request and return the parsed JSON body plus
   * selected headers. Non-2xx responses raise {@link RestError}. When auth
   * came from `authCallback` and the service reports the token invalid or
   * expired, a fresh token is fetched and the request retried once.
   */
  private async request(
    method: string,
    path: string,
    body?: unknown,
    flags?: { auth?: boolean; retried?: boolean },
  ): Promise<{ json: unknown; linkNext: string | null }> {
    const doFetch = this.options.fetch ?? fetch;
    const headers: Record<string, string> = {};
    if (flags?.auth !== false) {
      headers['Authorization'] = await this.authorizationHeader();
    }
    const init: RequestInit = { method, headers };
    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(body);
    }
    const response = await doFetch(this.baseUrl + path, init);
    if (!response.ok) {
      const error = await errorFromResponse(response);
      const tokenProblem = error.statusCode === 401;
      if (tokenProblem && this.options.authCallback && !flags?.retried) {
        this.cachedToken = null;
        return this.request(method, path, body, { ...flags, retried: true });
      }
      throw error;
    }
    const text = await response.text();
    return {
      json: text === '' ? undefined : JSON.parse(text),
      linkNext: parseLinkNext(response.headers.get('Link')),
    };
  }

  /** Resolve the Authorization header for the configured credential. */
  private async authorizationHeader(): Promise<string> {
    if (this.options.key) {
      return 'Basic ' + btoa(this.options.key);
    }
    if (this.options.token) {
      return 'Bearer ' + this.options.token;
    }
    if (!this.cachedToken) {
      this.cachedToken = await this.options.authCallback!();
    }
    return 'Bearer ' + this.cachedToken;
  }
}

/**
 * A channel handle for REST operations: publish, history, and presence.
 * Obtained from `rest.channels.get(name)`. It holds no server-side state.
 */
export class RestChannel {
  /** The channel name this instance is bound to. */
  readonly name: string;
  /** Presence reads for this channel. */
  readonly presence: RestPresence;
  private readonly rest: Rest;
  private readonly cipher: Cipher | undefined;

  constructor(rest: Rest, name: string, cipherParams?: CipherParams) {
    this.rest = rest;
    this.name = name;
    this.cipher = cipherParams ? new Cipher(cipherParams) : undefined;
    this.presence = new RestPresence(rest, name, this.cipher);
  }

  /**
   * Publish one message from an event name plus payload. On a channel with a
   * `cipher`, the payload is end-to-end encrypted before it is sent.
   * Resolves with the {@link PublishResult} once the service has accepted
   * the message durably. Rejects with {@link RestError} when the request
   * fails, for example a key without the publish capability.
   */
  async publish(name: string, data: unknown): Promise<PublishResult>;
  /** Publish one {@link RestPublishMessage}, which can also set `clientId`, `id`, or `ephemeral`. */
  async publish(message: RestPublishMessage): Promise<PublishResult>;
  /** Publish an array of messages, stored and delivered as one atomic batch under one id. */
  async publish(messages: readonly RestPublishMessage[]): Promise<PublishResult>;
  async publish(
    first: string | RestPublishMessage | readonly RestPublishMessage[],
    data?: unknown,
  ): Promise<PublishResult> {
    const messages = typeof first === 'string' ? [{ name: first, data }] : Array.isArray(first) ? first : [first as RestPublishMessage];
    const encoded = await Promise.all(messages.map((message) => this.encodeMessage(message)));
    const body = typeof first === 'string' || !Array.isArray(first) ? encoded[0] : encoded;
    const { json } = await this.rest['request']('POST', `/channels/${encodeURIComponent(this.name)}/messages`, body);
    const result = json as { messageId: string; serial?: number };
    return result.serial === undefined ? { messageId: result.messageId } : { messageId: result.messageId, serial: result.serial };
  }

  /**
   * Read the channel's message history, newest first by default. Batch
   * publishes come back as one item per message, sharing the batch's id and
   * serial. On a channel with a `cipher`, messages are decrypted before they
   * are returned. How far back history reaches depends on each message's
   * retention, see the [history docs](https://foony.io/docs/history).
   * Resolves with one page. Page through older messages with
   * `result.next()`. Rejects with {@link RestError} when history cannot be
   * read.
   */
  async history(params?: RestHistoryParams): Promise<PaginatedResult<RestMessage>> {
    const query = new URLSearchParams();
    if (params?.limit !== undefined) query.set('limit', String(params.limit));
    if (params?.start !== undefined) query.set('start', params.start);
    if (params?.direction !== undefined) query.set('direction', params.direction);
    const queryString = query.toString();
    const suffix = queryString === '' ? '' : `?${queryString}`;
    return this.historyPage(`/channels/${encodeURIComponent(this.name)}/messages${suffix}`);
  }

  /** Load one history page and wire up `next()` to load the following one. */
  private async historyPage(path: string): Promise<PaginatedResult<RestMessage>> {
    const { json, linkNext } = await this.rest['request']('GET', path);
    const raw = (json ?? []) as RestMessage[];
    const items = await Promise.all(raw.map((item) => this.decodeMessage(item)));
    return new PaginatedResult(items, linkNext, (nextPath) => this.historyPage(nextPath));
  }

  /** Apply the channel cipher (when configured) to one outgoing message. */
  private async encodeMessage(message: RestPublishMessage): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { name: message.name, data: message.data };
    const clientId = message.clientId ?? this.rest.clientId;
    if (clientId !== undefined) body['clientId'] = clientId;
    if (message.id !== undefined) body['id'] = message.id;
    if (message.ephemeral !== undefined) body['ephemeral'] = message.ephemeral;
    if (this.cipher) {
      const encrypted = await this.cipher.encrypt(message.data);
      body['data'] = encrypted.data;
      body['encoding'] = encrypted.encoding;
    }
    return body;
  }

  /** Decrypt one history item when the channel cipher can read it. */
  private async decodeMessage(item: RestMessage): Promise<RestMessage> {
    if (!this.cipher || !isCipherEncoding(item.encoding)) {
      return item;
    }
    try {
      const data = await this.cipher.decrypt(item.encoding, item.data);
      const { encoding: _consumed, ...decoded } = item;
      return { ...decoded, data };
    } catch {
      // Undecryptable (a rotated key, another key's publish): return the item
      // undecoded with `encoding` intact rather than failing the whole page.
      return item;
    }
  }
}

/** Presence reads for one channel, from `channel.presence`. */
export class RestPresence {
  private readonly rest: Rest;
  private readonly channel: string;
  private readonly cipher: Cipher | undefined;

  constructor(rest: Rest, channel: string, cipher: Cipher | undefined) {
    this.rest = rest;
    this.channel = channel;
    this.cipher = cipher;
  }

  /**
   * Fetch the channel's current members. The snapshot is complete (presence
   * sets are bounded), so the result is a single page. Resolves with the
   * members, decrypting their `data` when the channel has a `cipher`.
   * Rejects with {@link RestError} when the request fails.
   */
  async get(params?: RestPresenceParams): Promise<PaginatedResult<PresenceMember>> {
    const query = new URLSearchParams();
    if (params?.limit !== undefined) query.set('limit', String(params.limit));
    if (params?.clientId !== undefined) query.set('clientId', params.clientId);
    if (params?.connectionId !== undefined) query.set('connectionId', params.connectionId);
    const queryString = query.toString();
    const suffix = queryString === '' ? '' : `?${queryString}`;
    const { json } = await this.rest['request']('GET', `/channels/${encodeURIComponent(this.channel)}/presence${suffix}`);
    const raw = (json ?? []) as PresenceMember[];
    const members = await Promise.all(raw.map((member) => this.decodeMember(member)));
    return new PaginatedResult(members, null, async () => {
      throw new RestError('presence snapshots are a single page', 40001, 400);
    });
  }

  /** Decrypt one member's data when the channel cipher can read it. */
  private async decodeMember(member: PresenceMember): Promise<PresenceMember> {
    if (!this.cipher || !isCipherEncoding(member.encoding)) {
      return member;
    }
    try {
      const data = await this.cipher.decrypt(member.encoding, member.data);
      const { encoding: _consumed, ...decoded } = member;
      return { ...decoded, data };
    } catch {
      // Undecryptable (a rotated key, another key's entry): return the member
      // undecoded with `encoding` intact rather than failing the whole snapshot.
      return member;
    }
  }
}

/** Token minting against the service, from `rest.auth`. */
export class RestAuth {
  private readonly rest: Rest;

  constructor(rest: Rest) {
    this.rest = rest;
  }

  /**
   * Ask the service to mint a client JWT from this client's API key. The
   * granted capability must be a subset of the key's own. Resolves with the
   * {@link TokenDetails}, whose `expires` lets callers cache the token.
   * Throws when this client has no `key`, and rejects with {@link RestError}
   * when the service refuses, for example a capability outside the key's
   * grant. See the [auth docs](https://foony.io/docs/auth) for the full
   * token flow.
   */
  async requestToken(params: TokenParams): Promise<TokenDetails> {
    const key = this.rest.key;
    if (!key) {
      throw new Error('Rest.auth.requestToken: an API key is required');
    }
    if (key.indexOf(':') <= 0) {
      // Without this, a colon-less key would silently build a mangled URL and
      // surface as a confusing 401 from the service.
      throw new Error('Rest.auth.requestToken: malformed API key (expected "appSlug.publicKeyId:privateKey")');
    }
    const keyName = key.slice(0, key.indexOf(':'));
    const body: Record<string, unknown> = { clientId: params.clientId };
    if (params.ttl !== undefined) body['ttl'] = params.ttl;
    if (params.capability !== undefined) body['capability'] = params.capability;
    const { json } = await this.rest['request'](
      'POST',
      `/keys/${encodeURIComponent(keyName)}/requestToken`,
      body,
    );
    return json as TokenDetails;
  }
}

/** Resolve an endpoint (bare host or absolute URL) to the REST base URL. */
function endpointToHttpUrl(endpoint = DEFAULT_REALTIME_ENDPOINT): string {
  if (/^https?:\/\//u.test(endpoint)) {
    return endpoint.replace(/\/$/u, '');
  }
  return `https://${endpoint}`;
}

/** Extract the rel="next" target from a Link response header, if present. */
function parseLinkNext(header: string | null): string | null {
  if (!header) {
    return null;
  }
  for (const part of header.split(',')) {
    const match = /<([^>]+)>\s*;\s*rel="next"/u.exec(part);
    if (match) {
      return match[1] ?? null;
    }
  }
  return null;
}

/** Build a {@link RestError} from a non-2xx response, tolerating non-JSON bodies. */
async function errorFromResponse(response: Response): Promise<RestError> {
  const fallback = new RestError(`request failed with status ${response.status}`, 50000, response.status);
  try {
    const parsed = (await response.json()) as { error?: { message?: string; code?: number; statusCode?: number } };
    if (parsed.error?.message === undefined || parsed.error.code === undefined) {
      return fallback;
    }
    return new RestError(parsed.error.message, parsed.error.code, parsed.error.statusCode ?? response.status);
  } catch {
    return fallback;
  }
}
