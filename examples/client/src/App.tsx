/**
 * Top-level playground UI. Owns the `Realtime` client and its connection lifecycle, and renders the
 * channel workspace once connected. Authenticates with a raw API key (`key`) against the prod edge.
 */
import {useEffect, useRef, useState} from 'react';
import {Realtime, type ConnectionState} from '@foony/realtime';
import {ChannelWorkspace} from './ChannelWorkspace.tsx';
import {Field, StateBadge, useLog, LogView} from './ui.tsx';

export function App() {
  const [apiKey, setApiKey] = useState('');
  const [clientId, setClientId] = useState(() => `web-${Math.random().toString(36).slice(2, 7)}`);

  const [client, setClient] = useState<Realtime | null>(null);
  const [state, setState] = useState<ConnectionState>('initialized');
  const [connectionId, setConnectionId] = useState<string | null>(null);
  const [confirmedClientId, setConfirmedClientId] = useState<string | null>(null);
  const clientRef = useRef<Realtime | null>(null);
  const {entries, append, clear} = useLog();

  // Close the socket if the component unmounts while connected.
  useEffect(() => () => void clientRef.current?.close(), []);

  function connect() {
    if (clientRef.current) return;
    // No endpoint configured → the SDK connects to the prod default (wss://realtime.foony.com).
    const realtime = new Realtime({clientId, key: apiKey.trim()});
    realtime.connection.on((next, reason) => {
      setState(next);
      setConnectionId(realtime.getConnectionId());
      setConfirmedClientId(realtime.getClientId());
      append(next === 'failed' || next === 'disconnected' ? 'err' : 'sys', next, reason ? reason.message : '');
    });
    clientRef.current = realtime;
    setClient(realtime);
    append('sys', 'connect', 'key auth → wss://realtime.foony.com');
    realtime.connect().catch((error) => append('err', 'connect', String(error)));
  }

  async function disconnect() {
    const realtime = clientRef.current;
    if (!realtime) return;
    await realtime.close();
    clientRef.current = null;
    setClient(null);
    setConnectionId(null);
    setConfirmedClientId(null);
    append('sys', 'close', 'connection closed');
  }

  const isConnecting = state === 'connecting';
  return (
    <div className="app">
      <header className="app-header">
        <h1>@foony/realtime playground</h1>
        <p>Connect, subscribe, publish, and drive presence against the realtime edge.</p>
      </header>

      <section className="panel">
        <div className="spread">
          <h2>Connection</h2>
          <StateBadge state={state} />
        </div>

        <div className="row">
          <Field label="Client id">
            <input value={clientId} onChange={(e) => setClientId(e.target.value)} disabled={!!client} />
          </Field>
          <Field label="API key (appSlug.publicKeyId:privateKey)">
            <input value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="foony.kid_...:sk_..." disabled={!!client} />
          </Field>
        </div>

        <div className="row">
          {client ? (
            <button className="secondary" onClick={() => void disconnect()}>Disconnect</button>
          ) : (
            <button onClick={connect} disabled={isConnecting || !apiKey.trim()}>Connect</button>
          )}
          <span className="meta mono">connectionId: {connectionId ?? '—'}</span>
          <span className="meta mono">clientId: {confirmedClientId ?? '—'}</span>
          <button className="secondary" onClick={clear}>Clear log</button>
        </div>

        <LogView entries={entries} emptyText="Connection events appear here." />
      </section>

      {client ? <ChannelWorkspace client={client} /> : null}
    </div>
  );
}
