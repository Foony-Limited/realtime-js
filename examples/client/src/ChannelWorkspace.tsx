/**
 * Channel workspace: pick a channel, subscribe to messages (all or by event name), publish, and
 * drive presence (enter / update / leave). Listeners are wired imperatively via button handlers and
 * tracked in refs so React StrictMode's double-render can't double-subscribe.
 */
import {useEffect, useRef, useState} from 'react';
import {
  type Channel,
  type ChannelState,
  type MessageFrame,
  type PresenceEventFrame,
  type Realtime,
  type UnsubscribeFn,
} from '@foony/realtime';
import {describe, Field, latencySuffix, LogView, StateBadge, useLog} from './ui.tsx';

type Member = {readonly clientId: string; readonly connectionId: string; readonly data: unknown};

export function ChannelWorkspace({client}: {client: Realtime}) {
  const [channelName, setChannelName] = useState('demo:lobby');
  const [channel, setChannel] = useState<Channel | null>(null);
  const [channelState, setChannelState] = useState<ChannelState>('initialized');
  const [subscribed, setSubscribed] = useState(false);

  const [subscribeNames, setSubscribeNames] = useState('');
  const [publishName, setPublishName] = useState('chat.message');
  const [publishData, setPublishData] = useState('{\n  "body": "hello world"\n}');
  const [presenceData, setPresenceData] = useState('{\n  "name": "me"\n}');
  const [members, setMembers] = useState<readonly Member[]>([]);

  const messages = useLog();
  const presenceLog = useLog();

  // Unsubscribe handles for the listeners attached to the current channel.
  const stateUnsub = useRef<UnsubscribeFn | null>(null);
  const presenceUnsub = useRef<UnsubscribeFn | null>(null);
  const messageUnsub = useRef<UnsubscribeFn | null>(null);

  // Release the channel (and its listeners) when this component unmounts.
  useEffect(() => () => releaseChannel(), []);

  function useChannel() {
    if (channel) releaseChannel();
    const next = client.channels.get(channelName.trim());

    stateUnsub.current = next.on((change) => {
      setChannelState(change.current);
      messages.append('sys', change.current, `from ${change.previous}${change.resumed ? ' (resumed)' : ''}`);
    });
    presenceUnsub.current = next.presence.subscribe((event) => onPresence(event));

    setChannel(next);
    setChannelState(next.state);
    setSubscribed(false);
    setMembers([]);
    // Eagerly attach so the channel state transitions are visible even before subscribing.
    next.attach().catch((error) => messages.append('err', 'attach', String(error)));
  }

  function releaseChannel() {
    messageUnsub.current?.();
    stateUnsub.current?.();
    presenceUnsub.current?.();
    messageUnsub.current = stateUnsub.current = presenceUnsub.current = null;
    const name = channelName.trim();
    if (name) client.channels.release(name);
    setChannel(null);
    setSubscribed(false);
    setMembers([]);
  }

  function applySubscription() {
    if (!channel) return;
    messageUnsub.current?.();
    const names = subscribeNames.split(',').map((n) => n.trim()).filter(Boolean);
    const listener = (message: MessageFrame) => {
      messages.append('in', message.name, `${message.clientId ?? '?'}: ${describe(message.data)}${latencySuffix(message.data)}`);
    };
    // subscribe(fn) for all messages; subscribe(names[], fn) to filter by event name.
    messageUnsub.current = names.length === 0 ? channel.subscribe(listener) : channel.subscribe(names, listener);
    setSubscribed(true);
    messages.append('sys', 'subscribe', names.length === 0 ? 'all messages' : `names: ${names.join(', ')}`);
  }

  function clearSubscription() {
    messageUnsub.current?.();
    messageUnsub.current = null;
    setSubscribed(false);
    messages.append('sys', 'unsubscribe', 'stopped receiving messages');
  }

  function parsePayload(raw: string): unknown {
    const trimmed = raw.trim();
    if (!trimmed) return undefined;
    return JSON.parse(trimmed);
  }

  async function publish() {
    if (!channel) return;
    try {
      const parsed = parsePayload(publishData);
      // Stamp sentAt on object payloads so subscribers can measure latency via latencySuffix().
      const data =
        parsed && typeof parsed === 'object' && !Array.isArray(parsed)
          ? {...(parsed as Record<string, unknown>), sentAt: Date.now()}
          : parsed;
      await channel.publish(publishName.trim(), data);
      messages.append('out', publishName.trim(), describe(data));
    } catch (error) {
      messages.append('err', 'publish', String(error));
    }
  }

  function onPresence(event: PresenceEventFrame) {
    presenceLog.append(event.action === 'leave' ? 'err' : 'in', event.action, `${event.clientId}: ${describe(event.data)}`);
    setMembers((prev) => {
      const rest = prev.filter((m) => m.connectionId !== event.connectionId);
      if (event.action === 'leave') return rest;
      return [...rest, {clientId: event.clientId, connectionId: event.connectionId, data: event.data}];
    });
  }

  async function presence(action: 'enter' | 'update' | 'leave') {
    if (!channel) return;
    try {
      if (action === 'leave') {
        await channel.presence.leave();
      } else {
        const data = parsePayload(presenceData);
        await channel.presence[action](data);
      }
      presenceLog.append('out', action, action === 'leave' ? '' : presenceData.replace(/\s+/g, ' ').trim());
    } catch (error) {
      presenceLog.append('err', action, String(error));
    }
  }

  return (
    <>
      <section className="panel">
        <div className="spread">
          <h2>Channel</h2>
          {channel ? <StateBadge state={channelState} /> : <span className="meta">no channel</span>}
        </div>
        <div className="row">
          <Field label="Channel name">
            <input value={channelName} onChange={(e) => setChannelName(e.target.value)} disabled={!!channel} />
          </Field>
          {channel ? (
            <button className="secondary" onClick={releaseChannel}>Release</button>
          ) : (
            <button onClick={useChannel} disabled={!channelName.trim()}>Use channel</button>
          )}
        </div>
      </section>

      {channel ? (
        <div className="grid">
          <section className="panel">
            <div className="spread">
              <h2>Messages</h2>
              <span className="meta">{subscribed ? 'subscribed' : 'not subscribed'}</span>
            </div>
            <Field label="Event names to subscribe to (comma-separated; blank = all)">
              <input value={subscribeNames} onChange={(e) => setSubscribeNames(e.target.value)} placeholder="chat.message, chat.system" />
            </Field>
            <div className="row">
              <button onClick={applySubscription}>Subscribe</button>
              <button className="secondary" onClick={clearSubscription} disabled={!subscribed}>Unsubscribe</button>
              <button className="secondary grow" onClick={messages.clear}>Clear</button>
            </div>
            <LogView entries={messages.entries} emptyText="Subscribe, then messages (and channel state) show here." />

            <div className="col">
              <div className="row">
                <Field label="Publish event name">
                  <input value={publishName} onChange={(e) => setPublishName(e.target.value)} />
                </Field>
              </div>
              <Field label="Payload (JSON)">
                <textarea value={publishData} onChange={(e) => setPublishData(e.target.value)} />
              </Field>
              <div className="row">
                <button onClick={() => void publish()} disabled={!publishName.trim()}>Publish</button>
              </div>
            </div>
          </section>

          <section className="panel">
            <div className="spread">
              <h2>Presence</h2>
              <span className="meta">{members.length} member{members.length === 1 ? '' : 's'}</span>
            </div>
            <Field label="Presence data (JSON)">
              <textarea value={presenceData} onChange={(e) => setPresenceData(e.target.value)} />
            </Field>
            <div className="row">
              <button onClick={() => void presence('enter')}>Enter</button>
              <button className="secondary" onClick={() => void presence('update')}>Update</button>
              <button className="secondary" onClick={() => void presence('leave')}>Leave</button>
            </div>
            <div className="members">
              {members.length === 0 ? (
                <span className="log-empty">No members present.</span>
              ) : (
                members.map((m) => (
                  <div className="member" key={m.connectionId}>
                    <span className="who mono">{m.clientId}</span>
                    <span className="meta mono">{describe(m.data)}</span>
                  </div>
                ))
              )}
            </div>
            <LogView entries={presenceLog.entries} emptyText="Presence transitions show here." />
          </section>
        </div>
      ) : null}
    </>
  );
}
