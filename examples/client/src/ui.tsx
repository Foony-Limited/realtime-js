/** Small shared UI primitives and a rolling log hook used across the playground panels. */
import {useCallback, useRef, useState, type ReactNode} from 'react';

/** A single log line. `kind` drives the colour of the tag. */
export type LogEntry = {
  readonly id: number;
  readonly time: string;
  readonly kind: 'in' | 'out' | 'sys' | 'err';
  readonly tag: string;
  readonly body: string;
};

const MAX_LOG_ENTRIES = 200;

/**
 * Keeps a rolling, capped list of log lines. Returns the entries plus an `append` callback that is
 * stable across renders, so it's safe to use inside SDK listener effects without re-subscribing.
 */
export function useLog() {
  const [entries, setEntries] = useState<readonly LogEntry[]>([]);
  const nextId = useRef(0);
  const append = useCallback((kind: LogEntry['kind'], tag: string, body: string) => {
    const time = new Date().toLocaleTimeString();
    setEntries((prev) => {
      const entry: LogEntry = {id: nextId.current++, time, kind, tag, body};
      const next = [...prev, entry];
      return next.length > MAX_LOG_ENTRIES ? next.slice(next.length - MAX_LOG_ENTRIES) : next;
    });
  }, []);
  const clear = useCallback(() => setEntries([]), []);
  return {entries, append, clear};
}

/** Scrolling, colour-coded view of log entries. */
export function LogView({entries, emptyText}: {entries: readonly LogEntry[]; emptyText: string}) {
  return (
    <div className="log">
      {entries.length === 0 ? (
        <div className="log-empty">{emptyText}</div>
      ) : (
        entries.map((entry) => (
          <div className="log-line" key={entry.id}>
            <span className="ts">{entry.time}</span>
            <span className={`tag ${entry.kind}`}>{entry.tag}</span>
            <span className="body">{entry.body}</span>
          </div>
        ))
      )}
    </div>
  );
}

/** Pill showing a connection/channel state with a colour dot keyed off the state string. */
export function StateBadge({state}: {state: string}) {
  return (
    <span className="badge">
      <span className={`dot ${state}`} />
      {state}
    </span>
  );
}

/** Labelled form field wrapper. */
export function Field({label, children}: {label: string; children: ReactNode}) {
  return (
    <label className="field">
      {label}
      {children}
    </label>
  );
}

/**
 * If `data` carries a numeric `sentAt` (epoch ms), returns a ` (+<delta>ms)` latency suffix using
 * `Math.abs(Date.now() - sentAt)`; otherwise returns ''. Used to eyeball end-to-end message latency.
 */
export function latencySuffix(data: unknown): string {
  const sentAt = (data as {sentAt?: unknown} | null)?.sentAt;
  if (typeof sentAt !== 'number') return '';
  return ` (+${Math.abs(Date.now() - sentAt)}ms)`;
}

/** Best-effort pretty-print of an unknown payload for log/display. */
export function describe(value: unknown): string {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}
