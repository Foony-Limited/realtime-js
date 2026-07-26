/**
 * Presence verification script: checks that two SDK clients on the same channel see each
 * other correctly. Exists to verify reports of presence not working, in particular the
 * simultaneous-attach case where two clients joining at the same instant could each miss
 * the other's enter (and a missed enter never heals, since there is no re-sync).
 *
 * The server echoes your own presence events back (matching Ably), so the check is that
 * each client sees BOTH members, itself included.
 *
 * Each round uses a fresh channel and two fresh connections, in two scenarios:
 *   staggered     alice joins first, then bob. Checks both see both (bob gets alice via
 *                 the snapshot), then that bob's leave reaches alice.
 *   simultaneous  alice and bob subscribe and enter in the same tick, so both arrive via
 *                 live events. This is the known race shape.
 *
 * Run: `REALTIME_KEY="foony.kid_...:sk_..." npm run presence-test` (from the examples/ directory).
 * Env:
 *   REALTIME_KEY      API key in `appSlug.publicKeyId:privateKey` form (required)
 *   ROUNDS            Rounds per scenario (default 5)
 *   TIMEOUT_MS        How long to wait for the expected presence state (default 5000)
 *   CHANNEL_PREFIX    Channel name prefix, needs presence capability (default "demo:presence-test")
 *
 * Exits 0 when every round passes, 1 otherwise.
 */
import {Realtime} from '../../src/index.js';

const apiKey = process.env.REALTIME_KEY;
if (!apiKey) {
  throw new Error('presence-test: set REALTIME_KEY to a "foony.kid_...:sk_..." API key');
}
const rounds = Number(process.env.ROUNDS ?? 5);
const timeoutMs = Number(process.env.TIMEOUT_MS ?? 5000);
const channelPrefix = process.env.CHANNEL_PREFIX ?? 'demo:presence-test';
// Unique per run so reruns never see stale members from an earlier crashed run.
const runId = Math.random().toString(36).slice(2, 8);

type Participant = {
  readonly clientId: string,
  readonly realtime: Realtime,
  /** Live member set as this client sees it, built from its presence events. */
  readonly members: Map<string, unknown>,
  /** Every presence event this client received, for the failure report. */
  readonly log: string[],
  readonly enter: () => Promise<void>,
  readonly leave: () => Promise<void>,
};

/** Connect a fresh client and wire up presence tracking on `channelName`. */
async function join(clientId: string, channelName: string): Promise<Participant> {
  const realtime = new Realtime({clientId, key: apiKey!});
  await realtime.connect();
  const channel = realtime.channels.get(channelName);
  const members = new Map<string, unknown>();
  const log: string[] = [];
  channel.presence.subscribe((event) => {
    log.push(`${event.action} ${event.clientId}`);
    if (event.action === 'leave') {
      members.delete(event.clientId);
    } else {
      members.set(event.clientId, event.data);
    }
  });
  return {
    clientId,
    realtime,
    members,
    log,
    enter: () => channel.presence.enter({name: clientId}),
    leave: () => channel.presence.leave(),
  };
}

/** Poll `check` every 100ms until it holds or `timeoutMs` passes. */
async function waitFor(check: () => boolean): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return check();
}

function seesExactly(participant: Participant, expected: readonly string[]): boolean {
  return participant.members.size === expected.length && expected.every((id) => participant.members.has(id));
}

function reportFailure(label: string, participants: readonly Participant[]): void {
  for (const participant of participants) {
    const seen = [...participant.members.keys()].sort().join(', ') || '(nobody)';
    console.log(`    ${participant.clientId} sees: ${seen} | events: ${participant.log.join(' -> ') || '(none)'}`);
  }
  console.log(`  FAIL ${label}`);
}

/** One round of a scenario on a fresh channel. Returns true when every check passed. */
async function runRound(scenario: 'staggered' | 'simultaneous', round: number): Promise<boolean> {
  const channelName = `${channelPrefix}:${runId}:${scenario}:${round}`;
  const aliceId = `alice-${runId}-${round}`;
  const bobId = `bob-${runId}-${round}`;
  const [alice, bob] = await Promise.all([join(aliceId, channelName), join(bobId, channelName)]);
  try {
    if (scenario === 'staggered') {
      await alice.enter();
      // Alice's own enter echoes back, so waiting for it proves she is settled in.
      await waitFor(() => alice.members.has(aliceId));
      await bob.enter();
    } else {
      // Both enters race in the same tick, the shape that was reported to drop events.
      await Promise.all([alice.enter(), bob.enter()]);
    }

    const bothSeeBoth = await waitFor(
      () => seesExactly(alice, [aliceId, bobId]) && seesExactly(bob, [aliceId, bobId]),
    );
    if (!bothSeeBoth) {
      reportFailure(`${scenario} round ${round}: both members should see [${aliceId}, ${bobId}]`, [alice, bob]);
      return false;
    }

    if (scenario === 'staggered') {
      await bob.leave();
      const leaveSeen = await waitFor(() => seesExactly(alice, [aliceId]));
      if (!leaveSeen) {
        reportFailure(`${scenario} round ${round}: alice should see bob's leave`, [alice]);
        return false;
      }
    }

    console.log(`  PASS ${scenario} round ${round}`);
    return true;
  } finally {
    // Close cleanly so these members leave immediately instead of lingering for minutes.
    await Promise.all([alice.realtime.close(), bob.realtime.close()]);
  }
}

console.log(`presence-test: ${rounds} round(s) per scenario, channel prefix "${channelPrefix}", run ${runId}`);
let failures = 0;
for (const scenario of ['staggered', 'simultaneous'] as const) {
  console.log(`${scenario}:`);
  for (let round = 1; round <= rounds; round++) {
    const passed = await runRound(scenario, round);
    if (!passed) {
      failures++;
    }
  }
}

if (failures > 0) {
  console.log(`presence-test: ${failures} round(s) FAILED`);
  process.exit(1);
}
console.log('presence-test: all rounds passed');
process.exit(0);
