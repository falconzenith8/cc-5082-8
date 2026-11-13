import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeWorld, collect, foldSpectator } from './helpers.js';

test('join before the delay window filled stalls explicitly, with no data', () => {
  const world = makeWorld({ delay: 5, duration: 30 });
  world.run(2); // clock = 2 - 5 = -3: nothing may be shown yet
  const res = world.service.join({});
  assert.equal(res.kind, 'stall');
  assert.equal(res.reason, 'insufficient_history');
  assert.ok(res.retryAfterMs > 0);
  assert.ok(!('state' in res), 'stall carries no state');
});

test('join returns a starting point at or before the spectator clock, then catches up', () => {
  const world = makeWorld({ delay: 5, duration: 30 });
  world.run(9); // clock = 4
  const join = world.service.join({});
  assert.equal(join.kind, 'snapshot');
  assert.ok(join.t <= 4 + 1e-9, `snapshot t=${join.t} must be <= clock=4`);
  assert.ok(join.t > 0, 'snapshot is a real starting point');
  assert.equal(join.rebuilt, false);
  assert.equal(world.service.sessionState(join.sessionId), 'catching-up');

  // Catching up: the service releases the backlog, then the session goes live
  // at the delay edge — never ahead of it.
  const msgs = collect(world, join.sessionId, 9.5, 0.05);
  assert.equal(world.service.sessionState(join.sessionId), 'live');
  const events = msgs.filter((m) => m.kind === 'events').flatMap((m) => m.events);
  assert.ok(events.length > 0);
  for (const m of msgs) {
    if (m.kind !== 'events') continue;
    for (const ev of m.events) assert.ok(ev.t <= m.clock + 1e-9);
  }
  // The spectator's folded state sits at the delay edge, behind live memory.
  const state = foldSpectator([join, ...msgs]);
  assert.ok(state.t <= world.now - 5 + 1e-9);
  assert.ok(state.t < world.sim.state.t, 'spectator state must lag the live match');
});

test('snapshot is rebuilt from the log, not read from live match memory', () => {
  const world = makeWorld({ delay: 5, duration: 30 });
  world.run(9);
  const join = world.service.join({});
  assert.equal(join.kind, 'snapshot');
  assert.ok(join.state.t <= 4 + 1e-9, 'snapshot state is historical');
  assert.notEqual(join.state.t, world.sim.state.t);
  // Live memory has moved on; the snapshot must not reflect it.
  assert.notDeepEqual(join.state.players, world.sim.state.players);
});

test('score, stats and picture always share one spectator clock', () => {
  const world = makeWorld({ seed: 5, delay: 3, duration: 25 });
  world.run(3.1);
  const join = world.service.join({});
  const msgs = [join, ...collect(world, join.sessionId, 20)];

  // Every score event must be accompanied (same or earlier batch) by the
  // visible event that caused it — the picture can never trail the score.
  const delivered = [];
  for (const m of msgs) {
    if (m.kind !== 'events') continue;
    for (const ev of m.events) {
      if (ev.type === 'score') {
        const causeVisible = delivered.some((e) => e.t >= ev.t - 1e-9 && (
          (ev.reason === 'elimination' && e.type === 'death' && e.t === ev.t)
          || (ev.reason === 'trap' && e.type === 'death' && e.t === ev.t)
          || (ev.reason === 'objective' && e.type === 'objective_captured' && e.t === ev.t)
        )) || m.events.some((e) => (
          (ev.reason === 'objective' && e.type === 'objective_captured' && e.t === ev.t)
          || ((ev.reason === 'elimination' || ev.reason === 'trap') && e.type === 'death' && e.t === ev.t)
        ));
        assert.ok(causeVisible, `score event at t=${ev.t} released before its cause was visible`);
      }
      delivered.push(ev);
    }
  }
  assert.ok(delivered.some((e) => e.type === 'score'), 'match produced score events');

  // A client folding only spectator messages must reproduce exactly the
  // score the events carry — there is no side-channel score feed that could
  // run ahead of the picture.
  const folded = foldSpectator(msgs);
  assert.deepEqual(folded.score, recomputeScore(delivered));
});

function recomputeScore(events) {
  const score = { A: 0, B: 0 };
  for (const ev of events) if (ev.type === 'score') score[ev.team] = ev.total;
  return score;
}

test('buffer underrun stalls explicitly, then rebuilds and resumes cleanly', () => {
  const world = makeWorld({ delay: 2, duration: 30, bufferMax: 5 });
  world.run(6);
  const join = world.service.join({});
  assert.equal(join.kind, 'snapshot');
  collect(world, join.sessionId, 6.5, 0.05); // drain → session goes live
  assert.equal(world.service.sessionState(join.sessionId), 'live');

  world.run(9); // client stops draining; backlog (≈50 events) overflows bufferMax=5
  const msgs = world.service.poll(join.sessionId);
  assert.equal(msgs[0].kind, 'stalled');
  assert.equal(msgs[0].reason, 'buffer_underrun');
  assert.ok(msgs[0].dropped > 5);
  assert.equal(msgs[1].kind, 'snapshot');
  assert.equal(msgs[1].rebuilt, true);
  assert.ok(msgs[1].t <= world.now - 2 + 1e-9);

  // After the rebuild the session catches up again and keeps a consistent clock.
  const after = collect(world, join.sessionId, 10, 0.05);
  assert.equal(world.service.sessionState(join.sessionId), 'live');
  const state = foldSpectator([msgs[1], ...after]);
  assert.ok(state.t <= world.now - 2 + 1e-9);
  assert.ok(state.t > 6, 'spectator state advanced past the rebuild');
});

test('reconnect within retention resumes without gaps or duplicates', () => {
  const world = makeWorld({ delay: 2, duration: 30 });
  world.run(4);
  const join = world.service.join({});
  const first = collect(world, join.sessionId, 6, 0.05);
  const lastSeq = [...first].reverse().find((m) => m.kind === 'events')?.lastSeq ?? join.seq;
  assert.ok(lastSeq > 0);

  const re = world.service.join({ sessionId: join.sessionId, lastSeq });
  assert.equal(re.kind, 'resume');
  assert.equal(re.fromSeq, lastSeq);

  const second = collect(world, join.sessionId, 8, 0.05);
  const seqs = [...first, ...second]
    .filter((m) => m.kind === 'events')
    .flatMap((m) => m.events.map((e) => e.seq));
  for (let i = 1; i < seqs.length; i += 1) {
    assert.ok(seqs[i] > seqs[i - 1], 'events delivered in order, no duplicates');
  }
  const lastSeqs = [...first, ...second].filter((m) => m.kind === 'events').map((m) => m.lastSeq);
  for (let i = 1; i < lastSeqs.length; i += 1) {
    assert.ok(lastSeqs[i] >= lastSeqs[i - 1], 'stream cursor never rewinds');
  }
});

test('reconnect older than retention rebuilds explicitly instead of skipping', () => {
  const world = makeWorld({ delay: 2, duration: 30, maxEvents: 100 });
  world.run(20); // log trimmed; seq 1 long gone
  assert.ok(world.events.firstSeq > 2);
  const res = world.service.join({ sessionId: 'ghost', lastSeq: 1 });
  assert.equal(res.kind, 'snapshot');
  assert.equal(res.rebuilt, true);
  assert.ok(res.t <= world.now - 2 + 1e-9);
});

test('match end reaches spectators exactly through the delay, then the stream closes', () => {
  const world = makeWorld({ delay: 2, duration: 10 });
  world.run(2.1);
  const join = world.service.join({});
  assert.equal(join.kind, 'snapshot');

  const before = collect(world, join.sessionId, 11.9, 0.1);
  assert.ok(before.every((m) => m.kind !== 'end'), 'stream open before delayed end');
  assert.ok(
    before.filter((m) => m.kind === 'events').flatMap((m) => m.events).every((e) => e.type !== 'match_end'),
    'match_end not visible before clock reaches it',
  );

  const endT = world.events.events.find((e) => e.type === 'match_end').t;
  const after = collect(world, join.sessionId, endT + 2.2, 0.1);
  const endMsg = after.find((m) => m.kind === 'end');
  assert.ok(endMsg, 'end message delivered');
  assert.ok(endMsg.clock >= endT, `end delivered at clock=${endMsg.clock} >= ${endT}`);
  assert.ok(endMsg.result.winner, 'end carries the result');
  assert.deepEqual(world.service.poll(join.sessionId), [], 'session closed after end');
});

test('late joiner after the real end still gets the full delayed replay', () => {
  const world = makeWorld({ delay: 2, duration: 10 });
  world.run(15); // match really over; spectator clock also past the end
  const join = world.service.join({});
  assert.equal(join.kind, 'snapshot');
  assert.equal(join.state.phase, 'ended');
  assert.ok(join.state.result.winner);
  const msgs = collect(world, join.sessionId, 16, 0.1);
  assert.ok(msgs.some((m) => m.kind === 'end'), 'replay terminates with end');
});
