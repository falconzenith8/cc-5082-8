import { test } from 'node:test';
import assert from 'node:assert/strict';
import { rebuildAt } from '../src/rebuild.js';
import { sanitizeStateForSpectator } from '../src/filter.js';
import { makeWorld, makeOptimizedWorld, collect } from './helpers.js';

// One scripted run over joins, a buffer underrun and the match end. The two
// implementations see identical event logs (same seed), so their released
// messages must be identical too — output fields, object visibility and
// scoring moments included.
function dualScenario(world, service) {
  const log = [];
  world.run(3.1);
  const j1 = service.join({});
  log.push(j1);
  world.run(4.2);
  const j2 = service.join({});
  log.push(j2);
  while (world.now < 8) {
    world.run(world.now + 0.1);
    log.push(...service.poll(j2.sessionId));
  }
  log.push(...service.poll(j1.sessionId)); // j1 never drained → underrun rebuild
  while (world.now < 15) {
    world.run(world.now + 0.1);
    log.push(...service.poll(j1.sessionId));
    log.push(...service.poll(j2.sessionId));
  }
  return log;
}

test('optimized matches baseline message-for-message across a full match', () => {
  const b = makeWorld({ seed: 11, delay: 3, duration: 12, bufferMax: 5 });
  const o = makeOptimizedWorld({ seed: 11, delay: 3, duration: 12, bufferMax: 5 });
  let baselineRebuilds = 0;
  const snapshotAt = b.service.snapshotAt.bind(b.service);
  b.service.snapshotAt = (clock) => { baselineRebuilds += 1; return snapshotAt(clock); };

  const lb = dualScenario(b, b.service);
  const lo = dualScenario(o, o.service);
  assert.deepEqual(lo, lb);

  // Same rebuild points, strictly less work recomputed from scratch.
  assert.equal(o.cache.stats.requests, baselineRebuilds);
  assert.ok(o.cache.stats.fullRebuilds < baselineRebuilds,
    `optimized full rebuilds ${o.cache.stats.fullRebuilds} < baseline ${baselineRebuilds}`);
  assert.ok(o.cache.stats.hits + o.cache.stats.incremental > 0, 'cache actually engaged');

  // The savings did not come from a longer delay or a thinned stream.
  assert.equal(o.service.delay, b.service.delay);
  const count = (l) => l.filter((m) => m.kind === 'events').flatMap((m) => m.events).length;
  assert.ok(count(lo) > 0);
  assert.equal(count(lo), count(lb));
});

test('optimized join before the delay window filled stalls like the baseline', () => {
  const world = makeOptimizedWorld({ delay: 5, duration: 30 });
  world.run(2);
  const res = world.service.join({});
  assert.equal(res.kind, 'stall');
  assert.equal(res.reason, 'insufficient_history');
  assert.ok(!('state' in res), 'stall carries no state');
});

test('many spectators joining the same moment share one rebuild', () => {
  const world = makeOptimizedWorld({ seed: 5, delay: 3, duration: 30 });
  world.run(20);
  const joins = [];
  for (let i = 0; i < 8; i += 1) joins.push(world.service.join({}));
  assert.ok(joins.every((j) => j.kind === 'snapshot'));
  for (let i = 1; i < joins.length; i += 1) {
    assert.ok(joins[i].state === joins[0].state, 'same cached entry served to every spectator');
  }
  assert.equal(world.cache.stats.fullRebuilds, 1);
  assert.equal(world.cache.stats.hits, 7);
});

test('sessions on one shared service can watch different delays', () => {
  const world = makeOptimizedWorld({ seed: 5, delay: 3, duration: 30 });
  world.run(15);
  const near = world.service.join({ delay: 3 });
  const far = world.service.join({ delay: 8 });
  assert.ok(far.t < near.t, 'larger delay starts further back');
  const nearEv = [];
  const farEv = [];
  while (world.now < 20) {
    world.run(world.now + 0.1);
    for (const m of world.service.poll(near.sessionId)) if (m.kind === 'events') nearEv.push(...m.events);
    for (const m of world.service.poll(far.sessionId)) if (m.kind === 'events') farEv.push(...m.events);
  }
  assert.ok(nearEv.length > 0 && farEv.length > 0);
  assert.ok(Math.max(...nearEv.map((e) => e.t)) <= world.now - 3 + 1e-9, 'near session respects its delay');
  assert.ok(Math.max(...farEv.map((e) => e.t)) <= world.now - 8 + 1e-9, 'far session respects its delay');
  assert.equal(world.service.sessionState(near.sessionId), 'live');
  assert.equal(world.service.sessionState(far.sessionId), 'live');
});

test('seek previews an earlier moment without moving the follow cursor', () => {
  const world = makeOptimizedWorld({ seed: 5, delay: 3, duration: 30 });
  world.run(10);
  const join = world.service.join({});
  const cursor = world.service.sessions.get(join.sessionId).lastSeq;

  const preview = world.service.seek(join.sessionId, 4);
  assert.equal(preview.kind, 'snapshot');
  assert.ok(preview.preview, 'marked as a preview');
  assert.ok(preview.t <= 4 + 1e-9);
  // The preview equals the baseline formula: full rebuild + public filter.
  const expected = sanitizeStateForSpectator(rebuildAt(world.events, world.snapshots, 4).state);
  assert.deepEqual(preview.state, expected);

  // Seeking past the delay edge clamps to the spectator clock — no future.
  const future = world.service.seek(join.sessionId, 999);
  assert.ok(future.t <= world.now - 3 + 1e-9);

  // The follow stream is undisturbed.
  assert.equal(world.service.sessions.get(join.sessionId).lastSeq, cursor);

  // Seeking before any usable history stalls explicitly.
  const early = world.service.seek(join.sessionId, -100);
  assert.equal(early.kind, 'stall');
  assert.equal(early.reason, 'insufficient_history');
});

test('slow spectators never subscribe to the log or block the match', () => {
  const world = makeOptimizedWorld({ seed: 3, delay: 2, duration: 15, bufferMax: 5 });
  world.run(3);
  const slow = world.service.join({});
  const fast = world.service.join({});
  // Pull-only: the match never calls into spectator code.
  assert.equal(world.events.listeners.size, 0);

  // The slow session never drains; the match and the fast session run on.
  const msgs = collect(world, fast.sessionId, 18, 0.1);
  assert.ok(msgs.some((m) => m.kind === 'end'), 'fast session saw the end');
  assert.equal(world.sim.state.phase, 'ended', 'match completed unaffected');

  // The slow session is then resolved explicitly: stall + rebuild, no skip.
  const late = world.service.poll(slow.sessionId);
  assert.equal(late[0].kind, 'stalled');
  assert.equal(late[0].reason, 'buffer_underrun');
  assert.equal(late[1].kind, 'snapshot');
  assert.equal(late[1].rebuilt, true);
});

test('optimized reconnect older than retention rebuilds explicitly', () => {
  const world = makeOptimizedWorld({ delay: 2, duration: 30, maxEvents: 100 });
  world.run(20);
  assert.ok(world.events.firstSeq > 2);
  const res = world.service.join({ sessionId: 'ghost', lastSeq: 1 });
  assert.equal(res.kind, 'snapshot');
  assert.equal(res.rebuilt, true);
  assert.ok(res.t <= world.now - 2 + 1e-9);
});

test('late joiners after the real end share one cached final state', () => {
  const b = makeWorld({ seed: 5, delay: 2, duration: 10 });
  const o = makeOptimizedWorld({ seed: 5, delay: 2, duration: 10 });
  b.run(15);
  o.run(15);
  const jb = b.service.join({});
  const joins = [o.service.join({}), o.service.join({}), o.service.join({})];
  for (const j of joins) {
    assert.equal(j.kind, 'snapshot');
    assert.equal(j.state.phase, 'ended');
    assert.deepEqual(j.state, jb.state, 'same public state as the baseline');
  }
  assert.equal(o.cache.stats.fullRebuilds, 1);
  assert.equal(o.cache.stats.hits, 2);
  const msgs = collect(o, joins[0].sessionId, 16, 0.1);
  assert.ok(msgs.some((m) => m.kind === 'end'), 'replay terminates with end');
  assert.deepEqual(o.cache.stats.invalidations, { retention_gap: 0, rules_version: 0, capacity: 0 });
});

test('team audiences get their own projection; public sessions never see team secrets', () => {
  const world = makeOptimizedWorld({ seed: 11, delay: 3, duration: 25 });
  world.run(10);
  const pub = world.service.join({ audience: 'public' });
  const insider = world.service.join({ audience: 'team:A' });

  const hidden = Object.values(insider.state.objects).filter((o) => !o.revealed && o.team === 'A');
  assert.ok(hidden.length > 0, 'fixture has hidden own traps in the team view');
  for (const o of hidden) assert.ok(!(o.id in pub.state.objects), 'team trap leaked to public');
  assert.deepEqual(pub.state.scheduled, []);

  const pubEv = [];
  const inEv = [];
  while (world.now < 19) {
    world.run(world.now + 0.1);
    for (const m of world.service.poll(pub.sessionId)) if (m.kind === 'events') pubEv.push(...m.events);
    for (const m of world.service.poll(insider.sessionId)) if (m.kind === 'events') inEv.push(...m.events);
  }
  assert.ok(inEv.some((e) => e.type === 'trap_armed' && e.team === 'A'), 'team session sees own traps armed');
  assert.ok(pubEv.every((e) => e.type !== 'trap_armed'), 'public session never sees trap_armed');
  assert.ok(pubEv.every((e) => e.type !== 'schedule_set'), 'public session never sees the schedule');

  assert.throws(() => world.service.join({ audience: 'team:C' }), /unknown audience/);
});
