import { test } from 'node:test';
import assert from 'node:assert/strict';
import { rebuildAt } from '../src/rebuild.js';
import { sanitizeStateForSpectator, sanitizeStateForPlayer } from '../src/filter.js';
import { makeOptimizedWorld } from './helpers.js';

test('moments covered by the same event prefix share one cache entry', () => {
  const world = makeOptimizedWorld({ seed: 7, duration: 30 });
  world.run(10);
  const r1 = world.cache.stateAt(9.001, 'public');
  const next = world.events.events[r1.seq - world.events.firstSeq + 1];
  assert.ok(next, 'there is a later event');
  const t2 = next.t - 1e-6;
  assert.ok(t2 > 9.001, 't2 maps to the same event prefix');
  const r2 = world.cache.stateAt(t2, 'public');
  assert.ok(r2.state === r1.state, 'same entry object reused, not recomputed');
  assert.equal(world.cache.stats.hits, 1);
  assert.equal(world.cache.stats.fullRebuilds, 1);
});

test('incremental advance from a cached prefix matches the baseline rebuild', () => {
  // Sparse snapshots (every 5s) make the cached prefix strictly more recent
  // than the snapshot floor, so the incremental path is the shorter one.
  const world = makeOptimizedWorld({ seed: 7, duration: 30, snapshotEvery: 5 });
  world.run(10);
  world.cache.stateAt(9, 'public');
  const r = world.cache.stateAt(9.5, 'public');
  assert.equal(world.cache.stats.incremental, 1);
  assert.equal(world.cache.stats.fullRebuilds, 1);
  const expected = sanitizeStateForSpectator(rebuildAt(world.events, world.snapshots, 9.5).state);
  assert.deepEqual(r.state, expected);
  // The advance applied only the events since the cached prefix — fewer
  // than a replay from the snapshot floor would need.
  const fullCost = world.events.seqAt(9.5) - world.snapshots.floor(9.5).seq;
  const incCost = world.events.seqAt(9.5) - world.events.seqAt(9);
  assert.ok(incCost > 0 && incCost < fullCost, `incremental ${incCost} < full ${fullCost}`);
});

test('projections are keyed by information permission and never mix', () => {
  const world = makeOptimizedWorld({ seed: 11, delay: 3, duration: 25 });
  world.run(10);
  const auth = rebuildAt(world.events, world.snapshots, 8);
  const pub = world.cache.stateAt(8, 'public');
  const teamA = world.cache.stateAt(8, 'team:A');

  // Both projections equal the baseline filter applied to the same fold.
  assert.deepEqual(pub.state, sanitizeStateForSpectator(auth.state));
  assert.deepEqual(teamA.state, sanitizeStateForPlayer(auth.state, 'A'));
  assert.notEqual(pub.state, teamA.state, 'separate entries per audience');

  // Team-internal objects stay inside the team projection.
  const hiddenOwn = Object.values(auth.state.objects).filter((o) => !o.revealed && o.team === 'A');
  assert.ok(hiddenOwn.length > 0, 'fixture has an armed hidden team-A trap at t=8');
  for (const o of hiddenOwn) {
    assert.ok(!(o.id in pub.state.objects), `team trap ${o.id} leaked to public`);
    assert.ok(o.id in teamA.state.objects, `own trap ${o.id} missing from team view`);
  }
  assert.deepEqual(pub.state.scheduled, [], 'public projection carries no future schedule');

  // One shared authoritative fold underneath both projections.
  assert.equal(world.cache.stats.fullRebuilds, 1);
  assert.equal(world.cache.stats.projections, 2);
});

test('secret state crosses to public only at its reveal; the cache tracks both sides', () => {
  const world = makeOptimizedWorld({ seed: 11, delay: 3, duration: 25 });
  world.run(30);
  const arms = world.events.events.filter((e) => e.type === 'trap_armed');
  const reveals = world.events.events.filter((e) => e.type === 'trap_revealed');
  // A trap that stayed hidden for a while before being revealed.
  const reveal = reveals.find((r) => r.t - arms.find((a) => a.oid === r.oid).t >= 0.2);
  assert.ok(reveal, 'fixture has a trap revealed well after arming');
  const arm = arms.find((a) => a.oid === reveal.oid);

  // While armed-but-hidden: the authoritative fold holds the secret, the
  // public projection of that same fold does not, the owning team does.
  const midT = (arm.t + reveal.t) / 2;
  const fullMid = world.cache.fullAt(world.events.seqAt(midT), midT, world.snapshots.floor(midT));
  assert.equal(fullMid.state.objects[reveal.oid]?.revealed, false, 'authority holds the armed trap');
  const pubMid = world.cache.stateAt(midT, 'public');
  assert.ok(!(reveal.oid in pubMid.state.objects), 'public projection hides it');
  const teamMid = world.cache.stateAt(midT, `team:${arm.team}`);
  assert.ok(teamMid.state.objects[reveal.oid], 'owning team sees its own trap');

  // Reveal and consume land in the same tick: afterwards the trap is gone
  // from every projection, and the public entry still matches the baseline.
  const after = world.cache.stateAt(reveal.t + 1e-6, 'public');
  assert.ok(!(reveal.oid in after.state.objects), 'consumed trap stays out of later states');
  const baseline = sanitizeStateForSpectator(rebuildAt(world.events, world.snapshots, reveal.t + 1e-6).state);
  assert.deepEqual(after.state, baseline);
});

test('the match result becomes public exactly when the end crosses the clock', () => {
  const world = makeOptimizedWorld({ seed: 5, delay: 2, duration: 10 });
  world.run(20);
  const endT = world.events.events.find((e) => e.type === 'match_end').t;
  const before = world.cache.stateAt(endT - 1e-6, 'public');
  assert.equal(before.state.phase, 'running');
  assert.equal(before.state.result, null, 'result hidden before the end crosses the clock');
  const after = world.cache.stateAt(endT + 1e-6, 'public');
  assert.equal(after.state.phase, 'ended');
  assert.ok(after.state.result.winner, 'result public after the end crosses the clock');
  assert.deepEqual(before.state, sanitizeStateForSpectator(rebuildAt(world.events, world.snapshots, endT - 1e-6).state));
  assert.deepEqual(after.state, sanitizeStateForSpectator(rebuildAt(world.events, world.snapshots, endT + 1e-6).state));
});

test('entries near buffer eviction are served only inside their validity interval', () => {
  const world = makeOptimizedWorld({ seed: 7, delay: 2, duration: 40, maxEvents: 60 });
  world.run(6);
  const first = world.cache.stateAt(5, 'public');
  assert.ok(first);
  const S = first.seq;

  // Walk retention forward one event at a time. While the baseline could
  // still rebuild t=5, the cache serves the same entry; the moment the
  // justifying segment is gone, both say null — the cache never outlives
  // its evidence.
  let sawNull = false;
  while (world.events.firstSeq <= S + 1 && world.now < 30) {
    world.run(world.now + 0.05);
    const r = world.cache.stateAt(5, 'public');
    const snap = world.snapshots.floor(5);
    const baselineAvailable = snap && world.events.firstSeq <= snap.seq + 1;
    if (baselineAvailable) {
      assert.ok(r, 'must serve while the baseline rebuild is possible');
      assert.ok(r.state === first.state, 'served the cached entry, not a rebuild');
      assert.ok(world.cache.full.has(S), 'entry inside its validity interval');
    } else {
      assert.equal(r, null, 'no serving past the validity interval');
      assert.equal(rebuildAt(world.events, world.snapshots, 5), null, 'baseline agrees');
      sawNull = true;
    }
  }
  assert.ok(sawNull, 'retention walked past the snapshot segment');
  // Once firstSeq passed S+1 the entry itself was evicted and counted.
  assert.ok(!world.cache.full.has(S), 'expired entry evicted');
  assert.ok(world.cache.stats.invalidations.retention_gap >= 1);
  assert.ok(world.cache.invalidationLog.some((x) => x.seq === S && x.reason === 'retention_gap'));
});

test('a rules-version bump invalidates every entry', () => {
  const world = makeOptimizedWorld({ seed: 7, duration: 30 });
  world.run(10);
  const a = world.cache.stateAt(9, 'public');
  world.cache.setRulesVersion('rules-v2');
  assert.ok(world.cache.stats.invalidations.rules_version >= 2, 'full + view invalidated');
  assert.equal(world.cache.size().full, 0);
  const b = world.cache.stateAt(9, 'public');
  assert.ok(b.state !== a.state, 'recomputed, not the stale entry');
  assert.deepEqual(b.state, a.state, 'same rules content after recompute');
  assert.equal(world.cache.stats.fullRebuilds, 2);
});

test('capacity evictions are bounded and counted', () => {
  const world = makeOptimizedWorld({ seed: 7, duration: 40, maxEntries: 2 });
  world.run(20);
  world.cache.stateAt(5, 'public');
  world.cache.stateAt(6, 'public');
  world.cache.stateAt(7, 'public');
  assert.equal(world.cache.size().full, 2);
  assert.ok(!world.cache.full.has(world.events.seqAt(5)), 'oldest entry evicted');
  assert.ok(world.cache.stats.invalidations.capacity >= 1);
});

test('after the match ends, every late request hits one immutable final entry', () => {
  const world = makeOptimizedWorld({ seed: 5, delay: 2, duration: 10 });
  world.run(20); // match over at t=10, spectator clock past the end
  const a = world.cache.stateAt(18, 'public');
  const b = world.cache.stateAt(18.5, 'public');
  assert.equal(a.state.phase, 'ended');
  assert.ok(b.state === a.state, 'same final entry serves every late request');
  assert.equal(world.cache.stats.fullRebuilds, 1);
  assert.deepEqual(world.cache.stats.invalidations, { retention_gap: 0, rules_version: 0, capacity: 0 });
});
