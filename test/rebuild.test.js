import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventStore, SnapshotStore } from '../src/store.js';
import { rebuildAt } from '../src/rebuild.js';
import { makeWorld } from './helpers.js';

test('state rebuilt from events equals live state at the same time', () => {
  const world = makeWorld({ seed: 7, duration: 20 });
  const captures = [];
  // Capture live authoritative states at non-snapshot times (snapshots are
  // taken every 1s; 0.7 offsets guarantee replay across snapshot boundaries).
  for (let i = 1; i <= 10; i += 1) {
    world.run(i * 0.7 + 3);
    captures.push({ t: world.now, state: structuredClone(world.sim.state) });
  }
  for (const { t, state } of captures) {
    const rebuilt = rebuildAt(world.events, world.snapshots, t);
    assert.ok(rebuilt, `rebuild at t=${t}`);
    assert.deepEqual(rebuilt.state, state, `rebuilt state diverges at t=${t}`);
  }
});

test('rebuilds are immutable historical copies, unaffected by later play', () => {
  const world = makeWorld({ seed: 7, duration: 20 });
  world.run(5);
  const first = rebuildAt(world.events, world.snapshots, 5);
  const frozen = structuredClone(first.state);
  world.run(12); // match moves on; live memory changes
  assert.deepEqual(first.state, frozen, 'earlier rebuild was mutated');
  const second = rebuildAt(world.events, world.snapshots, 5);
  assert.deepEqual(second.state, frozen, 'rebuild at same T changed after more play');
  assert.notDeepEqual(second.state, world.sim.state, 'rebuild must not equal current live state');
});

test('rebuild before any usable snapshot returns null (stay, do not guess)', () => {
  const events = new EventStore();
  const snapshots = new SnapshotStore();
  assert.equal(rebuildAt(events, snapshots, 0), null);
  assert.equal(rebuildAt(events, snapshots, -3), null);
});

test('rebuild fails explicitly when retention dropped the needed events', () => {
  const world = makeWorld({ seed: 7, duration: 20, maxEvents: 10, snapshotEvery: 5 });
  world.run(18);
  // Oldest snapshot is at t=5 but the log only retains the last 10 events,
  // so the segment after that snapshot is gone.
  const rebuilt = rebuildAt(world.events, world.snapshots, 6);
  assert.equal(rebuilt, null);
});
