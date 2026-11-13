import { test } from 'node:test';
import assert from 'node:assert/strict';
import { initialState, applyEvent } from '../src/state.js';
import { makeWorld } from './helpers.js';

test('simulator is deterministic for a fixed seed', () => {
  const a = makeWorld({ seed: 99, duration: 15 });
  const b = makeWorld({ seed: 99, duration: 15 });
  a.run(15);
  b.run(15);
  assert.deepEqual(a.events.events, b.events.events);
});

test('simulator drives real movement, abilities, hidden objects and scoring', () => {
  const world = makeWorld({ seed: 3, duration: 30 });
  world.run(30.2);
  const types = world.events.events.map((e) => e.type);
  const count = (t) => types.filter((x) => x === t).length;
  assert.ok(count('positions') > 500, 'continuous movement');
  assert.ok(count('ability') > 0, 'abilities cast');
  assert.ok(count('hit') > 0, 'hits landed');
  assert.ok(count('death') > 0, 'eliminations happened');
  assert.ok(count('score') > 0, 'score changed');
  assert.ok(count('trap_armed') > 0, 'hidden traps placed');
  assert.ok(count('cloak_start') > 0, 'cloaks used');
  assert.ok(count('objective_spawned') > 0, 'objectives spawned on schedule');
  assert.equal(count('match_end'), 1, 'match ended exactly once');
  assert.equal(world.sim.state.phase, 'ended');
});

test('authoritative state is exactly the fold of the event log', () => {
  const world = makeWorld({ seed: 3, duration: 30 });
  world.run(30.2);
  const folded = world.events.events.reduce(applyEvent, initialState());
  assert.deepEqual(folded, world.sim.state);
});
