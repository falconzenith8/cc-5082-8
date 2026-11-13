import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeWorld, collect } from './helpers.js';

const SECRET_TYPES = new Set(['trap_armed', 'trap_expired', 'schedule_set']);

// Run a full match with a spectator attached from the first possible moment
// and return every message the service released.
function spectateMatch(seed = 11) {
  const world = makeWorld({ seed, delay: 3, duration: 25 });
  world.run(3.1); // let the delay window fill so a starting point exists
  const join = world.service.join({});
  assert.equal(join.kind, 'snapshot');
  const msgs = [join, ...collect(world, join.sessionId, 30)];
  return { world, join, msgs };
}

test('spectator stream contains no secret events and no future data', () => {
  const { msgs } = spectateMatch();
  const events = msgs.filter((m) => m.kind === 'events').flatMap((m) => m.events);
  assert.ok(events.length > 0, 'spectator received events');
  for (const ev of events) {
    assert.ok(!SECRET_TYPES.has(ev.type), `secret event leaked: ${ev.type}`);
  }
  for (const m of msgs) {
    if (m.kind === 'events') {
      for (const ev of m.events) {
        assert.ok(ev.t <= m.clock + 1e-9, `event t=${ev.t} released before its time (clock=${m.clock})`);
      }
    }
    if (m.kind === 'snapshot') {
      assert.deepEqual(m.state.scheduled, [], 'snapshot leaks future objective schedule');
      assert.ok(Object.values(m.state.objects).every((o) => o.revealed), 'snapshot leaks hidden object');
      assert.ok(m.t <= m.clock + 1e-9, 'snapshot is from the future');
    }
    const json = JSON.stringify(m);
    assert.ok(!json.includes('"schedule_set"'), 'schedule event leaked');
    assert.ok(!json.includes('"trap_armed"'), 'trap placement leaked');
  }
});

test('hidden trap stays invisible until its reveal crosses the delay edge', () => {
  const { world, msgs } = spectateMatch();
  const reveals = world.events.events.filter((e) => e.type === 'trap_revealed');
  assert.ok(reveals.length > 0, 'match produced trap reveals');

  const released = msgs.filter((m) => m.kind === 'events').flatMap((m) => m.events);
  for (const reveal of reveals) {
    const before = released.filter((e) => e.t < reveal.t);
    const mentionsBefore = before.some((e) => JSON.stringify(e).includes(reveal.oid));
    assert.ok(!mentionsBefore, `${reveal.oid} visible before its reveal`);
    const sawReveal = released.some((e) => e.type === 'trap_revealed' && e.oid === reveal.oid);
    assert.ok(sawReveal, `${reveal.oid} reveal never reached spectators`);
  }

  // Snapshots taken while a trap was armed-but-hidden must not contain it.
  const armed = world.events.events.filter((e) => e.type === 'trap_armed');
  assert.ok(armed.length > 0, 'match produced hidden traps');
  const snapshots = msgs.filter((m) => m.kind === 'snapshot');
  assert.ok(snapshots.length > 0);
  for (const snap of snapshots) {
    for (const trap of armed) {
      const reveal = reveals.find((r) => r.oid === trap.oid);
      const revealedBy = reveal ? reveal.t : Infinity;
      if (snap.t >= trap.t && snap.t < revealedBy) {
        assert.ok(!(trap.oid in snap.state.objects), `${trap.oid} leaked via snapshot at t=${snap.t}`);
      }
    }
  }
});

test('cloaked players drop out of spectator position updates', () => {
  const { world, msgs } = spectateMatch();
  const cloaks = world.events.events.filter((e) => e.type === 'cloak_start');
  assert.ok(cloaks.length > 0, 'match produced cloaks');
  const released = msgs.filter((m) => m.kind === 'events').flatMap((m) => m.events);
  const positions = released.filter((e) => e.type === 'positions');
  assert.ok(positions.length > 0);
  for (const cloak of cloaks) {
    const during = positions.filter((e) => e.t >= cloak.t && e.t < cloak.until);
    assert.ok(during.length > 0, `positions emitted during cloak of ${cloak.pid}`);
    for (const pos of during) {
      assert.ok(!(cloak.pid in pos.moves), `${cloak.pid} position leaked while cloaked at t=${pos.t}`);
    }
  }
});

test('match result is not visible before the end crosses the delay edge', () => {
  const { world, msgs } = spectateMatch();
  const end = world.events.events.find((e) => e.type === 'match_end');
  assert.ok(end, 'match ended');
  for (const m of msgs) {
    const at = m.clock;
    if (m.kind === 'snapshot' && at < end.t) {
      assert.equal(m.state.result, null, 'result leaked via snapshot');
      assert.equal(m.state.phase, 'running');
    }
    if (m.kind === 'events' && at < end.t) {
      assert.ok(m.events.every((e) => e.type !== 'match_end'), 'match_end released early');
    }
  }
  const sawEnd = msgs.some((m) => m.kind === 'events' && m.events.some((e) => e.type === 'match_end'));
  assert.ok(sawEnd, 'match_end eventually delivered');
  assert.ok(msgs.some((m) => m.kind === 'end'), 'stream closed with end message');
});
