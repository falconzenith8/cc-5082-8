import { EventStore, SnapshotStore } from '../src/store.js';
import { Simulator } from '../src/simulator.js';
import { SpectatorService } from '../src/service.js';
import { applyEvent } from '../src/state.js';
import { makeRng } from '../src/rng.js';

// A world wires the simulator, stores and spectator service to one virtual
// clock. Nothing here touches wall-clock time.
export function makeWorld({ seed = 42, delay = 5, duration = 30, bufferMax, maxEvents, snapshotEvery = 1 } = {}) {
  let now = 0;
  const events = new EventStore({ maxEvents });
  const snapshots = new SnapshotStore();
  const sim = new Simulator({ events, snapshots, rng: makeRng(seed), duration, snapshotEvery });
  const service = new SpectatorService({
    events, snapshots, delay, now: () => now,
    ...(bufferMax != null ? { bufferMax } : {}),
  });
  return {
    events, snapshots, sim, service,
    get now() { return now; },
    run(to) {
      while (now < to - 1e-9) {
        now = Math.min(to, now + sim.tick);
        sim.step(now);
      }
    },
  };
}

// Advance the world in small steps, polling the session after each step and
// collecting every message the service releases.
export function collect(world, sessionId, until, step = 0.1) {
  const msgs = [];
  while (world.now < until - 1e-9) {
    world.run(Math.min(until, world.now + step));
    msgs.push(...world.service.poll(sessionId));
  }
  return msgs;
}

// Fold a spectator message stream the way a client would: reset on
// snapshots, apply events otherwise.
export function foldSpectator(msgs) {
  let state = null;
  for (const m of msgs) {
    if (m.kind === 'snapshot') state = structuredClone(m.state);
    else if (m.kind === 'events') for (const ev of m.events) applyEvent(state, ev);
  }
  return state;
}
