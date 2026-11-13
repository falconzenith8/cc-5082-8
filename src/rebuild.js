import { applyEvent } from './state.js';

// Rebuild the authoritative state exactly at time T from a stored snapshot
// plus the event log. This is the ONLY way the spectator service produces
// historical state — it never reads the live match memory.
//
// Returns null when the data needed for an exact rebuild is not available
// (no snapshot old enough, or the event segment was dropped by retention).
// Callers must treat null as "stay / wait", never guess.
export function rebuildAt(events, snapshots, T) {
  const snap = snapshots.floor(T);
  if (!snap) return null;
  if (events.firstSeq > snap.seq + 1) return null; // event gap: cannot rebuild exactly
  const state = structuredClone(snap.state);
  let seq = snap.seq;
  let t = snap.t;
  for (const ev of events.range(snap.seq, T)) {
    applyEvent(state, ev);
    seq = ev.seq;
    t = ev.t;
  }
  return { state, t, seq };
}
