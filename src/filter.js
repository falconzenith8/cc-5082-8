// Information-filtering layer. This is where secrecy is enforced — not in
// the UI. Anything stripped here never reaches a spectator socket, so there
// is no client-side "hidden but present" data to leak.

// Events that are secret to everyone except the authority (and, for traps,
// the owning team). They never enter the spectator stream.
const SPECTATOR_SECRET_TYPES = new Set(['trap_armed', 'trap_expired', 'schedule_set']);

// Sanitize one event for a spectator. Returns null when the event must not
// be released at all. The caller still advances the stream cursor past
// dropped events, so sequence continuity is preserved.
export function sanitizeEventForSpectator(ev) {
  if (SPECTATOR_SECRET_TYPES.has(ev.type)) return null;
  if (ev.type === 'positions') {
    // Players under cloak are public knowledge (cloak_start is public) but
    // their positions are secret. Strip them field-level.
    const hidden = new Set(ev.hidden ?? []);
    const moves = {};
    for (const [pid, pos] of Object.entries(ev.moves)) {
      if (!hidden.has(pid)) moves[pid] = pos;
    }
    if (Object.keys(moves).length === 0) return null;
    const { hidden: _dropped, ...rest } = ev;
    return { ...rest, moves };
  }
  return ev;
}

// Sanitize a full state snapshot for a spectator.
export function sanitizeStateForSpectator(state) {
  const out = structuredClone(state);
  out.scheduled = []; // future spawns are authority-only
  if (out.phase !== 'ended') out.result = null;
  for (const [oid, obj] of Object.entries(out.objects)) {
    if (!obj.revealed) delete out.objects[oid];
  }
  return out;
}

// Player-perspective filtering for the demo's live view: a player sees the
// public stream plus their OWN team's secrets, live, with no delay.
export function sanitizeEventForPlayer(ev, team) {
  if (ev.type === 'schedule_set') return null;
  if (ev.type === 'trap_armed' || ev.type === 'trap_expired') {
    return ev.team === team ? ev : null;
  }
  if (ev.type === 'positions') {
    const hidden = new Set(ev.hidden ?? []);
    const moves = {};
    for (const [pid, pos] of Object.entries(ev.moves)) {
      if (!hidden.has(pid) || pid.startsWith(team)) moves[pid] = pos;
    }
    if (Object.keys(moves).length === 0) return null;
    const { hidden: _dropped, ...rest } = ev;
    return { ...rest, moves };
  }
  return ev;
}

export function sanitizeStateForPlayer(state, team) {
  const out = structuredClone(state);
  out.scheduled = [];
  if (out.phase !== 'ended') out.result = null;
  for (const [oid, obj] of Object.entries(out.objects)) {
    if (!obj.revealed && obj.team !== team) delete out.objects[oid];
  }
  return out;
}
