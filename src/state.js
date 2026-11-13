// Pure domain state. The authoritative match state is a pure fold of the
// event log: applyEvent(initialState(), ...events). The same reducer is used
// by the live match, the spectator rebuilder and the browser client, so a
// state rebuilt from an event prefix can never diverge from the live state
// at that same time.

export function initialState() {
  return {
    t: 0,
    phase: 'running', // 'running' | 'ended'
    result: null, // { winner, score } — only set by match_end
    players: {}, // pid -> { id, team, x, y, hp, alive, respawnAt, invisibleUntil }
    objects: {}, // oid -> { id, kind, team, x, y, revealed, armedAt }
    objectives: {}, // oid -> { id, x, y, active, capturedBy }
    score: { A: 0, B: 0 },
    stats: {}, // pid -> { kills, deaths, damage }
    scheduled: [], // authority-only future objective spawns — must never leak
  };
}

export function applyEvent(state, ev) {
  if (ev.t != null) state.t = ev.t;
  switch (ev.type) {
    case 'player_join':
      state.players[ev.pid] = {
        id: ev.pid, team: ev.team, x: ev.x, y: ev.y,
        hp: 100, alive: true, respawnAt: 0, invisibleUntil: 0,
      };
      state.stats[ev.pid] = { kills: 0, deaths: 0, damage: 0 };
      break;
    case 'positions':
      for (const [pid, pos] of Object.entries(ev.moves)) {
        const p = state.players[pid];
        if (p) { p.x = pos[0]; p.y = pos[1]; }
      }
      break;
    case 'hit': {
      const target = state.players[ev.target];
      if (target) target.hp = Math.max(0, target.hp - ev.dmg);
      const by = state.stats[ev.by];
      if (by) by.damage += ev.dmg;
      break;
    }
    case 'death': {
      const p = state.players[ev.pid];
      if (p) { p.alive = false; p.respawnAt = ev.t + 3; p.hp = 0; }
      const victim = state.stats[ev.pid];
      if (victim) victim.deaths += 1;
      const killer = state.stats[ev.by];
      if (killer) killer.kills += 1;
      break;
    }
    case 'respawn': {
      const p = state.players[ev.pid];
      if (p) { p.alive = true; p.hp = 100; p.x = ev.x; p.y = ev.y; p.respawnAt = 0; }
      break;
    }
    case 'score':
      state.score[ev.team] = ev.total;
      break;
    case 'cloak_start': {
      const p = state.players[ev.pid];
      if (p) p.invisibleUntil = ev.until;
      break;
    }
    case 'cloak_end': {
      const p = state.players[ev.pid];
      if (p) p.invisibleUntil = 0;
      break;
    }
    case 'trap_armed': // secret — spectators never receive this
      state.objects[ev.oid] = {
        id: ev.oid, kind: 'trap', team: ev.team, x: ev.x, y: ev.y,
        revealed: false, armedAt: ev.t, owner: ev.pid,
      };
      break;
    case 'trap_expired': // secret — removes a trap spectators never knew about
      delete state.objects[ev.oid];
      break;
    case 'trap_revealed': {
      // Upsert: the spectator client first learns of the trap here, because
      // trap_armed was withheld from the spectator stream.
      const existing = state.objects[ev.oid];
      state.objects[ev.oid] = existing
        ? { ...existing, revealed: true }
        : { id: ev.oid, kind: 'trap', team: ev.team, x: ev.x, y: ev.y, revealed: true, armedAt: ev.t, owner: ev.pid };
      break;
    }
    case 'trap_consumed':
      delete state.objects[ev.oid];
      break;
    case 'objective_spawned':
      state.objectives[ev.oid] = { id: ev.oid, x: ev.x, y: ev.y, active: true, capturedBy: null };
      state.scheduled = state.scheduled.filter((s) => s.oid !== ev.oid);
      break;
    case 'objective_captured': {
      const o = state.objectives[ev.oid];
      if (o) { o.active = false; o.capturedBy = ev.team; }
      break;
    }
    case 'schedule_set': // secret — future spawns known to the authority only
      state.scheduled = ev.objectives.map((o) => ({ ...o }));
      break;
    case 'match_end':
      state.phase = 'ended';
      state.result = { winner: ev.winner, score: { ...ev.score } };
      break;
    case 'ability':
      break; // visual-only (shot tracer), no state change
    default:
      throw new Error(`unknown event type: ${ev.type}`);
  }
  return state;
}

export function cloneState(state) {
  return structuredClone(state);
}
