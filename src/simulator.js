import { initialState, applyEvent } from './state.js';

// Match simulator. Drives real movement, ability and score events into the
// authoritative event log. The simulator's own state is itself a fold of the
// events it emits, so "state == fold(events)" holds by construction.
//
// Time is injected: step(t) advances the match to absolute time t (seconds).
// The demo passes wall-clock time; tests pass a virtual clock.

const ARENA = { w: 800, h: 600 };
const SPEED = 120; // units/s
const SHOT_RANGE = 130;
const SHOT_COOLDOWN = 0.8;
const TRAP_TRIGGER_R = 22;
const TRAP_TTL = 20;
const OBJECTIVE_R = 30;
const RESPAWN_DELAY = 3;

const BASES = {
  A: [{ x: 80, y: 80 }, { x: 80, y: 300 }, { x: 80, y: 520 }],
  B: [{ x: 720, y: 80 }, { x: 720, y: 300 }, { x: 720, y: 520 }],
};

export class Simulator {
  constructor({ events, snapshots, rng, duration = 60, snapshotEvery = 1, tick = 0.05 }) {
    this.events = events;
    this.snapshots = snapshots;
    this.rng = rng;
    this.duration = duration;
    this.snapshotEvery = snapshotEvery;
    this.tick = tick;
    this.state = initialState();
    this.brains = new Map(); // pid -> { cooldown, nextTrap, nextCloak, cloaked, wx, wy }
    this.nextOid = 1;
    this.nextObjectiveAt = Infinity;
    this.ended = false;
    this.nextSnapshot = -Infinity;
    this.started = false;
  }

  emit(ev) {
    this.events.append(ev);
    applyEvent(this.state, ev);
  }

  start(t0) {
    for (const team of ['A', 'B']) {
      BASES[team].forEach((base, i) => {
        const pid = `${team}${i + 1}`;
        this.brains.set(pid, {
          cooldown: 0, nextTrap: t0 + 2 + this.rng() * 4,
          nextCloak: t0 + 4 + this.rng() * 6, cloaked: false,
          wx: ARENA.w / 2, wy: ARENA.h / 2,
        });
        this.emit({ type: 'player_join', t: t0, pid, team, x: base.x, y: base.y });
      });
    }
    // The authority precomputes the objective schedule. This is exactly the
    // kind of "future result" that must never reach a spectator early.
    const scheduled = [];
    for (let i = 0; i < Math.floor(this.duration / 15); i += 1) {
      scheduled.push({
        oid: `obj${i + 1}`,
        t: t0 + 8 + i * 15,
        x: 150 + this.rng() * 500,
        y: 120 + this.rng() * 360,
      });
    }
    this.emit({ type: 'schedule_set', t: t0, objectives: scheduled });
    this.nextSnapshot = t0;
    this.maybeSnapshot(t0);
    this.startedAt = t0;
    this.started = true;
  }

  step(t) {
    if (!this.started) this.start(t);
    if (this.ended) return;
    if (t >= this.startedAt + this.duration) {
      const { A, B } = this.state.score;
      const winner = A === B ? 'draw' : A > B ? 'A' : 'B';
      this.emit({ type: 'match_end', t, winner, score: { A, B } });
      this.maybeSnapshot(t, true);
      this.ended = true;
      return;
    }

    this.respawnDue(t);
    this.cloakEndDue(t);
    this.spawnObjectivesDue(t);
    this.captureObjectives(t);

    const moves = {};
    for (const [pid, brain] of this.brains) {
      const p = this.state.players[pid];
      if (!p.alive) continue;
      this.move(pid, p, brain, t);
      moves[pid] = [round2(p.x), round2(p.y)];
      this.combat(pid, p, brain, t);
      this.maybeTrap(pid, p, brain, t);
      this.maybeCloak(pid, p, brain, t);
    }
    this.triggersAndExpiry(t);

    const hidden = Object.values(this.state.players)
      .filter((p) => p.alive && p.invisibleUntil > t)
      .map((p) => p.id);
    this.emit({ type: 'positions', t, moves, hidden });
    this.maybeSnapshot(t);
  }

  move(pid, p, brain, t) {
    const dt = this.tick;
    let tx = brain.wx;
    let ty = brain.wy;
    const objective = Object.values(this.state.objectives).find((o) => o.active);
    const enemy = this.nearestEnemy(p, 250);
    if (objective && this.rng() < 0.6) {
      tx = objective.x; ty = objective.y;
    } else if (enemy) {
      tx = enemy.x; ty = enemy.y;
    } else if (dist(p, { x: tx, y: ty }) < 20) {
      brain.wx = 40 + this.rng() * (ARENA.w - 80);
      brain.wy = 40 + this.rng() * (ARENA.h - 80);
      tx = brain.wx; ty = brain.wy;
    }
    const d = dist(p, { x: tx, y: ty });
    if (d > 1e-6) {
      const step = Math.min(SPEED * dt, d);
      p.x += ((tx - p.x) / d) * step;
      p.y += ((ty - p.y) / d) * step;
    }
    p.x = clamp(p.x, 0, ARENA.w);
    p.y = clamp(p.y, 0, ARENA.h);
  }

  combat(pid, p, brain, t) {
    if (t < brain.cooldown) return;
    const enemy = this.nearestEnemy(p, SHOT_RANGE);
    if (!enemy) return;
    brain.cooldown = t + SHOT_COOLDOWN;
    this.emit({ type: 'ability', t, pid, ability: 'shot', target: enemy.id });
    const dmg = 20 + Math.floor(this.rng() * 16);
    this.emit({ type: 'hit', t, pid, by: pid, target: enemy.id, dmg });
    if (this.state.players[enemy.id].hp <= 0) {
      this.emit({ type: 'death', t, pid: enemy.id, by: pid });
      const total = this.state.score[p.team] + 1;
      this.emit({ type: 'score', t, team: p.team, total, reason: 'elimination', pid });
    }
  }

  maybeTrap(pid, p, brain, t) {
    if (t < brain.nextTrap) return;
    brain.nextTrap = t + 6 + this.rng() * 4;
    const oid = `trap${this.nextOid++}`;
    this.emit({ type: 'trap_armed', t, oid, team: p.team, pid, x: round2(p.x), y: round2(p.y) });
  }

  maybeCloak(pid, p, brain, t) {
    if (t < brain.nextCloak) return;
    brain.nextCloak = t + 10 + this.rng() * 5;
    brain.cloaked = true;
    this.emit({ type: 'cloak_start', t, pid, until: t + 3 });
  }

  triggersAndExpiry(t) {
    for (const obj of Object.values(this.state.objects)) {
      if (obj.kind !== 'trap') continue;
      if (!obj.revealed) {
        const victim = Object.values(this.state.players).find(
          (p) => p.alive && p.team !== obj.team && dist(p, obj) < TRAP_TRIGGER_R,
        );
        if (victim) {
          const owner = obj.team;
          const ownerPid = obj.owner;
          this.emit({ type: 'trap_revealed', t, oid: obj.id, team: owner, pid: ownerPid, x: obj.x, y: obj.y });
          this.emit({ type: 'hit', t, pid: ownerPid, by: ownerPid, target: victim.id, dmg: 45 });
          this.emit({ type: 'trap_consumed', t, oid: obj.id });
          if (this.state.players[victim.id].hp <= 0) {
            this.emit({ type: 'death', t, pid: victim.id, by: ownerPid });
            const total = this.state.score[owner] + 1;
            this.emit({ type: 'score', t, team: owner, total, reason: 'trap', pid: ownerPid });
          }
        } else if (t - obj.armedAt > TRAP_TTL) {
          this.emit({ type: 'trap_expired', t, oid: obj.id, team: obj.team });
        }
      }
    }
  }

  respawnDue(t) {
    for (const p of Object.values(this.state.players)) {
      if (!p.alive && p.respawnAt > 0 && t >= p.respawnAt) {
        const base = BASES[p.team][Number(p.id.slice(1)) - 1];
        this.emit({ type: 'respawn', t, pid: p.id, x: base.x, y: base.y });
      }
    }
  }

  cloakEndDue(t) {
    for (const [pid, brain] of this.brains) {
      const p = this.state.players[pid];
      if (brain.cloaked && p.invisibleUntil > 0 && t >= p.invisibleUntil) {
        brain.cloaked = false;
        this.emit({ type: 'cloak_end', t, pid });
      }
    }
  }

  spawnObjectivesDue(t) {
    for (const s of [...this.state.scheduled]) {
      if (t >= s.t) {
        this.emit({ type: 'objective_spawned', t, oid: s.oid, x: round2(s.x), y: round2(s.y) });
      }
    }
  }

  captureObjectives(t) {
    for (const o of Object.values(this.state.objectives)) {
      if (!o.active) continue;
      const near = Object.values(this.state.players).filter((p) => p.alive && dist(p, o) < OBJECTIVE_R);
      if (near.length === 0) continue;
      const teams = new Set(near.map((p) => p.team));
      if (teams.size === 1) {
        const team = near[0].team;
        this.emit({ type: 'objective_captured', t, oid: o.id, team });
        const total = this.state.score[team] + 3;
        this.emit({ type: 'score', t, team, total, reason: 'objective' });
      }
    }
  }

  nearestEnemy(p, maxDist) {
    let best = null;
    let bestD = maxDist;
    for (const q of Object.values(this.state.players)) {
      if (!q.alive || q.team === p.team || q.invisibleUntil > this.state.t) continue;
      const d = dist(p, q);
      if (d < bestD) { best = q; bestD = d; }
    }
    return best;
  }

  maybeSnapshot(t, force = false) {
    if (force || t >= this.nextSnapshot) {
      this.snapshots.put(t, this.events.lastSeq, this.state);
      this.nextSnapshot = t + this.snapshotEvery;
    }
  }
}

function dist(a, b) {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

function clamp(v, lo, hi) {
  return Math.min(hi, Math.max(lo, v));
}

function round2(v) {
  return Math.round(v * 100) / 100;
}
