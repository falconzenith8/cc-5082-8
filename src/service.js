import { rebuildAt } from './rebuild.js';
import { sanitizeEventForSpectator, sanitizeStateForSpectator } from './filter.js';

// Spectator service. Owns the delay-rebuild + filtering pipeline:
//
//   authoritative event log ──(released only when t <= now - delay)──►
//   sanitize ──► spectator session
//
// Invariants enforced here:
//  - historical state comes ONLY from snapshots + event replay, never from
//    live match memory (the service does not even hold a reference to it);
//  - snapshot, score and stats in every message share one spectator clock,
//    because they are all folded from the same event prefix;
//  - missing data produces an explicit stall or rebuild, never a guess.
export class SpectatorService {
  constructor({ events, snapshots, delay, now, bufferMax = 200 }) {
    this.events = events;
    this.snapshots = snapshots;
    this.delay = delay;
    this.now = now;
    this.bufferMax = bufferMax;
    this.sessions = new Map();
    this.nextSession = 1;
  }

  clock() {
    return this.now() - this.delay;
  }

  // join({ sessionId, lastSeq, bufferMax }) -> stall | snapshot | resume
  join({ sessionId, lastSeq, bufferMax } = {}) {
    const clock = this.clock();

    // Reconnect path: resume only if the requested cursor is still covered
    // by the retained log; otherwise fall through to an explicit rebuild.
    if (sessionId && lastSeq != null && this.events.hasSeq(lastSeq)) {
      const s = this.session(sessionId, bufferMax);
      s.lastSeq = lastSeq;
      s.state = 'catching-up';
      s.closed = false;
      return { kind: 'resume', sessionId: s.id, fromSeq: lastSeq, clock };
    }

    const rebuilt = sessionId != null || lastSeq != null;
    const snap = this.snapshotAt(clock);
    if (!snap) {
      return { kind: 'stall', reason: 'insufficient_history', retryAfterMs: this.retryAfterMs(clock), clock };
    }
    const s = this.session(sessionId, bufferMax);
    s.lastSeq = snap.seq;
    s.state = 'catching-up';
    s.closed = false;
    return {
      kind: 'snapshot', sessionId: s.id, t: snap.t, seq: snap.seq,
      state: sanitizeStateForSpectator(snap.state), rebuilt, clock,
    };
  }

  // poll(sessionId) -> messages to send now. Empty array = nothing due.
  poll(sessionId) {
    const s = this.sessions.get(sessionId);
    if (!s || s.closed) return [];
    const clock = this.clock();

    // The session cursor fell out of the retained log: the missing segment
    // cannot be replayed, so the only honest options are stall or rebuild.
    if (s.lastSeq < this.events.firstSeq - 1) {
      return this.rebuildResponse(s, clock, 'retention_gap');
    }

    if (s.state === 'stalled') {
      const snap = this.snapshotAt(clock);
      if (!snap) return [];
      return this.emitSnapshot(s, snap, clock, true);
    }

    const due = this.events.range(s.lastSeq, clock);
    // Live sessions must be drained promptly; catching-up sessions get a
    // larger burst allowance because backlog delivery comes in slices.
    const allowance = s.state === 'live' ? s.bufferMax : s.bufferMax * 4;
    if (due.length > allowance) {
      // Buffer underrun: the client did not drain and the backlog overflowed
      // the session buffer. Explicitly stall, then rebuild from a snapshot —
      // the missing segment is never silently skipped.
      return this.rebuildResponse(s, clock, 'buffer_underrun', due.length);
    }

    const out = [];
    const batch = due.slice(0, s.bufferMax);
    if (batch.length > 0) {
      const events = [];
      for (const ev of batch) {
        const clean = sanitizeEventForSpectator(ev);
        if (clean) events.push(clean);
      }
      s.lastSeq = batch[batch.length - 1].seq;
      out.push({ kind: 'events', events, lastSeq: s.lastSeq, clock });
    }

    if (s.state === 'catching-up' && this.events.range(s.lastSeq, clock).length === 0) {
      s.state = 'live'; // caught up to the delay edge
    }

    if (this.events.ended && s.lastSeq >= this.events.lastSeq) {
      out.push({ kind: 'end', result: this.events.result, clock });
      s.closed = true;
    }
    return out;
  }

  rebuildResponse(s, clock, reason, dropped = 0) {
    const out = [{ kind: 'stalled', reason, dropped, clock }];
    const snap = this.snapshotAt(clock);
    if (!snap) {
      out.push({ kind: 'stall', reason: 'insufficient_history', retryAfterMs: this.retryAfterMs(clock), clock });
      s.state = 'stalled';
      return out;
    }
    s.state = 'catching-up';
    return [...out, ...this.emitSnapshot(s, snap, clock, true)];
  }

  emitSnapshot(s, snap, clock, rebuilt) {
    s.lastSeq = snap.seq;
    return [{
      kind: 'snapshot', sessionId: s.id, t: snap.t, seq: snap.seq,
      state: sanitizeStateForSpectator(snap.state), rebuilt, clock,
    }];
  }

  sessionState(sessionId) {
    return this.sessions.get(sessionId)?.state ?? 'unknown';
  }

  snapshotAt(clock) {
    if (clock < 0) return null;
    return rebuildAt(this.events, this.snapshots, clock);
  }

  retryAfterMs(clock) {
    const earliest = this.snapshots.earliest();
    const waitFor = earliest ? Math.max(earliest.t - clock, 0.2) : 1;
    return Math.ceil(waitFor * 1000);
  }

  session(sessionId, bufferMax) {
    let s = sessionId ? this.sessions.get(sessionId) : null;
    if (!s) {
      s = {
        id: sessionId ?? `s${this.nextSession++}`,
        lastSeq: 0,
        state: 'catching-up',
        closed: false,
        bufferMax: bufferMax ?? this.bufferMax,
      };
      this.sessions.set(s.id, s);
    } else if (bufferMax != null) {
      s.bufferMax = bufferMax;
    }
    return s;
  }
}
