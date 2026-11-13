import { isAudience, sanitizeEventForAudience } from './filter.js';

// Optimized spectator service. Same wire protocol and same release rules as
// SpectatorService (the baseline), but historical states come from a shared
// SpectatorStateCache instead of a per-service full rebuild, so N spectators
// watching the same delayed moment pay the rebuild cost once.
//
// Differences from the baseline, all server-internal:
//  - one service instance serves any number of connections; delay and
//    audience are per-session (the cache key covers the rest);
//  - seek() previews an earlier moment inside the delay window without
//    moving the session's follow cursor;
//  - nothing here subscribes to the event log: spectators poll, so a slow
//    spectator can never block the match itself.
//
// The delay is unchanged and no event is dropped from the stream — the
// savings come only from not recomputing identical historical states.
export class OptimizedSpectatorService {
  constructor({ cache, delay, now, bufferMax = 200 }) {
    this.cache = cache;
    this.events = cache.events;
    this.snapshots = cache.snapshots;
    this.delay = delay;
    this.now = now;
    this.bufferMax = bufferMax;
    this.sessions = new Map();
    this.nextSession = 1;
  }

  clockFor(session) {
    return this.now() - (session?.delay ?? this.delay);
  }

  clock() {
    return this.clockFor(null);
  }

  // join({ sessionId, lastSeq, bufferMax, delay, audience })
  //   -> stall | snapshot | resume   (same shapes as the baseline)
  join({ sessionId, lastSeq, bufferMax, delay, audience = 'public' } = {}) {
    if (!isAudience(audience)) throw new Error(`unknown audience: ${audience}`);
    const s = this.session(sessionId, bufferMax);
    if (delay != null) s.delay = delay;
    s.audience = audience;
    const clock = this.clockFor(s);

    if (sessionId && lastSeq != null && this.events.hasSeq(lastSeq)) {
      s.lastSeq = lastSeq;
      s.state = 'catching-up';
      s.closed = false;
      return { kind: 'resume', sessionId: s.id, fromSeq: lastSeq, clock };
    }

    const rebuilt = sessionId != null || lastSeq != null;
    const snap = this.cache.stateAt(clock, s.audience);
    if (!snap) {
      return { kind: 'stall', reason: 'insufficient_history', retryAfterMs: this.retryAfterMs(clock), clock };
    }
    s.lastSeq = snap.seq;
    s.state = 'catching-up';
    s.closed = false;
    return {
      kind: 'snapshot', sessionId: s.id, t: snap.t, seq: snap.seq,
      state: snap.state, rebuilt, clock,
    };
  }

  // poll(sessionId) -> messages to send now. Empty array = nothing due.
  poll(sessionId) {
    const s = this.sessions.get(sessionId);
    if (!s || s.closed) return [];
    const clock = this.clockFor(s);

    if (s.lastSeq < this.events.firstSeq - 1) {
      return this.rebuildResponse(s, clock, 'retention_gap');
    }

    if (s.state === 'stalled') {
      const snap = this.cache.stateAt(clock, s.audience);
      if (!snap) return [];
      return this.emitSnapshot(s, snap, clock, true);
    }

    const due = this.events.range(s.lastSeq, clock);
    const allowance = s.state === 'live' ? s.bufferMax : s.bufferMax * 4;
    if (due.length > allowance) {
      return this.rebuildResponse(s, clock, 'buffer_underrun', due.length);
    }

    const out = [];
    const batch = due.slice(0, s.bufferMax);
    if (batch.length > 0) {
      const events = [];
      for (const ev of batch) {
        const clean = sanitizeEventForAudience(ev, s.audience);
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

  // seek(sessionId, t) -> one-off preview of an earlier moment inside the
  // delay window. Never moves the follow cursor, never crosses the delay
  // edge: the requested time is clamped to this session's spectator clock.
  seek(sessionId, t) {
    const s = this.sessions.get(sessionId);
    const clock = this.clockFor(s);
    if (!s || s.closed) {
      return { kind: 'stall', reason: 'no_session', retryAfterMs: 500, clock };
    }
    const T = Math.min(t, clock);
    const snap = this.cache.stateAt(T, s.audience);
    if (!snap) {
      return { kind: 'stall', reason: 'insufficient_history', retryAfterMs: this.retryAfterMs(T), clock };
    }
    return {
      kind: 'snapshot', sessionId: s.id, t: snap.t, seq: snap.seq,
      state: snap.state, rebuilt: true, clock, preview: true,
    };
  }

  rebuildResponse(s, clock, reason, dropped = 0) {
    const out = [{ kind: 'stalled', reason, dropped, clock }];
    const snap = this.cache.stateAt(clock, s.audience);
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
      state: snap.state, rebuilt, clock,
    }];
  }

  sessionState(sessionId) {
    return this.sessions.get(sessionId)?.state ?? 'unknown';
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
        delay: undefined, // falls back to the service delay
        audience: 'public',
      };
      this.sessions.set(s.id, s);
    } else if (bufferMax != null) {
      s.bufferMax = bufferMax;
    }
    return s;
  }
}
