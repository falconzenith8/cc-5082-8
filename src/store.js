// Append-only authoritative event log with optional retention, plus the
// snapshot store used as rebuild starting points.

export class EventStore {
  constructor({ maxEvents = Infinity } = {}) {
    this.maxEvents = maxEvents;
    this.events = []; // contiguous, starting at firstSeq
    this.firstSeq = 1;
    this.lastSeq = 0;
    this.ended = false;
    this.result = null;
    this.listeners = new Set();
  }

  append(ev) {
    ev.seq = ++this.lastSeq;
    this.events.push(ev);
    if (ev.type === 'match_end') {
      this.ended = true;
      this.result = { winner: ev.winner, score: { ...ev.score } };
    }
    while (this.events.length > this.maxEvents) {
      this.events.shift();
      this.firstSeq += 1;
    }
    for (const cb of this.listeners) cb(ev);
    return ev;
  }

  onAppend(cb) {
    this.listeners.add(cb);
    return () => this.listeners.delete(cb);
  }

  hasSeq(seq) {
    return seq >= this.firstSeq - 1 && seq <= this.lastSeq;
  }

  // Events with seq > afterSeq and t <= uptoT, in order.
  range(afterSeq, uptoT) {
    const out = [];
    let i = Math.max(afterSeq - this.firstSeq + 1, 0);
    for (; i < this.events.length; i += 1) {
      const ev = this.events[i];
      if (ev.t > uptoT) break; // t is non-decreasing
      out.push(ev);
    }
    return out;
  }

  // Seq of the last event with t <= T (firstSeq - 1 when none is retained).
  // The folded state at clock T depends only on this prefix, so it is the
  // natural cache key for "the spectator moment T".
  seqAt(T) {
    let lo = 0;
    let hi = this.events.length - 1;
    let ans = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (this.events[mid].t <= T) { ans = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    return ans === -1 ? this.firstSeq - 1 : this.firstSeq + ans;
  }
}

export class SnapshotStore {
  constructor() {
    this.snaps = []; // { t, seq, state } sorted by t
  }

  put(t, seq, state) {
    this.snaps.push({ t, seq, state: structuredClone(state) });
  }

  // Latest snapshot at or before T, or null if none exists yet.
  floor(T) {
    let best = null;
    for (const s of this.snaps) {
      if (s.t <= T) best = s;
      else break;
    }
    return best;
  }

  earliest() {
    return this.snaps[0] ?? null;
  }
}
