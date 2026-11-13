import { applyEvent } from './state.js';
import { rebuildAt } from './rebuild.js';
import { sanitizeStateForAudience } from './filter.js';

// Shared spectator-state cache. Many spectators watch the same delayed
// moment, so the rebuild + sanitize pipeline runs once per
// (rulesVersion, spectator moment, audience) instead of once per spectator.
//
// Two levels:
//  - full: authoritative historical state, keyed by the event prefix
//    (seqAt(T)) that determines it. Server-only — never released to any
//    client. Advanced incrementally: from the nearest cached prefix, only
//    the events since then are applied, so just the objects those events
//    touch are updated.
//  - views: per-audience sanitized projections of a full entry. Team
//    projections and the public projection are separate entries, so the
//    shared cache cannot hand team-internal information to public spectators.
//
// Validity interval of an entry: it may only be served while the
// authoritative log could still justify it, i.e. while the baseline full
// rebuild (rebuildAt) would also succeed for the same clock. Entries whose
// event segment was trimmed by retention are evicted and counted; they are
// never served past their interval.
//
// Every request is accounted: hits, incremental advances, full rebuilds,
// events applied, projections, and invalidations with reasons. The numbers
// are real executions, so performance conclusions can be recomputed from
// them (see bench/load.js).
export class SpectatorStateCache {
  constructor({ events, snapshots, rulesVersion = 'rules-v1', maxEntries = 16, invalidationLogMax = 50 } = {}) {
    this.events = events;
    this.snapshots = snapshots;
    this.rulesVersion = rulesVersion;
    this.maxEntries = maxEntries;
    this.invalidationLogMax = invalidationLogMax;
    this.full = new Map(); // seq -> { seq, t, state } authoritative, server-only
    this.views = new Map(); // audience -> Map(seq -> { seq, t, state })
    this.stats = {
      requests: 0,
      hits: 0, // served from a cached view, zero recomputation
      incremental: 0, // full entry advanced from a cached prefix
      fullRebuilds: 0, // rebuildAt executions (the baseline cost)
      eventsApplied: 0, // applyEvent executions across rebuild + advance
      projections: 0, // sanitizeStateForAudience executions
      invalidations: { retention_gap: 0, rules_version: 0, capacity: 0 },
    };
    this.invalidationLog = []; // recent { seq, level, reason } for inspection
  }

  // Rules changed: every entry was folded by the old reducer/filter and is
  // void regardless of retention.
  setRulesVersion(version) {
    if (version === this.rulesVersion) return;
    this.rulesVersion = version;
    for (const seq of [...this.full.keys()]) this.invalidate(this.full, seq, 'rules_version', 'full');
    for (const [audience, views] of this.views) {
      for (const seq of [...views.keys()]) this.invalidate(views, seq, 'rules_version', audience);
    }
  }

  invalidate(map, seq, reason, level) {
    map.delete(seq);
    this.stats.invalidations[reason] = (this.stats.invalidations[reason] ?? 0) + 1;
    this.invalidationLog.push({ seq, level, reason });
    if (this.invalidationLog.length > this.invalidationLogMax) this.invalidationLog.shift();
  }

  // An entry at seq S can serve as an incremental base (or exact answer)
  // only while the events right after S are still retained. Once retention
  // trims past it the interval has ended — evict and count, never serve.
  sweepRetention() {
    for (const seq of this.full.keys()) {
      if (this.events.firstSeq > seq + 1) this.invalidate(this.full, seq, 'retention_gap', 'full');
    }
    for (const [audience, views] of this.views) {
      for (const seq of views.keys()) {
        if (this.events.firstSeq > seq + 1) this.invalidate(views, seq, 'retention_gap', audience);
      }
    }
  }

  put(map, seq, entry, level) {
    if (map.has(seq)) map.delete(seq); // refresh recency
    map.set(seq, entry);
    while (map.size > this.maxEntries) {
      this.invalidate(map, map.keys().next().value, 'capacity', level);
    }
  }

  viewMap(audience) {
    let views = this.views.get(audience);
    if (!views) {
      views = new Map();
      this.views.set(audience, views);
    }
    return views;
  }

  // State exactly at spectator clock T for an audience, or null when the
  // baseline full rebuild could not produce one either (stay / wait).
  stateAt(T, audience = 'public') {
    this.stats.requests += 1;
    if (T < 0) return null;
    this.sweepRetention();

    // Availability gate, identical to rebuildAt's preconditions. A cached
    // entry is served only inside the interval where the authoritative log
    // still justifies it — the cache can never outlive its evidence.
    const snap = this.snapshots.floor(T);
    if (!snap || this.events.firstSeq > snap.seq + 1) return null;

    const seq = this.events.seqAt(T);
    const views = this.viewMap(audience);
    const hit = views.get(seq);
    if (hit) {
      this.stats.hits += 1;
      views.delete(seq);
      views.set(seq, hit); // refresh recency
      return hit;
    }

    const full = this.fullAt(seq, T, snap);
    const view = { seq, t: full.t, state: sanitizeStateForAudience(full.state, audience) };
    this.stats.projections += 1;
    this.put(views, seq, view, audience);
    return view;
  }

  fullAt(seq, T, snap) {
    const existing = this.full.get(seq);
    if (existing) {
      this.full.delete(seq);
      this.full.set(seq, existing); // refresh recency
      return existing;
    }

    // Nearest cached prefix still inside its validity interval. It only
    // beats the baseline when it is strictly more recent than the snapshot
    // floor — otherwise the snapshot replay is the shorter path.
    let base = null;
    for (const [s, entry] of this.full) {
      if (s < seq && s > snap.seq && (!base || s > base.seq)) base = entry;
    }
    if (base) {
      // Incremental advance: apply only the events since the base — only
      // the objects those events touch are updated.
      const state = structuredClone(base.state);
      let t = base.t;
      let lastSeq = base.seq;
      for (const ev of this.events.range(base.seq, T)) {
        applyEvent(state, ev);
        this.stats.eventsApplied += 1;
        t = ev.t;
        lastSeq = ev.seq;
      }
      const entry = { seq: lastSeq, t, state };
      this.stats.incremental += 1;
      this.put(this.full, lastSeq, entry, 'full');
      return entry;
    }

    // Cold path: the baseline full rebuild, kept as the correctness anchor.
    const rebuilt = rebuildAt(this.events, this.snapshots, T, this.stats);
    this.stats.fullRebuilds += 1;
    const entry = { seq: rebuilt.seq, t: rebuilt.t, state: rebuilt.state };
    this.put(this.full, rebuilt.seq, entry, 'full');
    return entry;
  }

  size() {
    const views = {};
    for (const [audience, map] of this.views) views[audience] = map.size;
    return { full: this.full.size, views };
  }
}
