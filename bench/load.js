#!/usr/bin/env node
// 观众负载发生器：对比基线（每观众一个服务、各自全量重建）与优化实现
// （共享缓存）在三种负载下的成本：
//   first-join      首次加入（冷缓存）
//   stable-follow   稳定跟随（热路径，不应有额外开销）
//   mass-reconnect  大量观众同时重连（保留期之外 → 同时重建）
//
// 结论只从原始记录计算。原始记录写入 bench/out/load-raw.jsonl；
// `node bench/load.js --summarize bench/out/load-raw.jsonl` 可不重跑、
// 直接从记录重新计算结论。
import { performance } from 'node:perf_hooks';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { EventStore, SnapshotStore } from '../src/store.js';
import { Simulator } from '../src/simulator.js';
import { SpectatorService } from '../src/service.js';
import { SpectatorStateCache } from '../src/cache.js';
import { OptimizedSpectatorService } from '../src/optimized-service.js';
import { rebuildAt } from '../src/rebuild.js';
import { makeRng } from '../src/rng.js';

const TICK = 0.05;
const DELAY = 5;

function makeBenchWorld({ seed, duration = 120, maxEvents, snapshotEvery = 1 }) {
  let now = 0;
  const events = new EventStore({ maxEvents });
  const snapshots = new SnapshotStore();
  const sim = new Simulator({ events, snapshots, rng: makeRng(seed), duration, snapshotEvery });
  return {
    events, snapshots, sim,
    get now() { return now; },
    run(to) {
      while (now < to - 1e-9) {
        now = Math.min(to, now + TICK);
        sim.step(now);
      }
    },
  };
}

// Baseline with real execution counts: one service per spectator connection
// (the current production model), every join/rebuild is a full rebuildAt.
class InstrumentedBaseline extends SpectatorService {
  constructor(stats, opts) {
    super(opts);
    this.stats = stats;
  }

  snapshotAt(clock) {
    if (clock < 0) return null;
    this.stats.fullRebuilds += 1;
    return rebuildAt(this.events, this.snapshots, clock, this.stats);
  }
}

function attach(impl, world) {
  if (impl === 'optimized') {
    const cache = new SpectatorStateCache({ events: world.events, snapshots: world.snapshots });
    const service = new OptimizedSpectatorService({ cache, delay: DELAY, now: () => world.now });
    return {
      join: (req) => ({ res: service.join(req), svc: service }),
      stats: () => ({ ...cache.stats, invalidations: { ...cache.stats.invalidations } }),
    };
  }
  const stats = { fullRebuilds: 0, eventsApplied: 0 };
  return {
    join: (req) => {
      const svc = new InstrumentedBaseline(stats, {
        events: world.events, snapshots: world.snapshots, delay: DELAY, now: () => world.now,
      });
      return { res: svc.join(req), svc };
    },
    stats: () => ({ ...stats }),
  };
}

function timed(records, scenario, impl, op, i, fn) {
  const t0 = performance.now();
  const res = fn();
  records.push({ scenario, impl, op, i, ns: (performance.now() - t0) * 1e6, kind: res?.kind });
  return res;
}

// 首次加入：比赛进行中，N 个观众在同一时刻首次连接（冷缓存）。
function scenarioFirstJoin(records, impl, spectators, seed) {
  const world = makeBenchWorld({ seed });
  world.run(30);
  const att = attach(impl, world);
  for (let i = 0; i < spectators; i += 1) {
    timed(records, 'first-join', impl, 'join', i, () => att.join({}).res);
  }
  records.push({ scenario: 'first-join', impl, op: 'stats', spectators, ...att.stats() });
}

// 稳定跟随：N 个观众已在线，10 秒内每 100ms 轮询一次（热路径）。
function scenarioStableFollow(records, impl, spectators, seed, seconds = 10) {
  const world = makeBenchWorld({ seed });
  world.run(30);
  const att = attach(impl, world);
  const sessions = [];
  for (let i = 0; i < spectators; i += 1) {
    const { res, svc } = att.join({});
    sessions.push({ svc, id: res.sessionId });
  }
  let step = 0;
  let delivered = 0;
  while (world.now < 30 + seconds) {
    world.run(world.now + 0.1);
    const t0 = performance.now();
    let msgs = 0;
    for (const s of sessions) msgs += s.svc.poll(s.id).length;
    const ns = (performance.now() - t0) * 1e6;
    delivered += msgs;
    records.push({ scenario: 'stable-follow', impl, op: 'poll_step', i: step, ns, msgs });
    step += 1;
  }
  records.push({ scenario: 'stable-follow', impl, op: 'stats', spectators, delivered, ...att.stats() });
}

// 大量观众同时重连：日志保留有限，旧游标已淘汰 → 全部同时需要重建。
function scenarioMassReconnect(records, impl, spectators, seed) {
  const world = makeBenchWorld({ seed, maxEvents: 300 });
  world.run(30);
  const att = attach(impl, world);
  const ids = [];
  for (let i = 0; i < spectators; i += 1) ids.push(att.join({}).res.sessionId);
  world.run(50); // 保留 300 条 ≈ 15 秒日志，早期游标已被淘汰
  for (let i = 0; i < spectators; i += 1) {
    const res = timed(records, 'mass-reconnect', impl, 'reconnect', i,
      () => att.join({ sessionId: ids[i], lastSeq: 1 }).res);
    records[records.length - 1].rebuilt = res.rebuilt === true;
  }
  records.push({ scenario: 'mass-reconnect', impl, op: 'stats', spectators, ...att.stats() });
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

// 结论只由原始记录计算：汇总耗时分位数与真实执行次数。
export function summarize(records) {
  const scenarios = ['first-join', 'stable-follow', 'mass-reconnect'];
  const impls = [...new Set(records.map((r) => r.impl))];
  const rows = [];
  for (const scenario of scenarios) {
    for (const impl of impls) {
      const ops = records.filter((r) => r.scenario === scenario && r.impl === impl && r.op !== 'stats');
      const stats = records.find((r) => r.scenario === scenario && r.impl === impl && r.op === 'stats');
      if (ops.length === 0 || !stats) continue;
      const ns = ops.map((r) => r.ns).sort((a, b) => a - b);
      const us = (v) => (v / 1000).toFixed(1);
      rows.push({
        scenario, impl,
        spectators: stats.spectators,
        ops: ops.length,
        totalMs: (ns.reduce((a, b) => a + b, 0) / 1e6).toFixed(2),
        p50us: us(percentile(ns, 50)),
        p95us: us(percentile(ns, 95)),
        maxUs: us(ns[ns.length - 1]),
        hits: stats.hits ?? '-',
        incremental: stats.incremental ?? '-',
        fullRebuilds: stats.fullRebuilds,
        eventsApplied: stats.eventsApplied,
        projections: stats.projections ?? '-',
        invalidations: stats.invalidations ? JSON.stringify(stats.invalidations) : '-',
        delivered: stats.delivered ?? '-',
      });
    }
  }
  return rows;
}

function printSummary(rows) {
  const cols = ['scenario', 'impl', 'spectators', 'ops', 'totalMs', 'p50us', 'p95us', 'maxUs',
    'hits', 'incremental', 'fullRebuilds', 'eventsApplied', 'projections', 'invalidations', 'delivered'];
  const widths = Object.fromEntries(cols.map((c) => [c, Math.max(c.length, ...rows.map((r) => String(r[c]).length))]));
  const line = (row) => cols.map((c) => String(row[c]).padEnd(widths[c])).join('  ');
  console.log(line(Object.fromEntries(cols.map((c) => [c, c]))));
  console.log(cols.map((c) => '-'.repeat(widths[c])).join('  '));
  for (const row of rows) console.log(line(row));
}

const args = process.argv.slice(2);
if (args[0] === '--summarize') {
  const records = readFileSync(args[1], 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  printSummary(summarize(records));
} else {
  const spectators = Number(process.env.SPECTATORS ?? 200);
  const records = [];
  for (const impl of ['baseline', 'optimized']) {
    scenarioFirstJoin(records, impl, spectators, 7);
    scenarioStableFollow(records, impl, spectators, 8);
    scenarioMassReconnect(records, impl, spectators, 9);
  }
  mkdirSync(new URL('./out', import.meta.url), { recursive: true });
  const out = new URL('./out/load-raw.jsonl', import.meta.url);
  writeFileSync(out, `${records.map((r) => JSON.stringify(r)).join('\n')}\n`);
  printSummary(summarize(records));
  console.log(`\n原始记录: ${out.pathname}`);
  console.log(`重新计算结论: node bench/load.js --summarize ${out.pathname}`);
}
