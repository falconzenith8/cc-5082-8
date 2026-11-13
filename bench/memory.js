#!/usr/bin/env node
// 内存测量：共享缓存的驻留开销 vs 基线（每观众一个服务）。
// 三个场景与 bench/load.js 对应：首次加入、稳定跟随、大量观众同时重连。
// 用 `node --expose-gc bench/memory.js` 运行以获得干净的堆读数；
// 没有 gc 时退化为直接读数并标注。原始记录写入 bench/out/memory-raw.jsonl。
import { mkdirSync, writeFileSync } from 'node:fs';
import { EventStore, SnapshotStore } from '../src/store.js';
import { Simulator } from '../src/simulator.js';
import { SpectatorService } from '../src/service.js';
import { SpectatorStateCache } from '../src/cache.js';
import { OptimizedSpectatorService } from '../src/optimized-service.js';
import { makeRng } from '../src/rng.js';

const TICK = 0.05;
const DELAY = 5;
const gcOk = typeof global.gc === 'function';

function makeBenchWorld({ seed, duration = 200, maxEvents, snapshotEvery = 1 }) {
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

function heapMb() {
  if (gcOk) global.gc();
  return process.memoryUsage().heapUsed / 1048576;
}

function attach(impl, world) {
  if (impl === 'optimized') {
    const cache = new SpectatorStateCache({ events: world.events, snapshots: world.snapshots });
    const service = new OptimizedSpectatorService({ cache, delay: DELAY, now: () => world.now });
    return { service, cache, join: (req) => service.join(req), entries: () => cache.size() };
  }
  const services = [];
  return {
    services,
    join: (req) => {
      const svc = new SpectatorService({ events: world.events, snapshots: world.snapshots, delay: DELAY, now: () => world.now });
      services.push(svc);
      return svc.join(req);
    },
    entries: () => ({ services: services.length }),
  };
}

const records = [];
function measure(scenario, impl, spectators, fn) {
  const world = makeBenchWorld({ seed: 21 });
  const att = attach(impl, world);
  const before = heapMb();
  const detail = fn(world, att, spectators);
  const after = heapMb();
  records.push({
    scenario, impl, spectators, gc: gcOk,
    heapBeforeMb: Number(before.toFixed(3)),
    heapAfterMb: Number(after.toFixed(3)),
    heapDeltaMb: Number((after - before).toFixed(3)),
    ...detail,
  });
}

// 首次加入：N 个观众先后加入（各自不同的观战时刻，模拟陆续到达）。
for (const impl of ['baseline', 'optimized']) {
  measure('first-join', impl, 200, (world, att, n) => {
    world.run(30);
    let stateBytes = 0;
    for (let i = 0; i < n; i += 1) {
      world.run(world.now + 0.25); // 错开到达，产生不同的观战时刻
      const res = att.join({});
      if (res.state && stateBytes === 0) stateBytes = JSON.stringify(res.state).length;
    }
    return { entries: JSON.stringify(att.entries()), stateBytes };
  });
}

// 稳定跟随：N 个观众在线 30 秒，观察驻留增长是否有界。
for (const impl of ['baseline', 'optimized']) {
  measure('stable-follow', impl, 200, (world, att, n) => {
    world.run(30);
    const sessions = [];
    for (let i = 0; i < n; i += 1) {
      const res = att.join({});
      const svc = att.service ?? att.services[att.services.length - 1];
      sessions.push({ svc, id: res.sessionId });
    }
    const mid = heapMb();
    while (world.now < 60) {
      world.run(world.now + 0.1);
      for (const s of sessions) s.svc.poll(s.id);
    }
    return { entries: JSON.stringify(att.entries()), heapAtJoinMb: Number(mid.toFixed(3)) };
  });
}

// 大量观众同时重连：保留期之外的重连全部触发重建后的驻留。
for (const impl of ['baseline', 'optimized']) {
  measure('mass-reconnect', impl, 200, (world, att, n) => {
    const w = world;
    w.run(30);
    const ids = [];
    for (let i = 0; i < n; i += 1) ids.push(att.join({}).sessionId);
    w.run(50);
    for (let i = 0; i < n; i += 1) att.join({ sessionId: ids[i], lastSeq: 1 });
    return { entries: JSON.stringify(att.entries()) };
  });
}

mkdirSync(new URL('./out', import.meta.url), { recursive: true });
const out = new URL('./out/memory-raw.jsonl', import.meta.url);
writeFileSync(out, `${records.map((r) => JSON.stringify(r)).join('\n')}\n`);

const cols = ['scenario', 'impl', 'spectators', 'heapDeltaMb', 'heapAfterMb', 'entries', 'stateBytes'];
const rows = records.map((r) => ({ ...r, entries: r.entries ?? '-', stateBytes: r.stateBytes ?? '-' }));
const widths = Object.fromEntries(cols.map((c) => [c, Math.max(c.length, ...rows.map((r) => String(r[c] ?? '-').length))]));
const line = (row) => cols.map((c) => String(row[c] ?? '-').padEnd(widths[c])).join('  ');
console.log(`gc: ${gcOk ? 'on (--expose-gc)' : 'OFF — 直接读数，含未回收垃圾'}`);
console.log(line(Object.fromEntries(cols.map((c) => [c, c]))));
console.log(cols.map((c) => '-'.repeat(widths[c])).join('  '));
for (const row of rows) console.log(line(row));
console.log(`\n原始记录: ${out.pathname}`);
