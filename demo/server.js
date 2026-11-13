import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { WebSocketServer } from 'ws';
import { EventStore, SnapshotStore } from '../src/store.js';
import { Simulator } from '../src/simulator.js';
import { SpectatorService } from '../src/service.js';
import { SpectatorStateCache } from '../src/cache.js';
import { OptimizedSpectatorService } from '../src/optimized-service.js';
import { sanitizeEventForPlayer, sanitizeStateForPlayer, sanitizeStateForSpectator } from '../src/filter.js';
import { makeRng } from '../src/rng.js';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT ?? 8080;
const TICK_MS = 50;
const MATCH_DURATION = 180; // seconds; the world auto-restarts after the replay window
const RULES_VERSION = 'rules-v1';

const wallNow = () => Date.now() / 1000;

// Baseline service that counts its full rebuilds, so the UI can contrast
// "before the optimization" (per-connection service, always full rebuild)
// with "after" (shared cache) on the same match.
class CountingBaselineService extends SpectatorService {
  constructor(stats, opts) {
    super(opts);
    this.stats = stats;
  }

  snapshotAt(clock) {
    if (clock >= 0) this.stats.fullRebuilds += 1;
    return super.snapshotAt(clock);
  }
}

function makeWorld(seed) {
  const events = new EventStore();
  const snapshots = new SnapshotStore();
  const sim = new Simulator({ events, snapshots, rng: makeRng(seed), duration: MATCH_DURATION, tick: TICK_MS / 1000 });
  const cache = new SpectatorStateCache({ events, snapshots, rulesVersion: RULES_VERSION });
  // One shared optimized service for every spectator connection of this match.
  const optimized = new OptimizedSpectatorService({ cache, delay: 5, now: wallNow });
  const baselineStats = { fullRebuilds: 0 };
  return { events, snapshots, sim, cache, optimized, baselineStats, seed, endedAt: null };
}

let world = makeWorld(1);
const playerSockets = new Set(); // team-A live view
const spectatorSockets = new Map(); // ws -> connection state

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const file = url.pathname === '/' ? '/public/index.html'
    : url.pathname === '/src/state.js' ? '/../src/state.js'
    : null;
  if (!file) { res.writeHead(404); res.end(); return; }
  try {
    const body = await readFile(path.join(ROOT, file));
    res.writeHead(200, { 'content-type': file.endsWith('.js') ? 'text/javascript' : 'text/html' });
    res.end(body);
  } catch {
    res.writeHead(404); res.end();
  }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/ws/player') {
    playerSockets.add(ws);
    ws.send(JSON.stringify({ kind: 'snapshot', t: world.sim.state.t, state: sanitizeStateForPlayer(world.sim.state, 'A') }));
    ws.on('close', () => playerSockets.delete(ws));
    return;
  }
  if (url.pathname === '/ws/spectator') {
    const delay = Number(url.searchParams.get('delay') ?? 5);
    const mode = url.searchParams.get('mode') === 'baseline' ? 'baseline' : 'optimized';
    const sessionId = url.searchParams.get('sessionId') ?? undefined;
    const lastSeq = url.searchParams.has('lastSeq') ? Number(url.searchParams.get('lastSeq')) : undefined;
    const pollMs = Number(url.searchParams.get('pollMs') ?? 100);
    const bufferMax = url.searchParams.has('bufferMax') ? Number(url.searchParams.get('bufferMax')) : undefined;
    const conn = { mode, sessionId, pollMs, bufferMax, delay, timer: null, service: null };
    conn.service = mode === 'optimized'
      ? world.optimized // shared: every optimized spectator shares one cache
      : new CountingBaselineService(world.baselineStats, { events: world.events, snapshots: world.snapshots, delay, now: wallNow });
    spectatorSockets.set(ws, conn);
    joinAndPump(ws, conn, lastSeq);
    ws.on('message', (data) => {
      let msg;
      try { msg = JSON.parse(data); } catch { return; }
      if (msg.type === 'seek') ws.send(JSON.stringify(seekPreview(conn, Number(msg.t))));
      if (msg.type === 'status') ws.send(JSON.stringify(statusMessage(conn)));
    });
    ws.on('close', () => { clearInterval(conn.timer); spectatorSockets.delete(ws); });
    return;
  }
  ws.close();
});

function joinAndPump(ws, conn, lastSeq) {
  const res = conn.service.join({
    sessionId: conn.sessionId, lastSeq, bufferMax: conn.bufferMax,
    ...(conn.mode === 'optimized' ? { delay: conn.delay } : {}),
  });
  if (res.kind === 'stall') {
    ws.send(JSON.stringify(res));
    conn.timer = setTimeout(() => joinAndPump(ws, conn, lastSeq), res.retryAfterMs);
    return;
  }
  conn.sessionId = res.sessionId;
  ws.send(JSON.stringify(res));
  conn.timer = setInterval(() => {
    for (const msg of conn.service.poll(conn.sessionId)) {
      if (ws.readyState !== ws.OPEN) return;
      ws.send(JSON.stringify(msg));
    }
  }, conn.pollMs);
}

// One-off preview of an earlier moment inside the delay window. The delay
// edge is a hard wall in both modes; the follow cursor is never moved.
function seekPreview(conn, t) {
  if (conn.mode === 'optimized') return world.optimized.seek(conn.sessionId, t);
  const clock = conn.service.clock();
  const T = Math.min(t, clock);
  const snap = conn.service.snapshotAt(T);
  if (!snap) {
    return { kind: 'stall', reason: 'insufficient_history', retryAfterMs: conn.service.retryAfterMs(T), clock };
  }
  return {
    kind: 'snapshot', sessionId: conn.sessionId, t: snap.t, seq: snap.seq,
    state: sanitizeStateForSpectator(snap.state), rebuilt: true, clock, preview: true,
  };
}

function statusMessage(conn) {
  return {
    kind: 'status',
    mode: conn.mode,
    clock: wallNow() - conn.delay,
    sessionState: conn.service.sessionState?.(conn.sessionId) ?? 'unknown',
    sessions: world.optimized.sessions.size,
    cache: world.cache.stats,
    cacheLog: world.cache.invalidationLog.slice(-3),
    entries: world.cache.size(),
    baseline: world.baselineStats,
  };
}

// Live tick: advance the simulator, fan out the player view, restart ended matches.
let lastFannedSeq = 0;
setInterval(() => {
  world.sim.step(wallNow());
  if (world.sim.ended && !world.endedAt) world.endedAt = wallNow();
  if (world.endedAt && wallNow() > world.endedAt + 15) {
    world = makeWorld(world.seed + 1);
    lastFannedSeq = 0;
    for (const ws of playerSockets) {
      ws.send(JSON.stringify({ kind: 'snapshot', t: 0, state: sanitizeStateForPlayer(world.sim.state, 'A'), note: 'new_match' }));
    }
    for (const [ws, conn] of spectatorSockets) {
      clearInterval(conn.timer);
      conn.service = conn.mode === 'optimized'
        ? world.optimized
        : new CountingBaselineService(world.baselineStats, { events: world.events, snapshots: world.snapshots, delay: conn.delay, now: wallNow });
      conn.sessionId = undefined;
      joinAndPump(ws, conn, undefined);
    }
    return;
  }
  if (playerSockets.size === 0) return;
  const fresh = world.events.range(lastFannedSeq, Infinity);
  if (fresh.length === 0) return;
  lastFannedSeq = fresh[fresh.length - 1].seq;
  const pub = fresh.map((ev) => sanitizeEventForPlayer(ev, 'A')).filter(Boolean);
  if (pub.length === 0) return;
  const msg = JSON.stringify({ kind: 'events', events: pub });
  for (const ws of playerSockets) ws.send(msg);
}, TICK_MS);

server.listen(PORT, () => {
  console.log(`demo: http://localhost:${PORT}  (player view vs delayed spectator view)`);
});
