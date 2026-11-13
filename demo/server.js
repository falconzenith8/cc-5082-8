import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { WebSocketServer } from 'ws';
import { EventStore, SnapshotStore } from '../src/store.js';
import { Simulator } from '../src/simulator.js';
import { SpectatorService } from '../src/service.js';
import { sanitizeEventForPlayer, sanitizeStateForPlayer } from '../src/filter.js';
import { makeRng } from '../src/rng.js';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT ?? 8080;
const TICK_MS = 50;
const MATCH_DURATION = 180; // seconds; the world auto-restarts after the replay window

const wallNow = () => Date.now() / 1000;

function makeWorld(seed) {
  const events = new EventStore();
  const snapshots = new SnapshotStore();
  const sim = new Simulator({ events, snapshots, rng: makeRng(seed), duration: MATCH_DURATION, tick: TICK_MS / 1000 });
  const service = new SpectatorService({ events, snapshots, delay: 5, now: wallNow });
  return { events, snapshots, sim, service, seed, endedAt: null };
}

let world = makeWorld(1);
const playerSockets = new Set(); // team-A live view
const spectatorSockets = new Map(); // ws -> { sessionId, lastSeq, delay }

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
    const sessionId = url.searchParams.get('sessionId') ?? undefined;
    const lastSeq = url.searchParams.has('lastSeq') ? Number(url.searchParams.get('lastSeq')) : undefined;
    const pollMs = Number(url.searchParams.get('pollMs') ?? 100);
    const bufferMax = url.searchParams.has('bufferMax') ? Number(url.searchParams.get('bufferMax')) : undefined;
    const service = new SpectatorService({ events: world.events, snapshots: world.snapshots, delay, now: wallNow });
    const conn = { service, sessionId, pollMs, bufferMax, timer: null };
    spectatorSockets.set(ws, conn);
    joinAndPump(ws, conn, lastSeq);
    ws.on('close', () => { clearInterval(conn.timer); spectatorSockets.delete(ws); });
    return;
  }
  ws.close();
});

function joinAndPump(ws, conn, lastSeq) {
  const res = conn.service.join({ sessionId: conn.sessionId, lastSeq, bufferMax: conn.bufferMax });
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
      conn.service = new SpectatorService({ events: world.events, snapshots: world.snapshots, delay: conn.service.delay, now: wallNow });
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
