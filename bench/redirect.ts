// Redirect load test (PRD §8): fixed-rate load in three scenarios (all cache
// hits, all cache misses, a 95/5 mix), with separate percentiles for hits and
// misses. Needs `docker compose up -d` on the server machine.
//
// Modes (BENCH_MODE):
//   local (default)  seed, start the server and generate load on this machine.
//   serve            seed and start the server on this machine, then wait for a
//                    load machine. Also serves the seeded codes on port 3991.
//                    Enter or Ctrl+C stops it and removes the seeded links.
//   load             generate load only, against BENCH_TARGET (e.g.
//                    http://192.168.1.20:3990), using the codes fetched from
//                    the serve machine. Needs no database.
//
// The load is open-loop and evenly paced (requests go out on schedule whether
// or not earlier ones have returned) and spread over worker threads. Two things
// skewed earlier attempts: autocannon's rate limiter sends each second's
// requests in a burst, and a single generator thread saturates near 4k req/s,
// inflating p99 with its own event-loop delay. Each run reports how late
// requests left versus schedule; a late generator makes the latencies
// unreliable, and the run is flagged.
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { constants, cpus, networkInterfaces, release, setPriority, totalmem, type } from "node:os";
import { Worker } from "node:worker_threads";
import { Redis } from "ioredis";
import postgres from "postgres";
import { codeForId } from "../src/links/codes";
import { createIdAllocator } from "../src/links/id-allocator";

const MODE = process.env.BENCH_MODE ?? "local";
const RATE = Number(process.env.BENCH_RATE ?? 4000); // requests per second
const SECONDS = Number(process.env.BENCH_SECONDS ?? 30);
// 2 keeps pace with 4k req/s and, on a shared machine, leaves more CPU for the
// server; 4 fell behind there (docs/benchmarks.md).
const WORKERS = Number(process.env.BENCH_WORKERS ?? 2);
const REPS = Number(process.env.BENCH_REPS ?? 1);
const SCENARIOS = (process.env.BENCH_SCENARIOS ?? "hit,miss,mixed").split(",");
const HOT = 1000;
const TOTAL = RATE * SECONDS;
// Every cache-miss request needs a code nobody has requested yet, so each
// repetition of each scenario gets its own slice of cold codes.
const COLD_MISS = TOTAL + 10_000;
const COLD_MIX = Math.ceil(TOTAL * 0.05) + 10_000;
const PORT = 3990;
const CODES_PORT = 3991;
const MAX_SEND_LATENESS_MS = 5;

const DATABASE_URL = process.env.DATABASE_URL ?? "postgres://shortener:shortener@localhost:5432/shortener";
const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379/0";

type Codes = { hot: string[]; coldMiss: string[][]; coldMix: string[][] };

// Sends `paths` in order at `rps`, evenly paced, from its own thread.
const WORKER_SOURCE = `
const http = require("node:http");
const { parentPort, workerData } = require("node:worker_threads");
const { host, port, paths, misses, rps } = workerData;
const agent = new http.Agent({ keepAlive: true, maxSockets: 64 });
const latency = new Float64Array(paths.length).fill(-1);
const lateness = new Float64Array(paths.length);
const status = {};
const errorCodes = {};
let errors = 0, done = 0, sent = 0;
const start = performance.now();
function fire(i) {
  const t = performance.now();
  lateness[i] = t - (start + (i * 1000) / rps);
  http.get({ host, port, path: paths[i], agent }, (res) => {
    status[res.statusCode] = (status[res.statusCode] || 0) + 1;
    res.resume();
    res.on("end", () => { latency[i] = performance.now() - t; finish(); });
  }).on("error", (err) => { errors++; errorCodes[err.code] = (errorCodes[err.code] || 0) + 1; finish(); });
}
function finish() {
  if (++done === paths.length) {
    agent.destroy();
    parentPort.postMessage({ latency, lateness, status, errors, errorCodes, misses });
  }
}
(function tick() {
  const due = Math.min(paths.length, Math.floor(((performance.now() - start) / 1000) * rps) + 1);
  while (sent < due) fire(sent++);
  if (sent < paths.length) setImmediate(tick);
})();
`;

type WorkerResult = {
  latency: Float64Array;
  lateness: Float64Array;
  status: Record<string, number>;
  errors: number;
  errorCodes: Record<string, number>;
  misses: Uint8Array; // 1 where the request was a cache miss
};

// `miss(i)` marks requests expected to miss the cache, so hits and misses get
// separate percentiles.
async function run(target: URL, name: string, paths: string[], miss: (i: number) => boolean) {
  const started = performance.now();
  const shardIdx = Array.from({ length: WORKERS }, (_, w) => paths.map((_, i) => i).filter((i) => i % WORKERS === w));
  const results = await Promise.all(
    shardIdx.map(
      (idx) =>
        new Promise<WorkerResult>((resolve, reject) => {
          const worker = new Worker(WORKER_SOURCE, {
            eval: true,
            workerData: {
              host: target.hostname,
              port: Number(target.port),
              paths: idx.map((i) => paths[i]),
              misses: Uint8Array.from(idx, (i) => (miss(i) ? 1 : 0)),
              rps: RATE / WORKERS,
            },
          });
          worker.once("message", resolve);
          worker.once("error", reject);
        }),
    ),
  );
  const elapsed = (performance.now() - started) / 1000;

  const pick = (want: 0 | 1) =>
    results.flatMap((r) => [...r.latency].filter((l, i) => l >= 0 && r.misses[i] === want)).sort((a, b) => a - b);
  const lateness = results.flatMap((r) => [...r.lateness]).sort((a, b) => a - b);
  const at = (xs: number[], q: number) => xs[Math.min(xs.length - 1, Math.floor(xs.length * q))]!;
  const status: Record<string, number> = {};
  const errorCodes: Record<string, number> = {};
  for (const r of results) {
    for (const [code, n] of Object.entries(r.status)) status[code] = (status[code] ?? 0) + n;
    for (const [code, n] of Object.entries(r.errorCodes)) errorCodes[code] = (errorCodes[code] ?? 0) + n;
  }
  const summary = (xs: number[]) =>
    xs.length ? { n: xs.length, p50: at(xs, 0.5), p90: at(xs, 0.9), p99: at(xs, 0.99), max: xs.at(-1)! } : null;
  return {
    name,
    rps: paths.length / elapsed,
    hits: summary(pick(0)),
    misses: summary(pick(1)),
    errorCodes,
    non302: Object.entries(status).reduce((n, [code, c]) => (code === "302" ? n : n + c), 0),
    errors: results.reduce((n, r) => n + r.errors, 0),
    lateP99: at(lateness, 0.99),
  };
}

async function runScenarios(target: URL, codes: Codes) {
  // Hot entries expire from the cache after 60s, so warm them before every
  // scenario that should hit.
  const warm = () => Promise.all(codes.hot.map((c) => fetch(new URL(`/${c}`, target), { redirect: "manual" })));
  const runs = [];
  for (let rep = 0; rep < REPS; rep++) {
    const tag = REPS > 1 ? ` #${rep + 1}` : "";
    if (SCENARIOS.includes("hit")) {
      console.error(`running: cache hit${tag}`);
      await warm();
      const paths = Array.from({ length: TOTAL }, (_, i) => `/${codes.hot[i % HOT]}`);
      runs.push(await run(target, `cache hit (100%)${tag}`, paths, () => false));
    }
    if (SCENARIOS.includes("miss")) {
      console.error(`running: cache miss${tag}`);
      const paths = codes.coldMiss[rep]!.slice(0, TOTAL).map((c) => `/${c}`);
      runs.push(await run(target, `cache miss (100%)${tag}`, paths, () => true));
    }
    if (SCENARIOS.includes("mixed")) {
      console.error(`running: mixed${tag}`);
      await warm();
      const isMiss = Array.from({ length: TOTAL }, () => Math.random() >= 0.95);
      let cold = 0;
      const paths = isMiss.map((m, i) => (m ? `/${codes.coldMix[rep]![cold++]}` : `/${codes.hot[i % HOT]}`));
      runs.push(await run(target, `mixed (95% hit / 5% miss)${tag}`, paths, (i) => isMiss[i]!));
    }
    if (rep < REPS - 1) await new Promise((r) => setTimeout(r, 3000));
  }
  report(target, runs);
}

function report(target: URL, runs: Awaited<ReturnType<typeof run>>[]) {
  const cpu = cpus();
  console.log(
    `\nGenerator machine: ${cpu[0]?.model} (${cpu.length} logical cores), ${(totalmem() / 2 ** 30).toFixed(0)} GB RAM, ${type()} ${release()}`,
  );
  console.log(`Target ${target.origin}, ${RATE} req/s for ${SECONDS}s per run, open-loop, ${WORKERS} generator threads\n`);
  console.log("| Scenario | Requests | Achieved req/s | p50 ms | p90 ms | p99 ms | max ms | Non-302 | Errors | Send lateness p99 ms |");
  console.log("|---|---|---|---|---|---|---|---|---|---|");
  const f = (n: number) => n.toFixed(1);
  for (const r of runs) {
    const flag = r.lateP99 > MAX_SEND_LATENESS_MS ? " ⚠ generator behind; latencies unreliable" : "";
    const errs = r.errors ? `${r.errors} (${Object.entries(r.errorCodes).map(([c, n]) => `${c} ${n}`).join(", ")})` : "0";
    for (const [label, s] of [["hits", r.hits], ["misses", r.misses]] as const) {
      if (!s) continue;
      console.log(
        `| ${r.name}: ${label} | ${s.n} | ${r.rps.toFixed(0)} | ${f(s.p50)} | ${f(s.p90)} | ${f(s.p99)} | ${f(s.max)} | ${r.non302} | ${errs} | ${f(r.lateP99)}${flag} |`,
      );
    }
  }
}

async function seed(sql: postgres.Sql, redis: Redis): Promise<Codes> {
  const count = HOT + REPS * (COLD_MISS + COLD_MIX);
  console.error(`seeding ${count} links...`);
  const [tenant] = await sql<{ id: string }[]>`INSERT INTO tenants (name) VALUES ('bench') RETURNING id`;
  const ids = createIdAllocator(redis, sql, 100_000);
  const all: string[] = [];
  for (let start = 0; start < count; start += 10_000) {
    const batch = await Promise.all(Array.from({ length: Math.min(10_000, count - start) }, () => ids.next()));
    const batchCodes = batch.map(codeForId);
    const urls = batch.map((id) => `https://example.com/bench/${id}`);
    await sql`
      INSERT INTO links (id, code, tenant_id, long_url, normalized_url, status)
      SELECT u.id, u.code, ${tenant!.id}::uuid, u.url, u.url, 'active'
      FROM unnest(${batch}::bigint[], ${batchCodes}::text[], ${urls}::text[]) AS u(id, code, url)`;
    all.push(...batchCodes);
  }
  let next = HOT;
  const take = (n: number) => all.slice(next, (next += n));
  return {
    hot: all.slice(0, HOT),
    coldMiss: Array.from({ length: REPS }, () => take(COLD_MISS)),
    coldMix: Array.from({ length: REPS }, () => take(COLD_MIX)),
  };
}

async function startServer() {
  const child = spawn(process.execPath, ["--import", "tsx", "src/server.ts"], {
    env: {
      ...process.env,
      DATABASE_URL,
      REDIS_URL,
      OPERATOR_TOKEN: "bench",
      BASE_URL: `http://localhost:${PORT}`,
      PORT: String(PORT),
    },
    stdio: ["ignore", "pipe", "inherit"],
  });
  await new Promise<void>((resolve, reject) => {
    child.stdout!.on("data", (d: Buffer) => d.toString().includes("listening") && resolve());
    child.on("exit", (code) => reject(new Error(`server exited with ${code}`)));
  });
  // In local mode the generator shares the machine with the server; this keeps
  // it from starving the server of CPU. Set BENCH_SERVER_PRIORITY=normal to compare.
  if (process.env.BENCH_SERVER_PRIORITY !== "normal") setPriority(child.pid!, constants.priority.PRIORITY_ABOVE_NORMAL);
  return child;
}

// Seeds, runs `body` with the server up, then removes the seeded links.
async function withSeededServer(body: (codes: Codes) => Promise<void>) {
  const sql = postgres(DATABASE_URL, { max: 5, onnotice: () => {} });
  const redis = new Redis(REDIS_URL);
  let server: Awaited<ReturnType<typeof startServer>> | undefined;
  try {
    const codes = await seed(sql, redis);
    server = await startServer();
    await body(codes);
  } finally {
    server?.kill();
    console.error("removing seeded links...");
    await sql`DELETE FROM links WHERE tenant_id IN (SELECT id FROM tenants WHERE name = 'bench')`;
    await sql`DELETE FROM tenants WHERE name = 'bench'`;
    await sql.end();
    redis.disconnect();
  }
}

if (MODE === "local") {
  await withSeededServer((codes) => runScenarios(new URL(`http://127.0.0.1:${PORT}`), codes));
} else if (MODE === "serve") {
  await withSeededServer(async (codes) => {
    const codesServer = createServer((_, res) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(codes));
    }).listen(CODES_PORT);
    const addresses = Object.values(networkInterfaces())
      .flat()
      .filter((a) => a && a.family === "IPv4" && !a.internal)
      .map((a) => a!.address);
    console.log(`\nServer ready on port ${PORT}; codes on port ${CODES_PORT}. This machine's addresses: ${addresses.join(", ")}`);
    console.log(`On the load machine: BENCH_MODE=load BENCH_TARGET=http://<address>:${PORT} npm run bench:redirect`);
    console.log("Use the same BENCH_RATE, BENCH_SECONDS and BENCH_REPS on both machines. Press Enter (or Ctrl+C) here when done.");
    await new Promise<void>((resolve) => {
      process.once("SIGINT", resolve);
      process.stdin.once("data", () => resolve());
    });
    process.stdin.pause();
    codesServer.close();
  });
} else if (MODE === "load") {
  if (!process.env.BENCH_TARGET) throw new Error("BENCH_TARGET is required in load mode, e.g. http://192.168.1.20:3990");
  const target = new URL(process.env.BENCH_TARGET);
  const res = await fetch(`http://${target.hostname}:${CODES_PORT}/`);
  const codes = (await res.json()) as Codes;
  if (codes.coldMiss.length < REPS) throw new Error(`serve side seeded for ${codes.coldMiss.length} reps; set BENCH_REPS to match`);
  await runScenarios(target, codes);
} else {
  throw new Error(`unknown BENCH_MODE ${MODE}`);
}
