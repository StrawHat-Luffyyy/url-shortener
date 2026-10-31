# Redirect benchmarks

First load-test report (Phase 2). Reproduce with `docker compose up -d --wait` then `npm run bench:redirect` (see [bench/redirect.ts](../bench/redirect.ts) for the knobs: `BENCH_RATE`, `BENCH_SECONDS`, `BENCH_SCENARIOS`, `BENCH_WORKERS`, `BENCH_SERVER_PRIORITY`).

**Targets (PRD G1):** at 4,000 req/s, redirect p99 < 30 ms on a cache hit and < 80 ms on a cache miss.

**Result:** G1 is met on a single laptop running client, server and databases together.
- **With 2 generator threads, every run meets both targets** (3 of 3, no errors, generator on schedule).
  - Hit p99: 4.3–15.8 ms in the hit runs, 7.6–25.5 ms within the 95/5 mix (target < 30 ms).
  - Miss p99: 16.1–31.8 ms within the 95/5 mix (target < 80 ms).
- **An earlier series with 4 generator threads met both targets in 2 of 3 mixed runs.** The more threads the generator uses, the less CPU is left for the server on a shared machine (see [Findings](#findings)).
- **A 100% cache-miss load is over one process's capacity at 4k req/s.** One process sustains about 2,500 misses/s within the miss target.

No second machine was available, so the generator shared the laptop with the server in every run. That contention can only make latency worse, so these numbers are an upper bound on server latency. A run with a separate load machine (`BENCH_MODE=serve` / `load`) would still tighten the evidence.

## Setup

| | |
|---|---|
| Machine | Intel Core i5-12450H (4P + 4E cores, 12 threads), 16 GB RAM, Windows 11 (10.0.26300) |
| Server | One Node 24.11.1 process (`tsx src/server.ts`), Postgres pool 20, at above-normal OS priority |
| Data stores | PostgreSQL 17.11 and Redis 7.4.12 in Docker Desktop (WSL2 VM, 12 CPUs, 8 GB) on the same machine |
| Load | Open-loop, evenly paced, on the same machine (2 or 4 worker threads, per series); 20 s per run |
| Data | 1,000 hot links (warmed before the run) and about 80k cold links, never requested before the run |

Each request is `GET /:code` and must return a 302. Latency runs from when a request leaves the generator to the end of its response. The generator records how late each request was sent compared with its schedule; a run is flagged as unreliable if that p99 exceeds 5 ms.

## Results at 4,000 req/s

### 2 generator threads (current default)

One `npm run bench:redirect` with `BENCH_WORKERS=2 BENCH_SECONDS=20 BENCH_REPS=3 BENCH_SCENARIOS=hit,mixed`, cache re-warmed before each scenario. Achieved 3,982–3,986 req/s. Every request returned a 302; no errors. Send lateness p99 was 0.5–1.9 ms, so the generator stayed on schedule. A first attempt in the same session with 4 threads was flagged as generator-behind and discarded.

| Scenario | Run | Requests | p50 ms | p90 ms | p99 ms | max ms | Target met |
|---|---|---|---|---|---|---|---|
| 100% hits | 1 | 80,000 | 0.9 | 1.7 | 4.3 | 53.9 | yes |
| 100% hits | 2 | 80,000 | 1.4 | 2.6 | 15.8 | 29.0 | yes |
| 100% hits | 3 | 80,000 | 1.3 | 2.3 | 5.2 | 20.4 | yes |
| 95/5 mix: hits | 1 | 76,044 | 1.7 | 3.2 | 7.6 | 47.9 | yes |
| 95/5 mix: misses | 1 | 3,956 | 4.8 | 7.8 | 16.1 | 32.5 | yes |
| 95/5 mix: hits | 2 | 76,008 | 1.7 | 3.3 | 14.4 | 72.9 | yes |
| 95/5 mix: misses | 2 | 3,992 | 4.8 | 8.1 | 21.2 | 107.2 | yes |
| 95/5 mix: hits | 3 | 76,006 | 1.7 | 3.3 | 25.5 | 58.9 | yes |
| 95/5 mix: misses | 3 | 3,994 | 4.7 | 8.1 | 31.8 | 58.9 | yes |

Max values exceed the p99 targets in places (up to 107 ms). G1 is stated as p99, so these single outliers don't break it, but they are the tail to watch. Mixed-run hit p99 also varied from 7.6 to 25.5 ms, so the margin under 30 ms is real but not large on this machine.

### 4 generator threads (earlier series)

Three runs each, run separately with a pause between. Every request returned a 302; no errors.

| Scenario | Run | p50 ms | p90 ms | p99 ms | max ms | Target met |
|---|---|---|---|---|---|---|
| 100% hits | 1 | 2.0 | 4.4 | 20.6 | 52.1 | yes |
| 100% hits | 2 | 2.3 | 4.5 | 14.3 | 55.1 | yes |
| 100% hits | 3 | 2.3 | 4.5 | 16.9 | 56.5 | yes |
| 95/5 mix: hits | 1 | 2.7 | 8.6 | 55.2 | 118.8 | **no** |
| 95/5 mix: misses | 1 | 7.1 | 19.4 | 121.6 | 176.0 | **no** |
| 95/5 mix: hits | 2 | 2.5 | 4.8 | 14.3 | 35.7 | yes |
| 95/5 mix: misses | 2 | 6.7 | 11.9 | 31.0 | 61.0 | yes |
| 95/5 mix: hits | 3 | 2.6 | 5.1 | 18.7 | 48.7 | yes |
| 95/5 mix: misses | 3 | 6.8 | 11.9 | 30.5 | 71.8 | yes |

## Cache-miss capacity (100% misses, one process)

| Rate req/s | p50 ms | p90 ms | p99 ms | max ms |
|---|---|---|---|---|
| 2,000 | 10.2 | 21.2 | 45.1 | 105.9 |
| 2,500 | 20.3 | 35.0 | 60.1 | 85.8 |
| 3,000 | 31.0 | 56.7 | 92.4 | 145.9 |
| 4,000 | over capacity: latency grows with run length (p50 > 3 s in a 30 s run) |

A miss costs a Redis `MGET`, a Postgres lookup and a Lua cache fill. During a 3,000 misses/s run the server process used 91% of one core, Postgres 41% and Redis 33%. **The single Node event loop is the limit.** A bigger Postgres pool makes things worse: at 3,000 misses/s, p50 was 13 ms with a pool of 10, 54 ms with 20 and 471 ms with 40. The PRD's route to more throughput is more processes (horizontal scaling). One process covers the target mix, whose miss rate is about 200/s.

## Findings

These came out of getting trustworthy numbers. Each was confirmed by an experiment, not assumed.

1. **autocannon's `overallRate` sends in bursts.** It sends each second's requests at once, so at 500 req/s it reported a p99 of 82 ms. An evenly paced client at the same rate measured 4.7 ms. Replaced with the open-loop generator in `bench/redirect.ts`.
2. **One generator thread saturates near 4k req/s.** At the same total of 4,000 req/s, one generator process gave a p99 of 55 ms; two processes at 2,000 each gave 3.5–4.2 ms. The bench now spreads load across worker threads and reports how late requests were sent.
3. **Keep-alive race caused `ECONNRESET`s.** Node's HTTP server closes idle keep-alive sockets after 5 s, so a client reusing one at that moment gets reset (8–134 per run). Fixed in [src/server.ts](../src/server.ts): `keepAliveTimeout` is now 65 s, above typical load-balancer idle timeouts, which a production deploy needs anyway. Errors dropped to 0.
4. **Remaining tail variance is CPU contention on the shared laptop, not the server.**
   - An event-loop and GC probe in the server showed no stalls during runs (loop delay max about 7 ms, GC pauses ≤ 1 ms), while clients saw p99 of 60 ms or more.
   - Going from 4 to 8 generator threads starved the server (p50 1.8–3.8 s).
   - Raising the server's OS priority brought hit p99 from 23–102 ms down to 14–21 ms.

## Click tracking (Phase 3)

Not yet benchmarked for latency on a quiet machine. The attempt in this session ran while a browser was using about 2.7 cores, so the generator fell behind in every run and the latencies were unusable. What could be measured reliably was **server CPU time per redirect** at 1,000 req/s (20 s, cache hits), alternating:

| Click tracking | Run 1 | Run 2 |
|---|---|---|
| off | 553 µs | 563 µs |
| on (Kafka, `clicks.bench` topic) | 680 µs | 580 µs |

So tracking costs roughly 20–130 µs of CPU per redirect, about 0.1–0.5 of a core at 4k req/s. The redirect never waits on Kafka (`test/click-emitter.test.ts`). The absolute numbers are inflated by the busy machine; compare the rows, not the values. To recheck G1 with tracking on a quiet machine:
`KAFKA_BROKERS=localhost:9092 IP_HASH_SALT=bench CLICKS_TOPIC=clicks.bench BENCH_SECONDS=20 BENCH_REPS=3 BENCH_SCENARIOS=hit,mixed npm run bench:redirect`.

5. **Kafka's Docker healthcheck kept a core busy.** It ran a Kafka CLI tool every 3 s, and each run boots a JVM costing about 1.5 cores for a few seconds. Idle, the container sat at 112% CPU (6% after the fix), slowing every benchmark on the machine. The check now runs every 2 s during startup only, then every 5 minutes.

## Not covered yet

- **Creation latency under sustained load.** PRD target: p99 < 150 ms; not benchmarked yet.
- **A separate load-generator machine** (see the caveat above). The bench supports it:
  - On the server machine, run `BENCH_MODE=serve`. It seeds, starts the server and serves the codes on port 3991.
  - On the other machine, run `BENCH_MODE=load BENCH_TARGET=http://<server-ip>:3990` with the same `BENCH_RATE`, `BENCH_SECONDS` and `BENCH_REPS`.
  - Ports 3990–3991 must be reachable. A wired link is preferred, because Wi-Fi jitter lands directly in p99.
- **Results under Redis or Postgres failure.** Behavior is covered by tests; performance isn't measured.
