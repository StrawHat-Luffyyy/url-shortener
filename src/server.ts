import { serve } from "@hono/node-server";
import { Redis } from "ioredis";
import postgres from "postgres";
import { createApp } from "./app";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

const sql = postgres(requireEnv("DATABASE_URL"), {
  max: Number(process.env.DATABASE_POOL_SIZE ?? 20),
  onnotice: () => {},
});
// Fail fast instead of queueing while Redis is unreachable, so redirects fall
// back to Postgres and rate limits to in-memory buckets without waiting.
const redis = new Redis(requireEnv("REDIS_URL"), {
  enableOfflineQueue: false,
  maxRetriesPerRequest: 1,
  commandTimeout: 100,
});
redis.on("error", (err) => console.error("redis:", err.message));
const app = createApp({
  sql,
  redis,
  operatorToken: requireEnv("OPERATOR_TOKEN"),
  baseUrl: requireEnv("BASE_URL").replace(/\/$/, ""),
});
const port = Number(process.env.PORT ?? 3000);

const server = serve({ fetch: app.fetch, port }, () => console.log(`listening on :${port}`));
// Node closes idle keep-alive sockets after 5s; a client or load balancer that
// reuses one at that moment gets ECONNRESET. Stay above common LB idle
// timeouts (60s) so the far side always closes first.
if ("keepAliveTimeout" in server) {
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 66_000;
}
