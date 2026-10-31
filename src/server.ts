import { serve } from "@hono/node-server";
import { Redis } from "ioredis";
import postgres from "postgres";
import type { ClickTracking } from "./analytics/click-tracking";
import { createClickEmitter, createKafkaSink } from "./analytics/emitter";
import { createKafka, ensureTopics } from "./analytics/kafka";
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

// Click tracking needs Kafka. Topic creation runs in the background so a Kafka
// outage at startup can't keep redirects from serving; the emitter buffers and
// retries until Kafka is reachable.
let clickTracking: ClickTracking | undefined;
let closeClicks = async () => {};
if (process.env.KAFKA_BROKERS) {
  const kafka = createKafka(process.env.KAFKA_BROKERS.split(","));
  const topic = process.env.CLICKS_TOPIC ?? "clicks.raw";
  ensureTopics(kafka, topic).catch((err) => console.error("could not create Kafka topics:", err));
  const sink = createKafkaSink(kafka, topic);
  const emitter = createClickEmitter({ sink });
  clickTracking = { emitter, ipHashSalt: requireEnv("IP_HASH_SALT"), trustProxy: process.env.TRUST_PROXY === "true" };
  closeClicks = async () => {
    await emitter.close();
    await sink.disconnect();
  };
} else {
  console.warn("KAFKA_BROKERS not set: click tracking is off");
}

const app = createApp({
  sql,
  redis,
  operatorToken: requireEnv("OPERATOR_TOKEN"),
  baseUrl: requireEnv("BASE_URL").replace(/\/$/, ""),
  clickTracking,
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

// Deliver buffered clicks before exiting.
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    server.close();
    closeClicks().finally(() => process.exit(0));
  });
}
