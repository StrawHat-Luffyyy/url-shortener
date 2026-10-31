import { randomBytes } from "node:crypto";
import type postgres from "postgres";
import { afterAll } from "vitest";
import { createClickConsumer } from "../src/analytics/consumer";
import { createClickEmitter, createKafkaSink } from "../src/analytics/emitter";
import { createKafka, ensureTopics } from "../src/analytics/kafka";

export const kafkaBrokers = () => (process.env.TEST_KAFKA_BROKERS ?? "localhost:9092").split(",");

// A topic of its own per test file, so files running in parallel don't read
// each other's events.
export async function setupClickPipeline(sql: postgres.Sql) {
  const kafka = createKafka(kafkaBrokers());
  const topic = `clicks.raw.test-${randomBytes(4).toString("hex")}`;
  await ensureTopics(kafka, topic, 3);
  const sink = createKafkaSink(kafka, topic);
  const emitter = createClickEmitter({ sink, flushIntervalMs: 5 });
  const consumers: ReturnType<typeof createClickConsumer>[] = [];

  const startConsumer = async (groupId = `${topic}.analytics`) => {
    const consumer = createClickConsumer({ kafka, sql, topic, groupId });
    consumers.push(consumer);
    await consumer.start();
    return consumer;
  };

  afterAll(async () => {
    await emitter.close({ timeoutMs: 2000 });
    await sink.disconnect();
    await Promise.all(consumers.map((c) => c.stop()));
  }, 30_000);

  return { kafka, topic, sink, emitter, startConsumer };
}

export async function eventually<T>(fn: () => Promise<T>, ok: (v: T) => boolean, timeoutMs = 30_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await fn();
    if (ok(value) || Date.now() > deadline) return value;
    await new Promise((r) => setTimeout(r, 200));
  }
}
