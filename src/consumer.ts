import postgres from "postgres";
import { createClickConsumer } from "./analytics/consumer";
import { createKafka, ensureTopics } from "./analytics/kafka";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

// The analytics consumer process: clicks.raw → click_events + hourly rollups.
// Run one per few partitions; Kafka spreads partitions across the group.
const sql = postgres(requireEnv("DATABASE_URL"), { max: 5, onnotice: () => {} });
const kafka = createKafka(requireEnv("KAFKA_BROKERS").split(","));
const topic = process.env.CLICKS_TOPIC ?? "clicks.raw";
await ensureTopics(kafka, topic);
const consumer = createClickConsumer({ kafka, sql, topic, groupId: process.env.CONSUMER_GROUP ?? "analytics" });
await consumer.start();
console.log(`consuming ${topic}`);

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, async () => {
    await consumer.stop();
    await sql.end();
    process.exit(0);
  });
}
