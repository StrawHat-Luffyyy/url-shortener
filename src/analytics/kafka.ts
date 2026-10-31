import { KafkaJS } from "@confluentinc/kafka-javascript";

export type Kafka = KafkaJS.Kafka;

export function createKafka(brokers: string[], clientId = "url-shortener"): Kafka {
  return new KafkaJS.Kafka({
    kafkaJS: { brokers, clientId, logLevel: KafkaJS.logLevel.ERROR },
  });
}

// Click events are keyed by link_id, so one link's clicks stay ordered in one
// partition (PRD §6.2). Invalid events go to `<topic>.dlq`.
export async function ensureTopics(kafka: Kafka, topic: string, partitions = 6): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = [topic, `${topic}.dlq`].filter((t) => !existing.has(t));
    if (missing.length) await admin.createTopics({ topics: missing.map((t) => ({ topic: t, numPartitions: partitions })) });
  } finally {
    await admin.disconnect();
  }
}
