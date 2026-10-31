import type { Kafka } from "./kafka";

// One click on a short link (PRD §4.2). Snake case because this is the wire
// format on clicks.raw, shared with the Python bot-detection service.
export type ClickEvent = {
  event_id: string;
  link_id: string;
  tenant_id: string;
  ts: string; // ISO 8601
  ip_hash: string | null;
  asn: number | null;
  country: string | null;
  user_agent: string | null;
  referrer: string | null;
  accept_language: string | null;
  is_datacenter_asn: boolean | null;
  response_code: number;
};

// Delivers a batch, or rejects so the batch can be retried.
export type ClickSink = (events: ClickEvent[]) => Promise<void>;

export type ClickEmitter = {
  // Queues the event and returns at once. Never throws, never waits.
  emit(event: ClickEvent): void;
  stats(): { buffered: number; inFlight: number; sent: number; dropped: number; failedSends: number };
  // Tries to deliver what is buffered, giving up after `timeoutMs`.
  close(opts?: { timeoutMs?: number }): Promise<void>;
};

type Options = {
  sink: ClickSink;
  maxBuffered?: number;
  maxBatch?: number;
  flushIntervalMs?: number;
  retryDelayMs?: number;
};

// Fire-and-forget delivery of click events (FR-11, PRD §6.3). The redirect
// calls emit() and moves on; batches go out in the background, one at a time.
// When the sink fails, the batch goes back in the buffer and is retried after
// a delay. Past `maxBuffered`, new events are dropped and counted: availability
// over completeness, so the redirect never waits and memory stays bounded.
export function createClickEmitter({
  sink,
  maxBuffered = 10_000,
  maxBatch = 500,
  flushIntervalMs = 20,
  retryDelayMs = 1000,
}: Options): ClickEmitter {
  let buffer: ClickEvent[] = [];
  let inFlight = 0;
  let sent = 0;
  let dropped = 0;
  let failedSends = 0;
  let timer: NodeJS.Timeout | null = null;
  let sending: Promise<void> | null = null;
  let lastErrorLog = 0;

  function schedule(delay: number) {
    if (timer || sending) return;
    timer = setTimeout(flush, delay);
    timer.unref();
  }

  function flush() {
    timer = null;
    if (sending || buffer.length === 0) return;
    const batch = buffer.slice(0, maxBatch);
    buffer = buffer.slice(batch.length);
    inFlight = batch.length;
    sending = sink(batch).then(
      () => {
        sent += batch.length;
        sending = null;
        inFlight = 0;
        if (buffer.length) schedule(0);
      },
      (err: unknown) => {
        failedSends++;
        if (Date.now() - lastErrorLog > 10_000) {
          lastErrorLog = Date.now();
          console.error(`click delivery failed; ${buffer.length + batch.length} buffered, ${dropped} dropped so far`, err);
        }
        // Put the batch back in front; whatever no longer fits is dropped.
        const room = Math.max(0, maxBuffered - buffer.length);
        dropped += Math.max(0, batch.length - room);
        buffer = batch.slice(0, room).concat(buffer);
        sending = null;
        inFlight = 0;
        schedule(retryDelayMs);
      },
    );
  }

  return {
    emit(event) {
      if (buffer.length >= maxBuffered) {
        dropped++;
        return;
      }
      buffer.push(event);
      schedule(buffer.length >= maxBatch ? 0 : flushIntervalMs);
    },

    stats: () => ({ buffered: buffer.length, inFlight, sent, dropped, failedSends }),

    async close({ timeoutMs = 5000 } = {}) {
      const deadline = Date.now() + timeoutMs;
      while ((buffer.length || sending) && Date.now() < deadline) {
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        flush();
        await Promise.race([sending, new Promise((r) => setTimeout(r, Math.max(0, deadline - Date.now())))]);
      }
      if (timer) clearTimeout(timer);
    },
  };
}

// FNV-1a: a cheap, stable string hash for picking a partition.
function hash(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 0x01000193);
  return h >>> 0;
}

// Most events per Kafka message, keeping messages well under Kafka's 1 MB default.
const MAX_EVENTS_PER_MESSAGE = 1000;

// Packs events into one Kafka message per partition: a JSON array of events.
// One message per click cost the producer and broker 2–5× more CPU per click
// (docs/benchmarks.md). A link always hashes to the same partition, so its
// clicks stay in order. Changing the partition count remaps links; avoid it.
export function packByPartition(events: ClickEvent[], partitions: number) {
  const groups = new Map<number, ClickEvent[]>();
  for (const event of events) {
    const p = hash(event.link_id) % partitions;
    const group = groups.get(p);
    if (group) group.push(event);
    else groups.set(p, [event]);
  }
  const messages: { partition: number; value: string }[] = [];
  for (const [partition, group] of groups) {
    for (let i = 0; i < group.length; i += MAX_EVENTS_PER_MESSAGE) {
      messages.push({ partition, value: JSON.stringify(group.slice(i, i + MAX_EVENTS_PER_MESSAGE)) });
    }
  }
  return messages;
}

// Sends batches to a Kafka topic. librdkafka can take over a minute to give up
// on an unreachable broker, so each attempt (connect + send) is capped at
// `timeoutMs`; after a failure the next attempt starts with a fresh producer.
export function createKafkaSink(
  kafka: Kafka,
  topic: string,
  { timeoutMs = 10_000 } = {},
): ClickSink & { disconnect(): Promise<void> } {
  let producer: ReturnType<Kafka["producer"]> | null = null;
  let connected: Promise<number> | null = null; // resolves to the topic's partition count

  function reset() {
    const old = producer;
    producer = null;
    connected = null;
    old?.disconnect().catch(() => {});
  }

  async function connect(p: NonNullable<typeof producer>) {
    await p.connect();
    const admin = kafka.admin();
    await admin.connect();
    try {
      const [meta] = await admin.fetchTopicMetadata({ topics: [topic] });
      if (!meta?.partitions.length) throw new Error(`topic ${topic} not found`);
      return meta.partitions.length;
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  const sink = async (events: ClickEvent[]) => {
    producer ??= kafka.producer({ kafkaJS: { acks: 1 }, "linger.ms": 5, "message.timeout.ms": timeoutMs });
    const current = producer;
    connected ??= connect(current);
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Kafka send timed out after ${timeoutMs}ms`)), timeoutMs);
    });
    try {
      await Promise.race([
        connected.then((partitions) => current.send({ topic, messages: packByPartition(events, partitions) })),
        timeout,
      ]);
    } catch (err) {
      reset();
      throw err;
    } finally {
      clearTimeout(timer);
    }
  };
  return Object.assign(sink, { disconnect: async () => reset() });
}
