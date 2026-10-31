import type { Sql } from "postgres";
import { z } from "zod";
import type { ClickEvent } from "./emitter";
import type { Kafka } from "./kafka";
import { createPartitionManager } from "./partitions";
import { classifyUserAgent } from "./user-agent";

const clickEventSchema = z.object({
  event_id: z.uuid(),
  link_id: z.string().regex(/^\d+$/),
  tenant_id: z.uuid(),
  ts: z.iso.datetime({ offset: true }),
  ip_hash: z.string().max(64).nullable(),
  asn: z.number().int().nullable(),
  country: z.string().max(8).nullable(),
  user_agent: z.string().max(512).nullable(),
  referrer: z.string().max(255).nullable(),
  accept_language: z.string().max(128).nullable(),
  is_datacenter_asn: z.boolean().nullable(),
  response_code: z.number().int(),
}) satisfies z.ZodType<ClickEvent>;

// Counted per event: one Kafka message can carry many.
export type ConsumerStats = { events: number; inserted: number; duplicates: number; invalid: number };

// Writes events to click_events and adds them to the hourly rollups, in one
// statement. Rollups only count rows that were actually inserted, so an event
// seen again (redelivery, replay) changes nothing: ON CONFLICT skips it and
// there is no row left to count. That is what makes reprocessing a partition
// safe (AI-6).
async function ingest(sql: Sql, events: ClickEvent[]): Promise<number> {
  const rows = events.map((e) => ({ e, ua: classifyUserAgent(e.user_agent) }));
  const col = <T>(f: (r: (typeof rows)[number]) => T) => rows.map(f);
  const [result] = await sql<{ inserted: number }[]>`
    WITH input AS (
      SELECT * FROM unnest(
        ${col((r) => r.e.event_id)}::uuid[], ${col((r) => r.e.link_id)}::bigint[],
        ${col((r) => r.e.ts)}::timestamptz[], ${col((r) => r.e.country)}::text[],
        ${col((r) => r.e.asn)}::int[], ${col((r) => r.e.referrer)}::text[],
        ${col((r) => r.ua.browser)}::text[], ${col((r) => r.ua.device)}::text[],
        ${col((r) => r.ua.label)}::text[], ${col((r) => r.e.ip_hash)}::text[],
        ${col((r) => r.e.response_code)}::smallint[]
      ) AS t(event_id, link_id, ts, country, asn, referrer, ua_family, device, label, ip_hash, response_code)
    ), inserted AS (
      INSERT INTO click_events (event_id, link_id, ts, country, asn, referrer, ua_family, device, label, ip_hash, response_code)
      SELECT * FROM input
      ON CONFLICT DO NOTHING
      RETURNING link_id, ts, label
    ), rolled_up AS (
      INSERT INTO click_rollups_hourly AS r (link_id, hour, total, human, suspected_bot)
      SELECT link_id, date_trunc('hour', ts, 'UTC'), count(*),
             count(*) FILTER (WHERE label = 'human'), count(*) FILTER (WHERE label <> 'human')
      FROM inserted GROUP BY 1, 2
      ON CONFLICT (link_id, hour) DO UPDATE SET
        total = r.total + excluded.total,
        human = r.human + excluded.human,
        suspected_bot = r.suspected_bot + excluded.suspected_bot
    )
    SELECT count(*)::int AS inserted FROM inserted`;
  return result!.inserted;
}

type Options = { kafka: Kafka; sql: Sql; topic: string; groupId: string };

// The analytics consumer (PRD §6.2). Reads clicks.raw, labels each click with
// the user-agent rules, and stores it. Offsets are committed only after the
// database write, so a crash in between means the batch is read again, which
// ingest() makes harmless: at-least-once delivery, counted exactly once.
// Messages that don't parse go to `<topic>.dlq` instead of blocking the partition.
export function createClickConsumer({ kafka, sql, topic, groupId }: Options) {
  const consumer = kafka.consumer({ kafkaJS: { groupId, fromBeginning: true, autoCommit: false } });
  const dlq = kafka.producer();
  const partitions = createPartitionManager(sql);
  const stats: ConsumerStats = { events: 0, inserted: 0, duplicates: 0, invalid: 0 };
  let maintenance: NodeJS.Timeout | undefined;

  // A message is a JSON array of events (packed by the sink) or a single
  // event object (the earlier format, still accepted). Each unreadable event,
  // or message, goes to the DLQ on its own.
  async function handle(values: (Buffer | null)[]) {
    const valid = new Map<string, ClickEvent>();
    const invalid: string[] = [];
    let seen = 0;
    for (const value of values) {
      const text = value?.toString() ?? "";
      let items: unknown[];
      try {
        const json: unknown = JSON.parse(text);
        items = Array.isArray(json) ? json : [json];
      } catch {
        invalid.push(text);
        continue;
      }
      for (const item of items) {
        const parsed = clickEventSchema.safeParse(item);
        if (parsed.success) {
          seen++;
          valid.set(parsed.data.event_id, parsed.data);
        } else {
          invalid.push(JSON.stringify(item));
        }
      }
    }
    const events = [...valid.values()];
    await partitions.ensure(new Set(events.map((e) => new Date(e.ts).toISOString().slice(0, 10))));
    const inserted = events.length ? await ingest(sql, events) : 0;
    if (invalid.length) await dlq.send({ topic: `${topic}.dlq`, messages: invalid.map((value) => ({ value })) });

    stats.events += seen;
    stats.inserted += inserted;
    stats.invalid += invalid.length;
    stats.duplicates += seen - inserted;
  }

  return {
    async start() {
      await partitions.maintain();
      maintenance = setInterval(() => partitions.maintain().catch((err) => console.error("partition maintenance failed", err)), 3_600_000);
      maintenance.unref();
      await dlq.connect();
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });
      await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
          if (batch.messages.length === 0) return;
          await handle(batch.messages.map((m) => m.value));
          const last = batch.messages.at(-1)!.offset;
          resolveOffset(last);
          await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(last) + 1n).toString() }]);
          await heartbeat();
        },
      });
    },

    stats: (): ConsumerStats => ({ ...stats }),

    async stop() {
      clearInterval(maintenance);
      await consumer.disconnect().catch(() => {});
      await dlq.disconnect().catch(() => {});
    },
  };
}
