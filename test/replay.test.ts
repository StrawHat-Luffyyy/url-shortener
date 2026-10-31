import { randomUUID } from "node:crypto";
import postgres from "postgres";
import { afterAll, describe, expect, it } from "vitest";
import type { ClickEvent } from "../src/analytics/emitter";
import { eventually, setupClickPipeline } from "./kafka-helpers";
import { setupApp, type Link } from "./helpers";

const { call, createTenant } = setupApp();
const sql = postgres(process.env.TEST_DATABASE_URL!, { max: 5, onnotice: () => {} });
afterAll(() => sql.end());
const pipeline = await setupClickPipeline(sql);
// A topic of its own, so it holds only that test's messages.
const formats = await setupClickPipeline(sql);

async function totals(token: string, code: string, view = "all") {
  const res = await call("GET", `/v1/links/${code}/analytics?view=${view}`, { token });
  return ((await res.json()) as { totals: { clicks: number; uniqueVisitors: number } }).totals;
}

// Phase 3 exit criterion: reprocessing the stream must not double count.
describe("replaying click events", () => {
  it("re-consuming the whole topic, and duplicate deliveries, leave every count unchanged", { timeout: 120_000 }, async () => {
    const { tenant, apiKey } = await createTenant();
    const links: Link[] = [];
    for (let i = 0; i < 3; i++) {
      const res = await call("POST", "/v1/links", { token: apiKey.key, body: { url: `https://example.com/${i}` } });
      links.push(((await res.json()) as { link: Link }).link);
    }
    const linkIds = await Promise.all(
      links.map(async (l) => {
        // The analytics API is keyed by code; events carry the link's id.
        const [row] = await sql<{ id: string }[]>`SELECT id::text FROM links WHERE code = ${l.code}`;
        return row!.id;
      }),
    );

    // 300 distinct events across 3 links, 30 visitors, a third of them bots;
    // 50 of them are then delivered a second time, as a producer retry would.
    const events: ClickEvent[] = Array.from({ length: 300 }, (_, i) => ({
      event_id: randomUUID(),
      link_id: linkIds[i % 3]!,
      tenant_id: tenant.id,
      ts: new Date(Date.now() - (i % 5) * 3_600_000).toISOString(),
      ip_hash: `visitor-${i % 30}`,
      asn: null,
      country: i % 2 ? "IN" : "DE",
      user_agent: i % 3 === 0 ? "Googlebot/2.1" : "Mozilla/5.0 (X11; Linux x86_64) Firefox/140.0",
      referrer: null,
      accept_language: null,
      is_datacenter_asn: null,
      response_code: 302,
    }));
    const delivered = [...events, ...events.slice(0, 50)];
    await pipeline.sink(delivered);

    const groupId = `${pipeline.topic}.replay`;
    const first = await pipeline.startConsumer(groupId);
    await eventually(async () => first.stats(), (s) => s.events >= delivered.length, 60_000);
    const snapshot = async () =>
      Promise.all(links.flatMap((l) => ["all", "human", "bot"].map((view) => totals(apiKey.key, l.code, view))));
    const before = await snapshot();
    expect(first.stats()).toMatchObject({ events: 350, inserted: 300, duplicates: 50 });
    expect(before.filter((_, i) => i % 3 === 0).reduce((n, t) => n + t.clicks, 0)).toBe(300);

    // Rewind: drop the group's committed offsets and consume everything again.
    await first.stop();
    const admin = pipeline.kafka.admin();
    await admin.connect();
    await admin.deleteGroups([groupId]);
    await admin.disconnect();
    const second = await pipeline.startConsumer(groupId);
    await eventually(async () => second.stats(), (s) => s.events >= delivered.length, 60_000);

    expect(second.stats()).toMatchObject({ events: 350, inserted: 0, duplicates: 350 });
    expect(await snapshot()).toEqual(before);
  });

  it("reads single-event messages (the old format), packed ones, and sends unreadable ones to the DLQ", { timeout: 60_000 }, async () => {
    const { tenant, apiKey } = await createTenant();
    const res = await call("POST", "/v1/links", { token: apiKey.key, body: { url: "https://example.com/" } });
    const link = ((await res.json()) as { link: Link }).link;
    const [row] = await sql<{ id: string }[]>`SELECT id::text FROM links WHERE code = ${link.code}`;
    const event = (): ClickEvent => ({
      event_id: randomUUID(),
      link_id: row!.id,
      tenant_id: tenant.id,
      ts: new Date().toISOString(),
      ip_hash: randomUUID(),
      asn: null,
      country: null,
      user_agent: "Mozilla/5.0 (X11; Linux x86_64) Firefox/140.0",
      referrer: null,
      accept_language: null,
      is_datacenter_asn: null,
      response_code: 302,
    });
    const producer = formats.kafka.producer();
    await producer.connect();
    await producer.send({
      topic: formats.topic,
      messages: [
        { key: row!.id, value: JSON.stringify(event()) },
        { key: row!.id, value: JSON.stringify([event(), event()]) },
        { key: row!.id, value: "not json" },
        { key: row!.id, value: JSON.stringify([event(), { event_id: "bad" }]) },
      ],
    });
    await producer.disconnect();

    const consumer = await formats.startConsumer();
    await eventually(async () => consumer.stats(), (s) => s.events + s.invalid >= 6, 60_000);

    expect(consumer.stats()).toMatchObject({ inserted: 4, invalid: 2 });
    expect(await totals(apiKey.key, link.code)).toEqual({ clicks: 4, uniqueVisitors: 4 });
  });
});
