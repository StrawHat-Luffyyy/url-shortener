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
    await pipeline.sink(delivered.map((e) => ({ key: e.link_id, value: JSON.stringify(e) })));

    const groupId = `${pipeline.topic}.replay`;
    const first = await pipeline.startConsumer(groupId);
    await eventually(async () => first.stats(), (s) => s.messages >= delivered.length, 60_000);
    const snapshot = async () =>
      Promise.all(links.flatMap((l) => ["all", "human", "bot"].map((view) => totals(apiKey.key, l.code, view))));
    const before = await snapshot();
    expect(first.stats()).toMatchObject({ messages: 350, inserted: 300, duplicates: 50 });
    expect(before.filter((_, i) => i % 3 === 0).reduce((n, t) => n + t.clicks, 0)).toBe(300);

    // Rewind: drop the group's committed offsets and consume everything again.
    await first.stop();
    const admin = pipeline.kafka.admin();
    await admin.connect();
    await admin.deleteGroups([groupId]);
    await admin.disconnect();
    const second = await pipeline.startConsumer(groupId);
    await eventually(async () => second.stats(), (s) => s.messages >= delivered.length, 60_000);

    expect(second.stats()).toMatchObject({ messages: 350, inserted: 0, duplicates: 350 });
    expect(await snapshot()).toEqual(before);
  });
});
