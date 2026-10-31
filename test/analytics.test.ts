import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { eventually, setupClickPipeline } from "./kafka-helpers";
import { setupApp, type Link } from "./helpers";

const { call, callOn, newInstance, createTenant } = setupApp();
const sql = postgres(process.env.TEST_DATABASE_URL!, { max: 5, onnotice: () => {} });
afterAll(() => sql.end());
const pipeline = await setupClickPipeline(sql);
const app = newInstance({ clickTracking: { emitter: pipeline.emitter, ipHashSalt: "test-salt", trustProxy: true } });
beforeAll(() => pipeline.startConsumer(), 60_000);

const CHROME_DESKTOP = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36";
const SAFARI_IPHONE =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1";
const GOOGLEBOT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";

type Analytics = {
  totals: { clicks: number; uniqueVisitors: number };
  series: { start: string; clicks: number }[];
  top: Record<"countries" | "referrers" | "browsers" | "devices", { value: string; clicks: number }[]>;
};

async function createLink(token: string) {
  const res = await call("POST", "/v1/links", { token, body: { url: "https://example.com/" } });
  return ((await res.json()) as { link: Link }).link;
}

function click(code: string, headers: Record<string, string>) {
  return callOn(app, "GET", `/${code}`, { headers });
}

async function analytics(token: string, code: string, query = "") {
  const res = await call("GET", `/v1/links/${code}/analytics${query}`, { token });
  return { status: res.status, body: (await res.json()) as Analytics };
}

const waitForClicks = (token: string, code: string, n: number, query = "") =>
  eventually(
    () => analytics(token, code, query),
    (a) => a.body.totals?.clicks >= n,
  );

describe("click analytics", () => {
  it("counts clicks and unique visitors, with top countries, referrers, browsers and devices", { timeout: 60_000 }, async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key);
    const alice = { "x-forwarded-for": "203.0.113.1", "cf-ipcountry": "IN", "user-agent": CHROME_DESKTOP };
    const bob = { "x-forwarded-for": "203.0.113.2", "cf-ipcountry": "US", "user-agent": SAFARI_IPHONE };

    await click(link.code, { ...alice, referer: "https://news.example.org/story?id=1" });
    await click(link.code, alice);
    await click(link.code, bob);

    const { body } = await waitForClicks(apiKey.key, link.code, 3);
    expect(body.totals).toEqual({ clicks: 3, uniqueVisitors: 2 });
    expect(body.top.countries).toEqual([
      { value: "IN", clicks: 2 },
      { value: "US", clicks: 1 },
    ]);
    expect(body.top.referrers).toEqual([{ value: "news.example.org", clicks: 1 }]);
    expect(body.top.browsers).toEqual([
      { value: "Chrome", clicks: 2 },
      { value: "Safari", clicks: 1 },
    ]);
    expect(body.top.devices).toEqual([
      { value: "desktop", clicks: 2 },
      { value: "mobile", clicks: 1 },
    ]);
    expect(body.series.reduce((n, b) => n + b.clicks, 0)).toBe(3);
  });

  it("separates human and bot traffic", { timeout: 60_000 }, async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key);

    await click(link.code, { "x-forwarded-for": "198.51.100.1", "user-agent": CHROME_DESKTOP });
    await click(link.code, { "x-forwarded-for": "66.249.66.1", "user-agent": GOOGLEBOT });
    await click(link.code, { "x-forwarded-for": "198.51.100.9", "user-agent": "curl/8.9.1" });
    await waitForClicks(apiKey.key, link.code, 3);

    const human = await analytics(apiKey.key, link.code, "?view=human");
    const bot = await analytics(apiKey.key, link.code, "?view=bot");
    expect(human.body.totals.clicks).toBe(1);
    expect(bot.body.totals.clicks).toBe(2);
    expect(human.body.top.browsers).toEqual([{ value: "Chrome", clicks: 1 }]);
  });

  it("returns hourly or daily series, zero-filled", { timeout: 60_000 }, async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key);
    await click(link.code, { "x-forwarded-for": "192.0.2.1", "user-agent": CHROME_DESKTOP });
    await waitForClicks(apiKey.key, link.code, 1);
    const to = new Date();
    const from = new Date(to.getTime() - 3 * 86_400_000);

    const daily = await analytics(apiKey.key, link.code, `?granularity=day&from=${from.toISOString()}&to=${to.toISOString()}`);
    const hourly = await analytics(apiKey.key, link.code, `?granularity=hour&from=${from.toISOString()}&to=${to.toISOString()}`);

    expect(daily.body.series).toHaveLength(4);
    expect(daily.body.series.at(-1)!.clicks).toBe(1);
    expect(hourly.body.series.length).toBeGreaterThanOrEqual(72);
    expect(hourly.body.series.filter((b) => b.clicks > 0)).toHaveLength(1);
  });

  it("another tenant can't see a link's analytics", { timeout: 60_000 }, async () => {
    const owner = await createTenant("Owner");
    const other = await createTenant("Other");
    const link = await createLink(owner.apiKey.key);

    const res = await analytics(other.apiKey.key, link.code);

    expect(res.status).toBe(404);
  });

  it("rejects an unknown view", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key);

    expect((await analytics(apiKey.key, link.code, "?view=robots")).status).toBe(400);
  });
});
