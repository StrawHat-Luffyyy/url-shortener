import { Redis } from "ioredis";
import postgres from "postgres";
import { afterAll, describe, expect, it } from "vitest";
import { createApp } from "../src/app";
import { BASE_URL, OPERATOR_TOKEN, setupApp, type Link } from "./helpers";

const { call, callOn, newInstance, createTenant } = setupApp();

async function createLink(token: string, body: Record<string, unknown>) {
  const res = await call("POST", "/v1/links", { token, body: { url: "https://example.com/", ...body } });
  return ((await res.json()) as { link: Link }).link;
}

describe("max clicks", () => {
  it("redirects up to the limit, then the link is gone", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key, { maxClicks: 3 });

    const statuses = [];
    for (let i = 0; i < 5; i++) statuses.push((await call("GET", `/${link.code}`)).status);

    expect(statuses).toEqual([302, 302, 302, 410, 410]);
  });

  it("the limit is shared across instances", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key, { maxClicks: 2 });
    const a = newInstance();
    const b = newInstance();

    expect((await callOn(a, "GET", `/${link.code}`)).status).toBe(302);
    expect((await callOn(b, "GET", `/${link.code}`)).status).toBe(302);
    expect((await callOn(a, "GET", `/${link.code}`)).status).toBe(410);
  });

  it("holds under concurrent clicks: exactly the limit get through", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key, { maxClicks: 10 });

    const statuses = await Promise.all(Array.from({ length: 50 }, async () => (await call("GET", `/${link.code}`)).status));

    expect(statuses.filter((s) => s === 302)).toHaveLength(10);
    expect(statuses.filter((s) => s === 410)).toHaveLength(40);
  });

  it("links without a limit are unaffected", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key, {});

    const statuses = [];
    for (let i = 0; i < 5; i++) statuses.push((await call("GET", `/${link.code}`)).status);

    expect(statuses).toEqual(Array(5).fill(302));
  });

  it("isn't enforced while Redis is unreachable, so redirects keep working", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key, { maxClicks: 1 });
    const sql = postgres(process.env.TEST_DATABASE_URL!, { max: 2, onnotice: () => {} });
    const deadRedis = new Redis("redis://127.0.0.1:1", { lazyConnect: true, enableOfflineQueue: false, maxRetriesPerRequest: 0 });
    afterAll(async () => {
      await sql.end();
      deadRedis.disconnect();
    });
    const isolated = createApp({ sql, redis: deadRedis, operatorToken: OPERATOR_TOKEN, baseUrl: BASE_URL });

    expect((await callOn(isolated, "GET", `/${link.code}`)).status).toBe(302);
    expect((await callOn(isolated, "GET", `/${link.code}`)).status).toBe(302);
  });
});
