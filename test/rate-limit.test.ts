import { Redis } from "ioredis";
import postgres from "postgres";
import { afterAll, describe, expect, it } from "vitest";
import { createApp } from "../src/app";
import { BASE_URL, OPERATOR_TOKEN, setupApp } from "./helpers";

const { callOn, newInstance, createTenant } = setupApp();

const tight = { create: { capacity: 3, refillPerSecond: 2 }, api: { capacity: 5, refillPerSecond: 2 } };

const createLink = (app: ReturnType<typeof newInstance>, token: string) =>
  callOn(app, "POST", "/v1/links", { token, body: { url: "https://example.com/" } });

describe("per-tenant rate limits", () => {
  it("link creation beyond the burst is a 429 with Retry-After", async () => {
    const app = newInstance({ rateLimits: tight });
    const { apiKey } = await createTenant();

    const statuses = [];
    for (let i = 0; i < 4; i++) statuses.push((await createLink(app, apiKey.key)).status);
    const limited = await createLink(app, apiKey.key);

    expect(statuses.slice(0, 3)).toEqual([201, 201, 201]);
    expect(limited.status).toBe(429);
    expect(await limited.json()).toMatchObject({ error: { code: "RATE_LIMITED" } });
    expect(Number(limited.headers.get("retry-after"))).toBeGreaterThanOrEqual(1);
  });

  it("the bucket refills over time", async () => {
    const app = newInstance({ rateLimits: tight });
    const { apiKey } = await createTenant();
    for (let i = 0; i < 3; i++) await createLink(app, apiKey.key);
    expect((await createLink(app, apiKey.key)).status).toBe(429);

    await new Promise((r) => setTimeout(r, 600)); // 2 tokens/s → at least one token back

    expect((await createLink(app, apiKey.key)).status).toBe(201);
  });

  it("one tenant hitting its limit doesn't affect another", async () => {
    const app = newInstance({ rateLimits: tight });
    const noisy = await createTenant("Noisy");
    const quiet = await createTenant("Quiet");
    for (let i = 0; i < 4; i++) await createLink(app, noisy.apiKey.key);

    expect((await createLink(app, quiet.apiKey.key)).status).toBe(201);
  });

  it("the limit is shared across API instances", async () => {
    const a = newInstance({ rateLimits: tight });
    const b = newInstance({ rateLimits: tight });
    const { apiKey } = await createTenant();
    for (let i = 0; i < 3; i++) await createLink(a, apiKey.key);

    expect((await createLink(b, apiKey.key)).status).toBe(429);
  });

  it("other API calls have their own bucket", async () => {
    const app = newInstance({ rateLimits: tight });
    const { apiKey } = await createTenant();
    for (let i = 0; i < 3; i++) await createLink(app, apiKey.key);

    const reads = [];
    for (let i = 0; i < 6; i++) reads.push((await callOn(app, "GET", "/v1/keys", { token: apiKey.key })).status);

    expect(reads).toEqual([200, 200, 200, 200, 200, 429]);
  });

  it("redirects are not rate limited per tenant", async () => {
    const app = newInstance({ rateLimits: tight });
    const { apiKey } = await createTenant();
    const res = await createLink(app, apiKey.key);
    const { link } = (await res.json()) as { link: { code: string } };

    const statuses = [];
    for (let i = 0; i < 10; i++) statuses.push((await callOn(app, "GET", `/${link.code}`)).status);

    expect(statuses).toEqual(Array(10).fill(302));
  });

  it("still limits, per instance, when Redis is unreachable", async () => {
    const sql = postgres(process.env.TEST_DATABASE_URL!, { max: 2, onnotice: () => {} });
    const deadRedis = new Redis("redis://127.0.0.1:1", { lazyConnect: true, enableOfflineQueue: false, maxRetriesPerRequest: 0 });
    afterAll(async () => {
      await sql.end();
      deadRedis.disconnect();
    });
    const isolated = createApp({ sql, redis: deadRedis, operatorToken: OPERATOR_TOKEN, baseUrl: BASE_URL, rateLimits: tight });
    const { apiKey } = await createTenant();

    const reads = [];
    for (let i = 0; i < 6; i++) reads.push((await callOn(isolated, "GET", "/v1/tenant", { token: apiKey.key })).status);

    expect(reads).toEqual([200, 200, 200, 200, 200, 429]);
  });
});
