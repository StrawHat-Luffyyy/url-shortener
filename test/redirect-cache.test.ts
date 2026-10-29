import { randomBytes } from "node:crypto";
import { Redis } from "ioredis";
import postgres from "postgres";
import { afterAll, describe, expect, it } from "vitest";
import { createApp } from "../src/app";
import { BASE_URL, OPERATOR_TOKEN, setupApp, type Link } from "./helpers";

const { call, callOn, newInstance, createTenant } = setupApp();

async function createLink(token: string, body: Record<string, unknown>) {
  const res = await call("POST", "/v1/links", { token, body });
  return ((await res.json()) as { link: Link }).link;
}

describe("redirects with the cache", () => {
  it("a code that was looked up before an alias claimed it redirects once the alias exists", async () => {
    const { apiKey } = await createTenant();
    const alias = `late-${randomBytes(5).toString("hex")}`;
    expect((await call("GET", `/${alias}`)).status).toBe(404);

    await createLink(apiKey.key, { url: "https://example.com/late", alias });

    expect((await call("GET", `/${alias}`)).headers.get("location")).toBe("https://example.com/late");
  });

  it("an update made through one instance takes effect on another instance's next redirect", async () => {
    const { apiKey } = await createTenant();
    const other = newInstance();
    const link = await createLink(apiKey.key, { url: "https://old.example/" });
    expect((await callOn(other, "GET", `/${link.code}`)).headers.get("location")).toBe("https://old.example/");

    await call("PATCH", `/v1/links/${link.code}`, { token: apiKey.key, body: { url: "https://new.example/" } });

    expect((await callOn(other, "GET", `/${link.code}`)).headers.get("location")).toBe("https://new.example/");
  });

  it("a cached link still expires on time", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key, {
      url: "https://example.com/",
      expiresAt: new Date(Date.now() + 1500).toISOString(),
    });
    expect((await call("GET", `/${link.code}`)).status).toBe(302);

    await new Promise((r) => setTimeout(r, 1600));

    expect((await call("GET", `/${link.code}`)).status).toBe(410);
  });

  it("redirects keep working when Redis is unreachable", async () => {
    const { apiKey } = await createTenant();
    const link = await createLink(apiKey.key, { url: "https://example.com/up" });
    const sql = postgres(process.env.TEST_DATABASE_URL!, { max: 2, onnotice: () => {} });
    const deadRedis = new Redis("redis://127.0.0.1:1", { lazyConnect: true, enableOfflineQueue: false, maxRetriesPerRequest: 0 });
    afterAll(async () => {
      await sql.end();
      deadRedis.disconnect();
    });
    const isolated = createApp({ sql, redis: deadRedis, operatorToken: OPERATOR_TOKEN, baseUrl: BASE_URL });

    const res = await callOn(isolated, "GET", `/${link.code}`);

    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe("https://example.com/up");
    expect((await callOn(isolated, "GET", "/zzzzzzz")).status).toBe(404);
  });
});
