import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { setupApp, type Link } from "./helpers";

const { call, createTenant } = setupApp({ poolSize: 10 });

async function createLink(token: string, body: Record<string, unknown>, idempotencyKey?: string) {
  const headers = idempotencyKey ? { "idempotency-key": idempotencyKey } : undefined;
  const res = await call("POST", "/v1/links", { token, body, headers });
  return { res, body: (await res.json()) as { link: Link; error?: { code: string } } };
}

describe("idempotent creation", () => {
  it("repeating a create with the same Idempotency-Key returns the same link", async () => {
    const { apiKey } = await createTenant();
    const key = randomUUID();

    const first = await createLink(apiKey.key, { url: "https://example.com/" }, key);
    const second = await createLink(apiKey.key, { url: "https://example.com/" }, key);

    expect(first.res.status).toBe(201);
    expect(second.res.status).toBe(201);
    expect(second.body.link.code).toBe(first.body.link.code);
  });

  it("concurrent repeats of one key create exactly one link", async () => {
    const { apiKey } = await createTenant();
    const key = randomUUID();

    const results = await Promise.all(
      Array.from({ length: 20 }, () => createLink(apiKey.key, { url: "https://example.com/" }, key)),
    );

    expect(results.map((r) => r.res.status)).toEqual(Array(20).fill(201));
    expect(new Set(results.map((r) => r.body.link.code)).size).toBe(1);
  });

  it("reusing a key for a different request is a 422", async () => {
    const { apiKey } = await createTenant();
    const key = randomUUID();
    await createLink(apiKey.key, { url: "https://example.com/a" }, key);

    const { res, body } = await createLink(apiKey.key, { url: "https://example.com/b" }, key);

    expect(res.status).toBe(422);
    expect(body.error?.code).toBe("IDEMPOTENCY_KEY_REUSED");
  });

  it("the same key from two tenants creates two links", async () => {
    const alice = await createTenant("Alice");
    const bob = await createTenant("Bob");
    const key = randomUUID();

    const a = await createLink(alice.apiKey.key, { url: "https://example.com/" }, key);
    const b = await createLink(bob.apiKey.key, { url: "https://example.com/" }, key);

    expect(a.body.link.code).not.toBe(b.body.link.code);
  });

  it("without a key, repeats create separate links", async () => {
    const { apiKey } = await createTenant();

    const a = await createLink(apiKey.key, { url: "https://example.com/" });
    const b = await createLink(apiKey.key, { url: "https://example.com/" });

    expect(a.body.link.code).not.toBe(b.body.link.code);
  });
});

describe("reading, updating and deleting links", () => {
  it("the owner can read a link", async () => {
    const { apiKey } = await createTenant();
    const { body } = await createLink(apiKey.key, { url: "https://example.com/", maxClicks: 100 });

    const res = await call("GET", `/v1/links/${body.link.code}`, { token: apiKey.key });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ link: { code: body.link.code, maxClicks: 100 } });
  });

  it("updating the destination changes where the code redirects", async () => {
    const { apiKey } = await createTenant();
    const { body } = await createLink(apiKey.key, { url: "https://old.example/" });

    const res = await call("PATCH", `/v1/links/${body.link.code}`, {
      token: apiKey.key,
      body: { url: "https://new.example/" },
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ link: { url: "https://new.example/" } });
    expect((await call("GET", `/${body.link.code}`)).headers.get("location")).toBe("https://new.example/");
  });

  it("an update with an unsafe destination is rejected", async () => {
    const { apiKey } = await createTenant();
    const { body } = await createLink(apiKey.key, { url: "https://example.com/" });

    const res = await call("PATCH", `/v1/links/${body.link.code}`, {
      token: apiKey.key,
      body: { url: "javascript:alert(1)" },
    });

    expect(res.status).toBe(400);
  });

  it("a deactivated link is 410 and can be reactivated", async () => {
    const { apiKey } = await createTenant();
    const { body } = await createLink(apiKey.key, { url: "https://example.com/" });
    const patch = (active: boolean) =>
      call("PATCH", `/v1/links/${body.link.code}`, { token: apiKey.key, body: { active } });

    await patch(false);
    expect((await call("GET", `/${body.link.code}`)).status).toBe(410);

    await patch(true);
    expect((await call("GET", `/${body.link.code}`)).status).toBe(302);
  });

  it("clearing the expiry with null makes an expiring link permanent", async () => {
    const { apiKey } = await createTenant();
    const expiresAt = new Date(Date.now() + 3_600_000).toISOString();
    const { body } = await createLink(apiKey.key, { url: "https://example.com/", expiresAt });

    const res = await call("PATCH", `/v1/links/${body.link.code}`, { token: apiKey.key, body: { expiresAt: null } });

    expect(await res.json()).toMatchObject({ link: { expiresAt: null } });
  });

  it("a deleted link is 410 on redirect and 404 to its owner", async () => {
    const { apiKey } = await createTenant();
    const { body } = await createLink(apiKey.key, { url: "https://example.com/" });

    const del = await call("DELETE", `/v1/links/${body.link.code}`, { token: apiKey.key });

    expect(del.status).toBe(204);
    expect((await call("GET", `/${body.link.code}`)).status).toBe(410);
    expect((await call("GET", `/v1/links/${body.link.code}`, { token: apiKey.key })).status).toBe(404);
    expect((await call("DELETE", `/v1/links/${body.link.code}`, { token: apiKey.key })).status).toBe(404);
  });

  it("another tenant can't read, update or delete the link", async () => {
    const alice = await createTenant("Alice");
    const bob = await createTenant("Bob");
    const { body } = await createLink(alice.apiKey.key, { url: "https://alice.example/" });
    const path = `/v1/links/${body.link.code}`;

    expect((await call("GET", path, { token: bob.apiKey.key })).status).toBe(404);
    expect((await call("PATCH", path, { token: bob.apiKey.key, body: { url: "https://bob.example/" } })).status).toBe(404);
    expect((await call("DELETE", path, { token: bob.apiKey.key })).status).toBe(404);
    expect((await call("GET", `/${body.link.code}`)).headers.get("location")).toBe("https://alice.example/");
  });
});
