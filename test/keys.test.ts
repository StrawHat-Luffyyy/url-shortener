import { describe, expect, it } from "vitest";
import { setupApp } from "./helpers";

const { call, createTenant } = setupApp();

describe("API-key management", () => {
  it("a tenant can issue another key, which also works", async () => {
    const { tenant, apiKey } = await createTenant();

    const res = await call("POST", "/v1/keys", { token: apiKey.key });
    expect(res.status).toBe(201);
    const { apiKey: second } = (await res.json()) as { apiKey: { key: string } };

    const me = await call("GET", "/v1/tenant", { token: second.key });
    expect(await me.json()).toMatchObject({ tenant: { id: tenant.id } });
  });

  it("listing keys shows prefixes but never the secret", async () => {
    const { apiKey } = await createTenant();

    const res = await call("GET", "/v1/keys", { token: apiKey.key });

    expect(res.status).toBe(200);
    const body = await res.text();
    expect(body).not.toContain(apiKey.key);
    const { keys } = JSON.parse(body) as { keys: { id: string; prefix: string; revokedAt: string | null }[] };
    expect(keys).toEqual([
      { id: apiKey.id, prefix: apiKey.key.slice(0, 15), revokedAt: null, createdAt: expect.any(String) },
    ]);
  });

  it("a revoked key stops working", async () => {
    const { apiKey } = await createTenant();
    const res = await call("POST", "/v1/keys", { token: apiKey.key });
    const { apiKey: spare } = (await res.json()) as { apiKey: { id: string; key: string } };

    const revoke = await call("DELETE", `/v1/keys/${spare.id}`, { token: apiKey.key });
    expect(revoke.status).toBe(204);

    expect((await call("GET", "/v1/tenant", { token: spare.key })).status).toBe(401);
    expect((await call("GET", "/v1/tenant", { token: apiKey.key })).status).toBe(200);
  });

  it("revoking an unknown key id is a 404", async () => {
    const { apiKey } = await createTenant();

    const res = await call("DELETE", "/v1/keys/00000000-0000-0000-0000-000000000000", { token: apiKey.key });

    expect(res.status).toBe(404);
  });
});
