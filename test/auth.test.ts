import { describe, expect, it } from "vitest";
import { setupApp } from "./helpers";

const { call, createTenant } = setupApp();

describe("API-key authentication", () => {
  it("a new tenant's key authenticates as that tenant", async () => {
    const { tenant, apiKey } = await createTenant("Acme Corp");

    const res = await call("GET", "/v1/tenant", { token: apiKey.key });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ tenant: { id: tenant.id, name: "Acme Corp" } });
  });

  it.each([
    ["no key", undefined],
    ["a malformed key", "not-a-key"],
  ])("rejects a request with %s", async (_, token) => {
    const res = await call("GET", "/v1/tenant", { token });

    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ error: { code: "UNAUTHORIZED", message: expect.any(String) } });
  });

  it("rejects a key with a real prefix but the wrong secret", async () => {
    const { apiKey } = await createTenant();
    const forged = apiKey.key.slice(0, 16) + "A".repeat(43);

    const res = await call("GET", "/v1/tenant", { token: forged });

    expect(res.status).toBe(401);
  });
});

describe("operator endpoints", () => {
  it.each([
    ["no token", undefined],
    ["the wrong token", "wrong-token"],
  ])("refuse to create a tenant with %s", async (_, token) => {
    const res = await call("POST", "/v1/operator/tenants", { token, body: { name: "Mallory" } });

    expect(res.status).toBe(401);
  });

  it("refuse a tenant API key in place of the operator token", async () => {
    const { apiKey } = await createTenant();

    const res = await call("POST", "/v1/operator/tenants", { token: apiKey.key, body: { name: "x" } });

    expect(res.status).toBe(401);
  });

  it("reject a tenant without a name", async () => {
    const res = await call("POST", "/v1/operator/tenants", {
      token: "test-operator-token",
      body: { name: "   " },
    });

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({ error: { code: "VALIDATION_FAILED" } });
  });
});
