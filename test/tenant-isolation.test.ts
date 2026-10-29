import { describe, expect, it } from "vitest";
import { setupApp } from "./helpers";

const { call, createTenant } = setupApp();

describe("tenant isolation", () => {
  it("a tenant cannot revoke another tenant's key", async () => {
    const alice = await createTenant("Alice");
    const bob = await createTenant("Bob");

    const res = await call("DELETE", `/v1/keys/${alice.apiKey.id}`, { token: bob.apiKey.key });

    expect(res.status).toBe(404);
    expect((await call("GET", "/v1/tenant", { token: alice.apiKey.key })).status).toBe(200);
  });

  it("a tenant's key list contains only its own keys", async () => {
    const alice = await createTenant("Alice");
    const bob = await createTenant("Bob");

    const res = await call("GET", "/v1/keys", { token: bob.apiKey.key });

    const { keys } = (await res.json()) as { keys: { id: string }[] };
    expect(keys.map((k) => k.id)).toEqual([bob.apiKey.id]);
    expect(keys.map((k) => k.id)).not.toContain(alice.apiKey.id);
  });

  it("each key resolves to its own tenant", async () => {
    const alice = await createTenant("Alice");
    const bob = await createTenant("Bob");

    const res = await call("GET", "/v1/tenant", { token: bob.apiKey.key });

    expect(await res.json()).toMatchObject({ tenant: { id: bob.tenant.id, name: "Bob" } });
    expect(bob.tenant.id).not.toBe(alice.tenant.id);
  });
});
