import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { setupApp, type Link } from "./helpers";

const { call, createTenant } = setupApp({ poolSize: 10 });

// Aliases are global, so every test uses a fresh one.
const freshAlias = (prefix = "a") => `${prefix}-${randomBytes(6).toString("hex")}`;

async function createLink(token: string, body: Record<string, unknown>) {
  const res = await call("POST", "/v1/links", { token, body });
  return { res, body: (await res.json()) as { link: Link; error?: { code: string } } };
}

describe("custom aliases", () => {
  it("a link can be created and reached under a custom alias", async () => {
    const { apiKey } = await createTenant();
    const alias = freshAlias("Launch_Day");

    const { res, body } = await createLink(apiKey.key, { url: "https://example.com/launch", alias });

    expect(res.status).toBe(201);
    expect(body.link.code).toBe(alias);
    const redirect = await call("GET", `/${alias}`);
    expect(redirect.headers.get("location")).toBe("https://example.com/launch");
  });

  it("an alias that is taken, even by another tenant, is a 409", async () => {
    const alice = await createTenant("Alice");
    const bob = await createTenant("Bob");
    const alias = freshAlias();
    await createLink(alice.apiKey.key, { url: "https://alice.example/", alias });

    const { res, body } = await createLink(bob.apiKey.key, { url: "https://bob.example/", alias });

    expect(res.status).toBe(409);
    expect(body.error?.code).toBe("ALIAS_TAKEN");
    expect((await call("GET", `/${alias}`)).headers.get("location")).toBe("https://alice.example/");
  });

  it("parallel claims on one alias produce exactly one winner", async () => {
    const tenants = await Promise.all(Array.from({ length: 5 }, () => createTenant()));
    const alias = freshAlias();

    const results = await Promise.all(
      tenants.flatMap((t, i) =>
        Array.from({ length: 10 }, () => createLink(t.apiKey.key, { url: `https://t${i}.example/`, alias })),
      ),
    );

    const statuses = results.map((r) => r.res.status);
    expect(statuses.filter((s) => s === 201)).toHaveLength(1);
    expect(statuses.filter((s) => s === 409)).toHaveLength(49);
  });

  it.each([
    ["too short", "ab"],
    ["too long", "a".repeat(33)],
    ["a slash", "a/b-c"],
    ["a dot", "a.bcd"],
  ])("rejects an alias that is %s", async (_, alias) => {
    const { apiKey } = await createTenant();

    const { res, body } = await createLink(apiKey.key, { url: "https://example.com/", alias });

    expect(res.status).toBe(400);
    expect(body.error?.code).toBe("VALIDATION_FAILED");
  });

  it.each(["operator", "admin", "API", "Health"])("rejects the reserved word %s", async (alias) => {
    const { apiKey } = await createTenant();

    const { res, body } = await createLink(apiKey.key, { url: "https://example.com/", alias });

    expect(res.status).toBe(400);
    expect(body.error?.code).toBe("ALIAS_RESERVED");
  });
});
