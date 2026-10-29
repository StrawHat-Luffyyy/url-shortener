import { describe, expect, it } from "vitest";
import { BASE_URL, setupApp, type Link } from "./helpers";

const { call, createTenant } = setupApp();

async function createLink(token: string, body: Record<string, unknown>, headers?: Record<string, string>) {
  const res = await call("POST", "/v1/links", { token, body, headers });
  return { res, body: (await res.json()) as { link: Link; error?: { code: string } } };
}

describe("creating a link", () => {
  it("returns a 7-character code that redirects to the destination with a 302", async () => {
    const { apiKey } = await createTenant();

    const { res, body } = await createLink(apiKey.key, { url: "https://example.com/some/page?x=1" });

    expect(res.status).toBe(201);
    expect(body.link).toMatchObject({
      url: "https://example.com/some/page?x=1",
      status: "active",
      redirectType: 302,
    });
    expect(body.link.code).toMatch(/^[0-9A-Za-z]{7}$/);
    expect(body.link.shortUrl).toBe(`${BASE_URL}/${body.link.code}`);

    const redirect = await call("GET", `/${body.link.code}`);
    expect(redirect.status).toBe(302);
    expect(redirect.headers.get("location")).toBe("https://example.com/some/page?x=1");
  });

  it("consecutive links get codes that don't look sequential", async () => {
    const { apiKey } = await createTenant();

    const codes: string[] = [];
    for (let i = 0; i < 5; i++) codes.push((await createLink(apiKey.key, { url: "https://example.com/" })).body.link.code);

    // Sequential IDs encoded directly would share all but the last character.
    const sharedPrefix = (a: string, b: string) => [...a].findIndex((ch, i) => ch !== b[i]);
    for (let i = 1; i < codes.length; i++) expect(sharedPrefix(codes[i - 1]!, codes[i]!)).toBeLessThan(5);
  });

  it.each([
    ["javascript:alert(1)", "UNSUPPORTED_SCHEME"],
    ["not a url", "INVALID_URL"],
    ["https://user:pw@example.com/", "URL_HAS_CREDENTIALS"],
  ])("rejects %s with %s", async (url, code) => {
    const { apiKey } = await createTenant();

    const { res, body } = await createLink(apiKey.key, { url });

    expect(res.status).toBe(400);
    expect(body.error?.code).toBe(code);
  });

  it("requires an API key", async () => {
    const res = await call("POST", "/v1/links", { body: { url: "https://example.com/" } });
    expect(res.status).toBe(401);
  });
});

describe("redirecting", () => {
  it("unknown codes are a 404", async () => {
    const res = await call("GET", "/zzzzzzz");
    expect(res.status).toBe(404);
  });

  it("a link created with redirectType 301 redirects permanently", async () => {
    const { apiKey } = await createTenant();
    const { body } = await createLink(apiKey.key, { url: "https://example.com/", redirectType: 301 });

    const res = await call("GET", `/${body.link.code}`);

    expect(res.status).toBe(301);
  });

  it("an expired link is 410 Gone", async () => {
    const { apiKey } = await createTenant();
    const expiresAt = new Date(Date.now() + 1500).toISOString();
    const { body } = await createLink(apiKey.key, { url: "https://example.com/", expiresAt });
    expect((await call("GET", `/${body.link.code}`)).status).toBe(302);

    await new Promise((r) => setTimeout(r, 1600));

    expect((await call("GET", `/${body.link.code}`)).status).toBe(410);
  });

  it("an expiry in the past is rejected", async () => {
    const { apiKey } = await createTenant();

    const { res, body } = await createLink(apiKey.key, {
      url: "https://example.com/",
      expiresAt: new Date(Date.now() - 60_000).toISOString(),
    });

    expect(res.status).toBe(400);
    expect(body.error?.code).toBe("VALIDATION_FAILED");
  });
});
