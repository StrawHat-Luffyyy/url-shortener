import { randomBytes } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import { createLinkResolver, type CachedLink } from "../src/links/resolver";

const redis = new Redis(process.env.TEST_REDIS_URL!);
afterAll(() => redis.disconnect());

const freshCode = () => `t${randomBytes(5).toString("hex")}`;
const link = (url: string): CachedLink => ({ url, status: "active", redirectType: 302, active: true, expiresAt: null });

// A stand-in for the Postgres lookup that counts calls and can be held open.
function fakeLoader(initial: Record<string, CachedLink> = {}) {
  const rows = new Map(Object.entries(initial));
  let gate: Promise<void> | null = null;
  let onLoad: (() => void) | null = null;
  const loader = {
    calls: 0,
    rows,
    // Makes the next loads wait. `started` resolves once one has read its row.
    hold() {
      let release!: () => void;
      gate = new Promise((r) => (release = r));
      const started = new Promise<void>((r) => (onLoad = r));
      return {
        started,
        release: () => {
          gate = null;
          release();
        },
      };
    },
    load: async (code: string) => {
      loader.calls++;
      const value = rows.get(code) ?? null; // read before waiting, like a query that already ran
      onLoad?.();
      if (gate) await gate;
      return value;
    },
  };
  return loader;
}

describe("link resolver", () => {
  it("loads a link once, then serves it from the cache", async () => {
    const code = freshCode();
    const loader = fakeLoader({ [code]: link("https://example.com/") });
    const resolver = createLinkResolver({ redis, load: loader.load });

    expect(await resolver.resolve(code)).toEqual(link("https://example.com/"));
    expect(await resolver.resolve(code)).toEqual(link("https://example.com/"));

    expect(loader.calls).toBe(1);
  });

  it("the cache is shared between instances", async () => {
    const code = freshCode();
    const loader = fakeLoader({ [code]: link("https://example.com/") });
    await createLinkResolver({ redis, load: loader.load }).resolve(code);

    await createLinkResolver({ redis, load: loader.load }).resolve(code);

    expect(loader.calls).toBe(1);
  });

  it("100 concurrent misses on one code do a single load", async () => {
    const code = freshCode();
    const loader = fakeLoader({ [code]: link("https://example.com/") });
    const resolver = createLinkResolver({ redis, load: loader.load });

    const results = await Promise.all(Array.from({ length: 100 }, () => resolver.resolve(code)));

    expect(loader.calls).toBe(1);
    expect(new Set(results.map((r) => r?.url))).toEqual(new Set(["https://example.com/"]));
  });

  it("caches unknown codes too, so repeated lookups don't reach the loader", async () => {
    const code = freshCode();
    const loader = fakeLoader();
    const resolver = createLinkResolver({ redis, load: loader.load });

    expect(await resolver.resolve(code)).toBeNull();
    expect(await resolver.resolve(code)).toBeNull();

    expect(loader.calls).toBe(1);
  });

  it("after invalidation the next resolve loads the current value", async () => {
    const code = freshCode();
    const loader = fakeLoader({ [code]: link("https://old.example/") });
    const resolver = createLinkResolver({ redis, load: loader.load });
    await resolver.resolve(code);

    loader.rows.set(code, link("https://new.example/"));
    await resolver.invalidate(code);

    expect((await resolver.resolve(code))?.url).toBe("https://new.example/");
  });

  it("invalidation clears a cached miss, e.g. when an alias is created for that code", async () => {
    const code = freshCode();
    const loader = fakeLoader();
    const resolver = createLinkResolver({ redis, load: loader.load });
    await resolver.resolve(code);

    loader.rows.set(code, link("https://example.com/"));
    await resolver.invalidate(code);

    expect((await resolver.resolve(code))?.url).toBe("https://example.com/");
  });

  it("a load that was in flight during an invalidation doesn't re-cache the old value", async () => {
    const code = freshCode();
    const loader = fakeLoader({ [code]: link("https://old.example/") });
    const other = createLinkResolver({ redis, load: loader.load });
    const held = loader.hold();
    const slow = other.resolve(code);
    await held.started; // it has read the old row but not cached it yet

    loader.rows.set(code, link("https://new.example/"));
    await createLinkResolver({ redis, load: loader.load }).invalidate(code);
    held.release();
    await slow;

    const fresh = createLinkResolver({ redis, load: loader.load });
    expect((await fresh.resolve(code))?.url).toBe("https://new.example/");
  });

  it("keeps resolving from the loader when Redis is unreachable", async () => {
    const down = new Redis("redis://127.0.0.1:1", { lazyConnect: true, enableOfflineQueue: false, maxRetriesPerRequest: 0 });
    afterAll(() => down.disconnect());
    const code = freshCode();
    const loader = fakeLoader({ [code]: link("https://example.com/") });
    const resolver = createLinkResolver({ redis: down, load: loader.load });

    expect((await resolver.resolve(code))?.url).toBe("https://example.com/");
    await expect(resolver.invalidate(code)).resolves.toBeUndefined();
  });
});
