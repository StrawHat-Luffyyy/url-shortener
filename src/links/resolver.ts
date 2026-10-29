import type { Redis } from "ioredis";

// What the redirect handler needs to know about a link.
export type CachedLink = {
  url: string;
  status: string;
  redirectType: 301 | 302;
  active: boolean;
  expiresAt: number | null; // epoch ms; checked at request time, not baked into the TTL
};

export type LinkResolver = {
  // The link for `code`, or null if there is none.
  resolve(code: string): Promise<CachedLink | null>;
  // Call after any change to the link behind `code` (including creating it).
  invalidate(code: string): Promise<void>;
};

type Options = {
  redis: Redis;
  load: (code: string) => Promise<CachedLink | null>;
  ttlSeconds?: number;
  missTtlSeconds?: number;
};

const MISS = "0";
// Each invalidation bumps a per-code generation. A load only caches its result
// if the generation is unchanged since it started, so a load that read the old
// row just before a change can't cache it afterwards. Generations only need to
// outlive in-flight loads; they expire after an hour.
const GENERATION_TTL_SECONDS = 3600;

const FILL_IF_GENERATION_UNCHANGED = `
if (redis.call('GET', KEYS[2]) or '') == ARGV[3] then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
end
return 0`;

const cacheKey = (code: string) => `link:c:${code}`;
const generationKey = (code: string) => `link:gen:${code}`;

// Cache-aside lookup of links in Redis (PRD §6.3):
// - Unknown codes are cached briefly too, to absorb scans for codes that don't exist.
// - Concurrent misses for one code on this instance share a single load.
// - TTLs are a safety net; correctness comes from invalidate().
// - If Redis is unreachable every lookup goes to `load`: redirects slow down
//   but keep working. Cache errors never reach the caller.
export function createLinkResolver({ redis, load, ttlSeconds = 60, missTtlSeconds = 30 }: Options): LinkResolver {
  const inFlight = new Map<string, Promise<CachedLink | null>>();

  async function loadAndFill(code: string, generation: string) {
    const link = await load(code);
    const value = link ? JSON.stringify(link) : MISS;
    const ttl = link ? ttlSeconds : missTtlSeconds;
    await redis
      .eval(FILL_IF_GENERATION_UNCHANGED, 2, cacheKey(code), generationKey(code), value, ttl, generation)
      .catch(() => {});
    return link;
  }

  return {
    async resolve(code) {
      const [cached, generation] = await redis
        .mget(cacheKey(code), generationKey(code))
        .catch(() => [null, null] as (string | null)[]);
      if (cached === MISS) return null;
      if (cached) return JSON.parse(cached) as CachedLink;

      let pending = inFlight.get(code);
      if (!pending) {
        pending = loadAndFill(code, generation ?? "").finally(() => {
          if (inFlight.get(code) === pending) inFlight.delete(code);
        });
        inFlight.set(code, pending);
      }
      return pending;
    },

    async invalidate(code) {
      // A load already in flight here may hold the old value; later lookups
      // must start a new one.
      inFlight.delete(code);
      await redis
        .multi()
        .incr(generationKey(code))
        .expire(generationKey(code), GENERATION_TTL_SECONDS)
        .del(cacheKey(code))
        .exec()
        .catch((err) => console.error(`cache invalidation failed for ${code}; stale for up to ${ttlSeconds}s`, err));
    },
  };
}
