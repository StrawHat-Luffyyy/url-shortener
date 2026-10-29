import type { Redis } from "ioredis";
import type { Sql } from "postgres";

// Highest id handed out so far, across all instances. It counts ids, not
// blocks, so instances with different block sizes can share it.
const COUNTER_KEY = "links:id_counter";
// Ids to skip past the highest id in Postgres when the counter has to be
// rebuilt. Instances may hold leased ids above that one that aren't stored yet;
// skipping a million ids (of a 3.5 trillion space) clears them.
const RESEED_HEADROOM = 1_000_000;

// If the counter is missing or below ARGV[1] (the highest stored id), it was
// lost or restored from an old snapshot: move it past ARGV[1] plus headroom.
// A healthy counter is left alone.
const RESEED = `
local cur = tonumber(redis.call('GET', KEYS[1]))
if cur == nil or cur < tonumber(ARGV[1]) then
  redis.call('SET', KEYS[1], tonumber(ARGV[1]) + tonumber(ARGV[2]))
end
return 0`;

export type IdAllocator = { next(): Promise<number> };

// Hands out unique link ids without a round trip per id: each instance leases a
// block of `blockSize` ids with one INCRBY on a shared Redis counter, then
// serves ids from memory. Ids left in a block when an instance stops are never
// used; those gaps are fine.
//
// Postgres is the source of truth for ids already used. The counter is checked
// against it before the first lease, and again whenever a lease goes backwards
// (Redis lost the counter while this instance was running).
//
// Don't call next() while holding a pooled connection: a lease can need one.
export function createIdAllocator(redis: Redis, sql: Sql, blockSize = 1000): IdAllocator {
  let nextId = 0;
  let blockEnd = 0;
  let leasedOnce = false;
  let leasing: Promise<void> | null = null;

  async function reseed() {
    const [row] = await sql<{ max: string | null }[]>`SELECT max(id)::text AS max FROM links`;
    await redis.eval(RESEED, 1, COUNTER_KEY, row?.max ?? "0", RESEED_HEADROOM);
  }

  async function lease() {
    if (!leasedOnce) await reseed();
    let end = await redis.incrby(COUNTER_KEY, blockSize);
    if (leasedOnce && end - blockSize < blockEnd) {
      await reseed();
      end = await redis.incrby(COUNTER_KEY, blockSize);
    }
    leasedOnce = true;
    nextId = end - blockSize + 1;
    blockEnd = end + 1;
  }

  return {
    async next() {
      // Concurrent callers share one in-flight lease instead of each leasing a block.
      while (nextId >= blockEnd) {
        leasing ??= lease().finally(() => (leasing = null));
        await leasing;
      }
      return nextId++;
    },
  };
}
