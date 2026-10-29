import type { Redis } from "ioredis";

export type Bucket = { capacity: number; refillPerSecond: number };
export type RateLimiter = {
  // Takes one token from the bucket at `key`. When none is left, returns how
  // long until one will be.
  take(key: string, bucket: Bucket): Promise<{ allowed: true } | { allowed: false; retryAfterMs: number }>;
};

// Token bucket (FR-16). Uses the Redis server's clock, so instances with skewed
// clocks still agree.
const TAKE = `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or now
tokens = math.min(capacity, tokens + (now - ts) * rate / 1000)
local wait = 0
if tokens >= 1 then tokens = tokens - 1 else wait = math.ceil((1 - tokens) * 1000 / rate) end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 1000 / rate) + 1000)
return wait`;

// The same bucket kept in this process's memory.
function createLocalLimiter() {
  const buckets = new Map<string, { tokens: number; ts: number }>();
  return (key: string, { capacity, refillPerSecond }: Bucket) => {
    const now = Date.now();
    const state = buckets.get(key) ?? { tokens: capacity, ts: now };
    state.tokens = Math.min(capacity, state.tokens + ((now - state.ts) * refillPerSecond) / 1000);
    state.ts = now;
    buckets.set(key, state);
    if (state.tokens >= 1) {
      state.tokens -= 1;
      return 0;
    }
    return Math.ceil(((1 - state.tokens) * 1000) / refillPerSecond);
  };
}

// Limits are shared by all instances through Redis. If Redis is unreachable
// each instance limits on its own (PRD §7), so the effective limit is up to
// N× looser with N instances.
export function createRateLimiter(redis: Redis): RateLimiter {
  const local = createLocalLimiter();
  return {
    async take(key, bucket) {
      const waitMs = await redis
        .eval(TAKE, 1, `rl:${key}`, bucket.capacity, bucket.refillPerSecond)
        .then(Number)
        .catch(() => local(key, bucket));
      return waitMs === 0 ? { allowed: true } : { allowed: false, retryAfterMs: waitMs };
    },
  };
}
