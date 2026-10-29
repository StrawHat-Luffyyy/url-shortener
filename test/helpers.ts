import { Redis } from "ioredis";
import postgres from "postgres";
import { afterAll } from "vitest";
import { createApp, type AppDeps } from "../src/app";

export const OPERATOR_TOKEN = "test-operator-token";
export const BASE_URL = "https://sho.rt";
const UNLIMITED = { create: { capacity: 1e9, refillPerSecond: 1e9 }, api: { capacity: 1e9, refillPerSecond: 1e9 } };

type CallOpts = { token?: string; body?: unknown; headers?: Record<string, string> };

export function setupApp({ poolSize = 5 } = {}) {
  const sql = postgres(process.env.TEST_DATABASE_URL!, { max: poolSize, onnotice: () => {} });
  const redis = new Redis(process.env.TEST_REDIS_URL!);
  afterAll(async () => {
    await sql.end();
    redis.disconnect();
  });

  // Each call builds a separate app instance with its own ID allocator, like a
  // separate API process; they share the database and Redis.
  // Rate limits are effectively off unless a test passes its own.
  const newInstance = (opts: Pick<AppDeps, "idBlockSize" | "rateLimits"> = {}) =>
    createApp({ sql, redis, operatorToken: OPERATOR_TOKEN, baseUrl: BASE_URL, rateLimits: UNLIMITED, ...opts });
  const app = newInstance();

  function callOn(instance: ReturnType<typeof createApp>, method: string, path: string, opts: CallOpts = {}) {
    const headers: Record<string, string> = { ...opts.headers };
    if (opts.token) headers.authorization = `Bearer ${opts.token}`;
    if (opts.body !== undefined) headers["content-type"] = "application/json";
    return instance.request(path, {
      method,
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
    });
  }
  const call = (method: string, path: string, opts?: CallOpts) => callOn(app, method, path, opts);

  async function createTenant(name = "Acme") {
    const res = await call("POST", "/v1/operator/tenants", {
      token: OPERATOR_TOKEN,
      body: { name },
    });
    if (res.status !== 201) throw new Error(`createTenant failed: ${res.status} ${await res.text()}`);
    return (await res.json()) as { tenant: { id: string; name: string }; apiKey: { id: string; key: string } };
  }

  return { app, call, callOn, newInstance, createTenant };
}

export type Link = {
  code: string;
  shortUrl: string;
  url: string;
  status: string;
  redirectType: number;
  active: boolean;
  expiresAt: string | null;
  maxClicks: number | null;
};
