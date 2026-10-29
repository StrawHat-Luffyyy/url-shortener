import { createHash, timingSafeEqual } from "node:crypto";
import { Hono } from "hono";
import type { Redis } from "ioredis";
import type { Sql } from "postgres";
import { z } from "zod";
import { authenticateApiKey, issueApiKey } from "./auth/api-keys";
import { ApiError, errorResponse, parseBody } from "./errors";
import { createIdAllocator } from "./links/id-allocator";
import { createLinkResolver } from "./links/resolver";
import { linkRoutes, loadLinkForRedirect, redirectRoute } from "./links/routes";
import { createRateLimiter, type Bucket } from "./rate-limit";

export type AppDeps = {
  sql: Sql;
  redis: Redis;
  operatorToken: string;
  baseUrl: string;
  // Link ids leased from Redis per round trip (default 1000).
  idBlockSize?: number;
  // Per-tenant token buckets: `create` for POST /v1/links, `api` for every
  // other tenant call. Redirects are not limited per tenant.
  rateLimits?: { create: Bucket; api: Bucket };
};

const DEFAULT_RATE_LIMITS = {
  create: { capacity: 100, refillPerSecond: 20 },
  api: { capacity: 600, refillPerSecond: 100 },
};
type Env = { Variables: { tenantId: string } };

function bearer(header: string | undefined): string | null {
  const m = header?.match(/^Bearer (.+)$/);
  return m ? m[1]! : null;
}

// Compares digests so the comparison is constant-time regardless of length.
function sameSecret(a: string, b: string): boolean {
  const h = (s: string) => createHash("sha256").update(s).digest();
  return timingSafeEqual(h(a), h(b));
}

export function createApp({
  sql,
  redis,
  operatorToken,
  baseUrl,
  idBlockSize,
  rateLimits = DEFAULT_RATE_LIMITS,
}: AppDeps) {
  const app = new Hono<Env>();

  app.onError((err, c) => {
    if (err instanceof ApiError) return errorResponse(c, err.status, err.code, err.message);
    console.error(err);
    return errorResponse(c, 500, "INTERNAL", "Internal server error");
  });
  app.notFound((c) => errorResponse(c, 404, "NOT_FOUND", "Not found"));

  // Platform-operator endpoints, guarded by a shared bearer token.
  const operator = new Hono<Env>();
  operator.use(async (c, next) => {
    const token = bearer(c.req.header("authorization"));
    if (!token || !sameSecret(token, operatorToken)) {
      throw new ApiError(401, "UNAUTHORIZED", "Invalid operator token");
    }
    await next();
  });

  operator.post("/tenants", async (c) => {
    const { name } = await parseBody(c.req.raw, z.object({ name: z.string().trim().min(1).max(200) }));
    const result = await sql.begin(async (tx) => {
      const [tenant] = await tx<{ id: string; name: string; plan: string }[]>`
        INSERT INTO tenants (name) VALUES (${name}) RETURNING id, name, plan`;
      const key = await issueApiKey(tx as unknown as Sql, tenant!.id);
      return { tenant: tenant!, key };
    });
    return c.json({ tenant: result.tenant, apiKey: result.key }, 201);
  });

  // Tenant endpoints, authenticated by API key. Every query below must be
  // scoped to c.get("tenantId").
  const tenantApi = new Hono<Env>();
  tenantApi.use(async (c, next) => {
    const token = bearer(c.req.header("authorization"));
    const tenantId = token ? await authenticateApiKey(sql, token) : null;
    if (!tenantId) throw new ApiError(401, "UNAUTHORIZED", "Missing or invalid API key");
    c.set("tenantId", tenantId);
    await next();
  });

  const limiter = createRateLimiter(redis);
  tenantApi.use(async (c, next) => {
    const isCreate = c.req.method === "POST" && c.req.path === "/v1/links";
    const name = isCreate ? "create" : "api";
    const result = await limiter.take(`${name}:${c.get("tenantId")}`, rateLimits[name]);
    if (!result.allowed) {
      c.header("retry-after", String(Math.ceil(result.retryAfterMs / 1000)));
      throw new ApiError(429, "RATE_LIMITED", "Rate limit exceeded");
    }
    await next();
  });

  tenantApi.get("/tenant", async (c) => {
    const [tenant] = await sql`SELECT id, name, plan, created_at FROM tenants WHERE id = ${c.get("tenantId")}`;
    return c.json({ tenant });
  });

  tenantApi.post("/keys", async (c) => {
    const { id, prefix, key, createdAt } = await issueApiKey(sql, c.get("tenantId"));
    return c.json({ apiKey: { id, prefix, key, createdAt } }, 201);
  });

  tenantApi.get("/keys", async (c) => {
    const keys = await sql`
      SELECT id, prefix, revoked_at AS "revokedAt", created_at AS "createdAt"
      FROM api_keys WHERE tenant_id = ${c.get("tenantId")}
      ORDER BY created_at`;
    return c.json({ keys });
  });

  tenantApi.delete("/keys/:id", async (c) => {
    const id = z.uuid().safeParse(c.req.param("id"));
    const revoked = id.success
      ? await sql`
          UPDATE api_keys SET revoked_at = now()
          WHERE id = ${id.data} AND tenant_id = ${c.get("tenantId")} AND revoked_at IS NULL
          RETURNING id`
      : [];
    if (revoked.length === 0) throw new ApiError(404, "NOT_FOUND", "API key not found");
    return c.body(null, 204);
  });

  const links = createLinkResolver({ redis, load: loadLinkForRedirect(sql) });
  tenantApi.route("/", linkRoutes({ sql, ids: createIdAllocator(redis, sql, idBlockSize), links, baseUrl }));

  app.route("/v1/operator", operator);
  app.route("/v1", tenantApi);
  app.route("/", redirectRoute(links));
  return app;
}
