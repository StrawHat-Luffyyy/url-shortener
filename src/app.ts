import { createHash, timingSafeEqual } from "node:crypto";
import { Hono } from "hono";
import type { Sql } from "postgres";
import { z } from "zod";
import { authenticateApiKey, issueApiKey } from "./auth/api-keys";
import { ApiError, errorResponse } from "./errors";

export type AppDeps = { sql: Sql; operatorToken: string };
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

async function parseBody<T extends z.ZodType>(req: Request, schema: T): Promise<z.infer<T>> {
  const parsed = schema.safeParse(await req.json().catch(() => undefined));
  if (!parsed.success) throw new ApiError(400, "VALIDATION_FAILED", z.prettifyError(parsed.error));
  return parsed.data;
}

export function createApp({ sql, operatorToken }: AppDeps) {
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

  app.route("/v1/operator", operator);
  app.route("/v1", tenantApi);
  return app;
}
