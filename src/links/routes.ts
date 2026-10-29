import { createHash } from "node:crypto";
import { Hono } from "hono";
import postgres, { type Sql, type TransactionSql } from "postgres";
import { z } from "zod";
import { ApiError, parseBody } from "../errors";
import { codeForId } from "./codes";
import { parseDestination } from "./destination";
import type { IdAllocator } from "./id-allocator";

type LinkRow = {
  id: string;
  code: string;
  long_url: string;
  status: string;
  redirect_type: number;
  active: boolean;
  expires_at: Date | null;
  max_clicks: number | null;
  created_at: Date;
  updated_at: Date;
};

type Deps = { sql: Sql; ids: IdAllocator; baseUrl: string };
type Env = { Variables: { tenantId: string } };

const futureDate = z.iso
  .datetime({ offset: true })
  .transform((s) => new Date(s))
  .refine((d) => d.getTime() > Date.now(), "must be in the future");

// Words an alias can't take because they are, or may become, routes.
const RESERVED_ALIASES = new Set([
  "v1", "v2", "api", "admin", "operator", "health", "healthz", "ready", "metrics",
  "status", "static", "assets", "login", "logout", "signup", "dashboard", "docs", "www",
]);

const createSchema = z.object({
  url: z.string(),
  alias: z.string().regex(/^[A-Za-z0-9_-]{3,32}$/, "3-32 characters of A-Z, a-z, 0-9, _ or -").optional(),
  expiresAt: futureDate.optional(),
  maxClicks: z.number().int().positive().max(2 ** 31 - 1).optional(),
  redirectType: z.union([z.literal(301), z.literal(302)]).default(302),
});

type CreateInput = z.infer<typeof createSchema>;

const updateSchema = z
  .object({
    url: z.string().optional(),
    expiresAt: futureDate.nullable().optional(),
    active: z.boolean().optional(),
  })
  .refine((u) => Object.keys(u).length > 0, "nothing to update");

const IDEMPOTENCY_KEY = /^[!-~]{1,255}$/;

const isUniqueViolation = (err: unknown, constraint: string) =>
  err instanceof postgres.PostgresError && err.code === "23505" && err.constraint_name === constraint;

function destination(input: string) {
  const parsed = parseDestination(input);
  if (!parsed.ok) throw new ApiError(400, parsed.reason, `Destination URL rejected: ${parsed.reason}`);
  return parsed;
}

export function toLinkJson(row: LinkRow, baseUrl: string) {
  return {
    code: row.code,
    shortUrl: `${baseUrl}/${row.code}`,
    url: row.long_url,
    status: row.status,
    redirectType: row.redirect_type,
    active: row.active,
    expiresAt: row.expires_at,
    maxClicks: row.max_clicks,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

// Tenant-scoped link management, mounted under /v1 behind API-key auth.
export function linkRoutes({ sql, ids, baseUrl }: Deps) {
  const routes = new Hono<Env>();

  async function insertLink(tx: TransactionSql, id: number, tenantId: string, input: CreateInput) {
    const dest = destination(input.url);
    const [row] = await tx<LinkRow[]>`
      INSERT INTO links (id, code, tenant_id, long_url, normalized_url, status, redirect_type, expires_at, max_clicks)
      VALUES (${id}, ${input.alias ?? codeForId(id)}, ${tenantId}, ${dest.url}, ${dest.normalized}, 'active',
              ${input.redirectType}, ${input.expiresAt ?? null}, ${input.maxClicks ?? null})
      RETURNING *`;
    return row!;
  }

  // Runs `create` in a transaction with a freshly allocated id. The id is taken
  // before the transaction opens: the allocator may need a pooled connection
  // itself, and waiting for it while holding one can starve the pool.
  //
  // A generated code can clash with an alias someone already chose (aliases
  // share the code namespace), or an id can repeat right after Redis lost its
  // counter; either way the whole transaction is retried with a new id.
  async function withNewId(alias: string | undefined, create: (tx: TransactionSql, id: number) => Promise<LinkRow>) {
    for (let attempt = 1; ; attempt++) {
      const id = await ids.next();
      try {
        return await sql.begin((tx) => create(tx, id));
      } catch (err) {
        const codeClash = isUniqueViolation(err, "links_code_key");
        if (codeClash && alias) throw new ApiError(409, "ALIAS_TAKEN", `"${alias}" is already taken`);
        if (!(codeClash || isUniqueViolation(err, "links_pkey")) || attempt >= 5) throw err;
      }
    }
  }

  routes.post("/links", async (c) => {
    const input = await parseBody(c.req.raw, createSchema);
    if (input.alias && RESERVED_ALIASES.has(input.alias.toLowerCase())) {
      throw new ApiError(400, "ALIAS_RESERVED", `"${input.alias}" is reserved`);
    }
    const tenantId = c.get("tenantId");
    const idempotencyKey = c.req.header("idempotency-key");
    if (idempotencyKey !== undefined && !IDEMPOTENCY_KEY.test(idempotencyKey)) {
      throw new ApiError(400, "VALIDATION_FAILED", "Idempotency-Key must be 1-255 printable ASCII characters");
    }

    const row = await withNewId(input.alias, async (tx, id) => {
      if (idempotencyKey === undefined) return insertLink(tx, id, tenantId, input);

      // FR-5. The advisory lock serializes concurrent requests with the same key,
      // so exactly one creates the link and the rest replay it.
      const requestHash = createHash("sha256").update(JSON.stringify(input)).digest();
      await tx`SELECT pg_advisory_xact_lock(hashtextextended(${`${tenantId}:${idempotencyKey}`}, 0))`;
      const [previous] = await tx<(LinkRow & { request_hash: Buffer })[]>`
        SELECT l.*, i.request_hash FROM idempotency_keys i JOIN links l ON l.id = i.link_id
        WHERE i.tenant_id = ${tenantId} AND i.key = ${idempotencyKey}`;
      if (previous) {
        if (!previous.request_hash.equals(requestHash)) {
          throw new ApiError(422, "IDEMPOTENCY_KEY_REUSED", "Idempotency-Key was already used for a different request");
        }
        return previous;
      }
      const created = await insertLink(tx, id, tenantId, input);
      await tx`
        INSERT INTO idempotency_keys (tenant_id, key, request_hash, link_id)
        VALUES (${tenantId}, ${idempotencyKey}, ${requestHash}, ${created.id})`;
      return created;
    });
    return c.json({ link: toLinkJson(row, baseUrl) }, 201);
  });

  // Deleted links are invisible to their owner (FR-6); other tenants' links are
  // indistinguishable from missing ones.
  const notFound = () => new ApiError(404, "NOT_FOUND", "Link not found");

  routes.get("/links/:code", async (c) => {
    const [row] = await sql<LinkRow[]>`
      SELECT * FROM links
      WHERE code = ${c.req.param("code")} AND tenant_id = ${c.get("tenantId")} AND status <> 'deleted'`;
    if (!row) throw notFound();
    return c.json({ link: toLinkJson(row, baseUrl) });
  });

  routes.patch("/links/:code", async (c) => {
    const input = await parseBody(c.req.raw, updateSchema);
    const changes: Record<string, unknown> = {};
    if (input.url !== undefined) {
      const dest = destination(input.url);
      changes.long_url = dest.url;
      changes.normalized_url = dest.normalized;
    }
    if (input.expiresAt !== undefined) changes.expires_at = input.expiresAt;
    if (input.active !== undefined) changes.active = input.active;

    const [row] = await sql<LinkRow[]>`
      UPDATE links SET ${sql(changes)}, updated_at = now()
      WHERE code = ${c.req.param("code")} AND tenant_id = ${c.get("tenantId")} AND status <> 'deleted'
      RETURNING *`;
    if (!row) throw notFound();
    return c.json({ link: toLinkJson(row, baseUrl) });
  });

  routes.delete("/links/:code", async (c) => {
    const deleted = await sql`
      UPDATE links SET status = 'deleted', updated_at = now()
      WHERE code = ${c.req.param("code")} AND tenant_id = ${c.get("tenantId")} AND status <> 'deleted'
      RETURNING id`;
    if (deleted.length === 0) throw notFound();
    return c.body(null, 204);
  });

  return routes;
}

// Public redirect (FR-8, FR-9). Mounted last so it never shadows /v1 routes.
export function redirectRoute(sql: Sql) {
  const routes = new Hono();

  routes.get("/:code", async (c) => {
    const [link] = await sql<Pick<LinkRow, "long_url" | "status" | "redirect_type" | "active" | "expires_at">[]>`
      SELECT long_url, status, redirect_type, active, expires_at FROM links WHERE code = ${c.req.param("code")}`;
    if (!link) throw new ApiError(404, "NOT_FOUND", "Short link not found");

    const expired = link.expires_at !== null && link.expires_at.getTime() <= Date.now();
    // Only active links redirect. Phase 4 adds the interstitial for quarantined links.
    if (link.status !== "active" || !link.active || expired) {
      throw new ApiError(410, "LINK_GONE", "This short link is no longer available");
    }

    if (link.redirect_type === 302) {
      // Keep every click observable: browsers must not cache a 302 (FR-8).
      c.header("cache-control", "private, no-store");
      return c.redirect(link.long_url, 302);
    }
    return c.redirect(link.long_url, 301);
  });

  return routes;
}
