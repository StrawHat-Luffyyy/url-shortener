import { createHash } from "node:crypto";
import { Hono } from "hono";
import type { Redis } from "ioredis";
import postgres, { type Sql, type TransactionSql } from "postgres";
import { z } from "zod";
import { ApiError, parseBody } from "../errors";
import { codeForId } from "./codes";
import { parseDestination } from "./destination";
import { clickEvent, type ClickTracking } from "../analytics/click-tracking";
import type { IdAllocator } from "./id-allocator";
import type { CachedLink, LinkResolver } from "./resolver";

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

type Deps = { sql: Sql; ids: IdAllocator; links: LinkResolver; baseUrl: string };
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
export function linkRoutes({ sql, ids, links, baseUrl }: Deps) {
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
    // The code may be cached as a miss (someone looked up the alias earlier).
    await links.invalidate(row.code);
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
    await links.invalidate(row.code);
    return c.json({ link: toLinkJson(row, baseUrl) });
  });

  routes.delete("/links/:code", async (c) => {
    const deleted = await sql`
      UPDATE links SET status = 'deleted', updated_at = now()
      WHERE code = ${c.req.param("code")} AND tenant_id = ${c.get("tenantId")} AND status <> 'deleted'
      RETURNING id`;
    if (deleted.length === 0) throw notFound();
    await links.invalidate(c.req.param("code"));
    return c.body(null, 204);
  });

  return routes;
}

// The redirect handler's view of a link, read from Postgres on a cache miss.
export function loadLinkForRedirect(sql: Sql) {
  return async (code: string): Promise<CachedLink | null> => {
    const [row] = await sql<
      (Pick<LinkRow, "id" | "long_url" | "status" | "redirect_type" | "active" | "expires_at" | "max_clicks"> & {
        tenant_id: string;
      })[]
    >`
      SELECT id, tenant_id, long_url, status, redirect_type, active, expires_at, max_clicks
      FROM links WHERE code = ${code}`;
    if (!row) return null;
    return {
      id: String(row.id),
      tenantId: row.tenant_id,
      url: row.long_url,
      status: row.status,
      redirectType: row.redirect_type === 301 ? 301 : 302,
      active: row.active,
      expiresAt: row.expires_at?.getTime() ?? null,
      maxClicks: row.max_clicks,
    };
  };
}

// Anything else can't be a code (generated or alias), so it is rejected
// without touching the cache or the database.
const CODE_SHAPE = /^[A-Za-z0-9_-]{3,32}$/;

// Counts a click against a link's max_clicks (FR-4) and returns the new count,
// or null if it couldn't be counted. Redis INCR is atomic, so concurrent
// clicks can't both take the last slot. If Redis is unreachable the limit
// isn't enforced (fails open) rather than failing the redirect.
export function redisClickCounter(redis: Redis) {
  return (linkId: string): Promise<number | null> =>
    redis.incr(`link:clicks:${linkId}`).catch((err: unknown) => {
      console.error(`max_clicks not enforced for link ${linkId}: Redis unavailable`, err);
      return null;
    });
}

type RedirectDeps = {
  links: LinkResolver;
  countClick: (linkId: string) => Promise<number | null>;
  clicks?: ClickTracking;
};

// Public redirect (FR-8, FR-9). Mounted last so it never shadows /v1 routes.
// Every response for an existing link emits a click event (FR-11); emitting
// never waits, so Kafka can't slow the redirect down.
export function redirectRoute({ links, countClick, clicks }: RedirectDeps) {
  const routes = new Hono();

  routes.get("/:code", async (c) => {
    const code = c.req.param("code");
    const link = CODE_SHAPE.test(code) ? await links.resolve(code) : null;
    if (!link) throw new ApiError(404, "NOT_FOUND", "Short link not found");

    const expired = link.expiresAt !== null && link.expiresAt <= Date.now();
    // Only active links redirect. Phase 4 adds the interstitial for quarantined links.
    let gone = link.status !== "active" || !link.active || expired;
    // Only links with a limit pay for the extra Redis call.
    if (!gone && link.maxClicks !== null) {
      const count = await countClick(link.id);
      gone = count !== null && count > link.maxClicks;
    }
    if (gone) {
      clicks?.emitter.emit(clickEvent(c, link, 410, clicks));
      throw new ApiError(410, "LINK_GONE", "This short link is no longer available");
    }

    clicks?.emitter.emit(clickEvent(c, link, link.redirectType, clicks));
    if (link.redirectType === 302) {
      // Keep every click observable: browsers must not cache a 302 (FR-8).
      c.header("cache-control", "private, no-store");
      return c.redirect(link.url, 302);
    }
    return c.redirect(link.url, 301);
  });

  return routes;
}
