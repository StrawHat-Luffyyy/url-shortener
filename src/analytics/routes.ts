import { Hono } from "hono";
import type { Sql } from "postgres";
import { z } from "zod";
import { ApiError } from "../errors";
import { RAW_RETENTION_DAYS } from "./partitions";

type Env = { Variables: { tenantId: string } };

const HOUR = 3_600_000;
const DAY = 24 * HOUR;

const querySchema = z
  .object({
    view: z.enum(["all", "human", "bot"]).default("all"),
    granularity: z.enum(["hour", "day"]).optional(),
    from: z.iso.datetime({ offset: true }).transform((s) => new Date(s)).optional(),
    to: z.iso.datetime({ offset: true }).transform((s) => new Date(s)).optional(),
  })
  .transform((q) => {
    const to = q.to ?? new Date();
    const from = q.from ?? new Date(to.getTime() - 7 * DAY);
    const granularity = q.granularity ?? (to.getTime() - from.getTime() <= 2 * DAY ? "hour" : "day");
    return { ...q, from, to, granularity };
  })
  .refine((q) => q.from < q.to, "from must be before to")
  .refine((q) => q.granularity === "day" || q.to.getTime() - q.from.getTime() <= 31 * DAY, "hourly series cover at most 31 days")
  .refine((q) => q.to.getTime() - q.from.getTime() <= 3660 * DAY, "range covers at most 10 years");

// Which rollup column and raw-event label a view reads. "bot" covers both
// suspected bots and known crawlers.
const VIEWS = {
  all: { column: "total", label: null },
  human: { column: "human", label: "human" },
  bot: { column: "suspected_bot", label: "bot" },
} as const;

const DIMENSIONS = { countries: "country", referrers: "referrer", browsers: "ua_family", devices: "device" } as const;

// GET /v1/links/:code/analytics (FR-12, FR-13).
// - Clicks and the time series come from the hourly rollups, which are kept
//   forever.
// - Unique visitors and the top-10 breakdowns come from raw events, which are
//   kept 90 days (FR-14). They only cover the part of the range inside that
//   window, given as `rawFrom`.
// - Buckets are UTC hours or days.
export function analyticsRoutes(sql: Sql) {
  const routes = new Hono<Env>();

  routes.get("/links/:code/analytics", async (c) => {
    const parsed = querySchema.safeParse(c.req.query());
    if (!parsed.success) throw new ApiError(400, "VALIDATION_FAILED", z.prettifyError(parsed.error));
    const { view, granularity, from, to } = parsed.data;

    const [link] = await sql<{ id: string }[]>`
      SELECT id FROM links
      WHERE code = ${c.req.param("code")} AND tenant_id = ${c.get("tenantId")} AND status <> 'deleted'`;
    if (!link) throw new ApiError(404, "NOT_FOUND", "Link not found");

    const { column, label } = VIEWS[view];
    const step = `1 ${granularity}`;
    const series = await sql<{ start: Date; clicks: number }[]>`
      SELECT b.start, COALESCE(sum(r.${sql(column)}), 0)::int AS clicks
      FROM generate_series(date_trunc(${granularity}, ${from}::timestamptz, 'UTC'), ${to}::timestamptz, ${step}::interval) AS b(start)
      LEFT JOIN click_rollups_hourly r
        ON r.link_id = ${link.id} AND r.hour >= b.start AND r.hour < b.start + ${step}::interval
      GROUP BY b.start ORDER BY b.start`;

    const rawFrom = new Date(Math.max(from.getTime(), Date.now() - RAW_RETENTION_DAYS * DAY));
    const labelFilter =
      label === null ? sql`` : label === "human" ? sql`AND label = 'human'` : sql`AND label <> 'human'`;
    const raw = sql`click_events WHERE link_id = ${link.id} AND ts >= ${rawFrom} AND ts <= ${to} ${labelFilter}`;

    const [uniques] = await sql<{ n: number }[]>`SELECT count(DISTINCT ip_hash)::int AS n FROM ${raw}`;
    const top = Object.fromEntries(
      await Promise.all(
        Object.entries(DIMENSIONS).map(async ([name, dim]) => {
          const rows = await sql<{ value: string; clicks: number }[]>`
            SELECT ${sql(dim)} AS value, count(*)::int AS clicks FROM ${raw} AND ${sql(dim)} IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 10`;
          return [name, rows] as const;
        }),
      ),
    );

    return c.json({
      code: c.req.param("code"),
      view,
      granularity,
      from,
      to,
      rawFrom,
      totals: { clicks: series.reduce((n, b) => n + b.clicks, 0), uniqueVisitors: uniques!.n },
      series,
      top,
    });
  });

  return routes;
}
