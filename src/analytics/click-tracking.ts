import { createHash, randomUUID } from "node:crypto";
import { getConnInfo } from "@hono/node-server/conninfo";
import type { Context } from "hono";
import type { CachedLink } from "../links/resolver";
import type { ClickEmitter, ClickEvent } from "./emitter";

export type ClickTracking = {
  emitter: ClickEmitter;
  // IPs are only ever stored as salted hashes (PRD §5). The salt must be the
  // same on every instance or unique-visitor counts split.
  ipHashSalt: string;
  // Read the client IP from X-Forwarded-For and the country from CF-IPCountry
  // / X-Country-Code. Only set behind a proxy that overwrites those headers;
  // otherwise clients could spoof them.
  trustProxy?: boolean;
};

function clientIp(c: Context, trustProxy: boolean): string | null {
  if (trustProxy) {
    const forwarded = c.req.header("x-forwarded-for")?.split(",")[0]?.trim();
    if (forwarded) return forwarded;
  }
  try {
    return getConnInfo(c).remote.address ?? null;
  } catch {
    return null; // no socket, e.g. app.request() in tests
  }
}

function referrerHost(header: string | undefined): string | null {
  if (!header) return null;
  try {
    return new URL(header).host || null;
  } catch {
    return null;
  }
}

// Builds the click event for a redirect. Referrers are reduced to their host,
// so paths and query strings (which can carry personal data) aren't stored.
// ASN lookup isn't wired up yet, so asn and is_datacenter_asn are null.
export function clickEvent(c: Context, link: CachedLink, responseCode: number, tracking: ClickTracking): ClickEvent {
  const trustProxy = tracking.trustProxy ?? false;
  const ip = clientIp(c, trustProxy);
  return {
    event_id: randomUUID(),
    link_id: link.id,
    tenant_id: link.tenantId,
    ts: new Date().toISOString(),
    ip_hash: ip ? createHash("sha256").update(tracking.ipHashSalt).update(ip).digest("base64url").slice(0, 22) : null,
    asn: null,
    country: trustProxy ? (c.req.header("cf-ipcountry") ?? c.req.header("x-country-code") ?? null) : null,
    user_agent: c.req.header("user-agent")?.slice(0, 512) ?? null,
    referrer: referrerHost(c.req.header("referer")),
    accept_language: c.req.header("accept-language")?.slice(0, 128) ?? null,
    is_datacenter_asn: null,
    response_code: responseCode,
  };
}
