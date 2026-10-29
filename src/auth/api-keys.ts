import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import type { Sql } from "postgres";

// Key format: sk_<12 hex prefix>_<43 char base64url secret>.
// The prefix is public (shown in listings, used for lookup); only a SHA-256 of
// the whole key is stored. A fast hash is enough because the secret carries 256
// bits of entropy; slow hashes only matter for low-entropy human passwords.
const KEY_PATTERN = /^(sk_[0-9a-f]{12})_[A-Za-z0-9_-]{43}$/;

function hashKey(key: string): Buffer {
  return createHash("sha256").update(key).digest();
}

export async function issueApiKey(sql: Sql, tenantId: string) {
  const prefix = `sk_${randomBytes(6).toString("hex")}`;
  const key = `${prefix}_${randomBytes(32).toString("base64url")}`;
  const [row] = await sql<{ id: string; created_at: Date }[]>`
    INSERT INTO api_keys (tenant_id, key_hash, prefix)
    VALUES (${tenantId}, ${hashKey(key)}, ${prefix})
    RETURNING id, created_at`;
  return { id: row!.id, prefix, key, createdAt: row!.created_at };
}

// Returns the tenant id for a valid, unrevoked key, otherwise null.
export async function authenticateApiKey(sql: Sql, key: string): Promise<string | null> {
  const match = KEY_PATTERN.exec(key);
  if (!match) return null;
  const [row] = await sql<{ tenant_id: string; key_hash: Buffer }[]>`
    SELECT tenant_id, key_hash FROM api_keys
    WHERE prefix = ${match[1]!} AND revoked_at IS NULL`;
  if (!row || !timingSafeEqual(row.key_hash, hashKey(key))) return null;
  return row.tenant_id;
}
