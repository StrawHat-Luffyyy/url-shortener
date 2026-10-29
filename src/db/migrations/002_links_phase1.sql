-- PATCH /v1/links/:code can switch a link off without deleting it (FR-6).
ALTER TABLE links ADD COLUMN active boolean NOT NULL DEFAULT true;

-- FR-5: replaying a create with the same Idempotency-Key returns the same link.
-- request_hash detects the same key being reused for a different request body.
CREATE TABLE idempotency_keys (
  tenant_id     uuid NOT NULL REFERENCES tenants(id),
  key           text NOT NULL,
  request_hash  bytea NOT NULL,
  link_id       bigint NOT NULL REFERENCES links(id),
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, key)
);
