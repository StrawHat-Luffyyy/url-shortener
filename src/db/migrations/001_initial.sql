-- Core schema (PRD §6.4).

CREATE TABLE tenants (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name        text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
  plan        text NOT NULL DEFAULT 'free',
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Only the SHA-256 of a key is stored. `prefix` is the public, non-secret part
-- used to find the row and to show the key in listings.
CREATE TABLE api_keys (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  key_hash    bytea NOT NULL,
  prefix      text NOT NULL UNIQUE,
  revoked_at  timestamptz,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);

CREATE TYPE link_status AS ENUM
  ('pending_scan', 'quarantined', 'active', 'blocked', 'expired', 'deleted');

CREATE TABLE links (
  id              bigint PRIMARY KEY,
  code            text NOT NULL UNIQUE,
  tenant_id       uuid NOT NULL REFERENCES tenants(id),
  long_url        text NOT NULL CHECK (length(long_url) <= 2048),
  normalized_url  text NOT NULL,
  status          link_status NOT NULL DEFAULT 'pending_scan',
  redirect_type   smallint NOT NULL DEFAULT 302 CHECK (redirect_type IN (301, 302)),
  expires_at      timestamptz,
  max_clicks      integer CHECK (max_clicks > 0),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX links_tenant_idx ON links (tenant_id, created_at DESC);

-- Fallback ID allocator for when Redis is down (PRD §7).
CREATE SEQUENCE link_id_fallback_seq;

CREATE TABLE link_scans (
  id             bigserial PRIMARY KEY,
  link_id        bigint NOT NULL REFERENCES links(id),
  stage          text NOT NULL,
  score          real,
  label          text,
  model_version  text,
  reasons        jsonb NOT NULL DEFAULT '{}',
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX link_scans_link_idx ON link_scans (link_id, created_at DESC);

-- Raw clicks, partitioned by day and kept 90 days. Daily partitions get created
-- by the analytics pipeline; the default partition catches anything else.
CREATE TABLE click_events (
  event_id   uuid NOT NULL,
  link_id    bigint NOT NULL,
  ts         timestamptz NOT NULL,
  country    text,
  asn        integer,
  referrer   text,
  ua_family  text,
  bot_score  real,
  label      text,
  PRIMARY KEY (event_id, ts)
) PARTITION BY RANGE (ts);
CREATE TABLE click_events_default PARTITION OF click_events DEFAULT;

CREATE TABLE click_rollups_hourly (
  link_id        bigint NOT NULL,
  hour           timestamptz NOT NULL,
  total          bigint NOT NULL DEFAULT 0,
  human          bigint NOT NULL DEFAULT 0,
  suspected_bot  bigint NOT NULL DEFAULT 0,
  unique_approx  bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (link_id, hour)
);

CREATE TABLE model_registry (
  id          bigserial PRIMARY KEY,
  kind        text NOT NULL,
  version     text NOT NULL,
  metrics     jsonb NOT NULL DEFAULT '{}',
  status      text NOT NULL DEFAULT 'candidate',
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (kind, version)
);
