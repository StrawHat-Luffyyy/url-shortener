-- Columns the analytics consumer writes (PRD §3.3, §4.2). ip_hash is the
-- salted hash from the redirect; raw IPs are never stored.
ALTER TABLE click_events
  ADD COLUMN ip_hash text,
  ADD COLUMN device text,
  ADD COLUMN response_code smallint;

-- Per-link queries over a time range (uniques and top-N in the analytics API).
CREATE INDEX click_events_link_ts_idx ON click_events (link_id, ts);
