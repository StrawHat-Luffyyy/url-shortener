# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

Phases 0 (Foundations) and 1 (Core shortener) are built. The spec is `PRD AI-Protected URL Shortening Service.docx` (Draft v1). Add commands below only once they actually work (k6/autocannon load tests and Kafka are still to come).

Deferred from Phase 1 on purpose. Each needs a piece from a later phase:
- `max_clicks` is stored but not enforced, because that needs click counting (Phase 3).
- Quarantined links don't get the interstitial page yet (Phase 4).
- Status transitions aren't audited yet.
- There's no DB-sequence fallback when Redis is down (`link_id_fallback_seq` exists but is unused; that's failure-mode work). Its range must not overlap the Redis counter's.

## Commands

```bash
docker compose up -d --wait          # Postgres 17 (5432; creates shortener + shortener_test) and Redis 7 (6379)
npm test                             # Vitest; global setup migrates shortener_test first. Needs Postgres + Redis (db 1)
npx vitest run test/keys.test.ts     # one file
npx vitest run -t "revoked key"      # tests whose name matches
npm run typecheck                    # tsc --noEmit (no lint configured yet)
npm run migrate                      # apply src/db/migrations/*.sql to $DATABASE_URL
npm run dev                          # tsx watch src/server.ts; needs DATABASE_URL, REDIS_URL, OPERATOR_TOKEN, BASE_URL (see .env.example)
```

There is no build step: tsx runs the TypeScript directly. Vitest is pinned to 4.x because vitest 5.0.5 pulls a `rolldown` tarball that 404s on npm.

## Code layout and conventions so far

- `src/app.ts`: `createApp({ sql, redis, operatorToken, baseUrl, idBlockSize? })` builds the Hono app with its dependencies passed in. Each call is a separate "instance" with its own ID allocator. Tests call `app.request()` against real Postgres and Redis (`test/helpers.ts`; `newInstance()` simulates extra API processes). The agreed test seams:
  - HTTP + real Postgres/Redis for behavior. Don't mock either, and don't assert by querying tables.
  - Pure table tests for `parseDestination` (`src/links/destination.ts`).

  Each test creates its own tenants and fresh aliases, so nothing is truncated between tests. Test files run in parallel against the same DB and Redis.
- Links live in `src/links/`:
  - `destination.ts`: validation + FR-7 normalization. Redirects go to `url` (the WHATWG serialization). `normalized` is for matching only.
  - `codes.ts`: id → 7-char code via multiply-mod-62⁷ + Base62.
  - `id-allocator.ts`: block leasing.
  - `routes.ts`: the CRUD endpoints plus the public `GET /:code`, mounted last.
- ID allocator invariants:
  - The Redis counter (`links:id_counter`) counts **ids, not blocks**, so instances with different block sizes can share it. Counting blocks once let the counter run 100× ahead.
  - It is reseeded from `max(links.id)` at startup and whenever a lease goes backwards.
  - **Never call `ids.next()` inside a transaction**: a lease may need a pooled connection, and 10k parallel creates deadlocked the pool that way. Take the id first, then open the transaction (`withNewId`), which retries the whole transaction on a code/pk clash.
- Aliases share the code namespace with generated codes and are global across tenants (409 `ALIAS_TAKEN`).
- Idempotency is a per-(tenant, key) advisory lock plus the `idempotency_keys` row. Reusing a key for a different body is a 422.
- Migrations: plain SQL files `src/db/migrations/NNN_name.sql`, applied in filename order and tracked in `schema_migrations`. Never edit an applied migration; add a new one.
- Auth: `/v1/operator/*` uses `Authorization: Bearer $OPERATOR_TOKEN` (the platform operator; creates tenants and their first key). Every other `/v1/*` route authenticates an API key and sets `c.get("tenantId")`. **Every tenant query must filter on that tenant id.** Return 404, not 403, for other tenants' resources.
- API keys look like `sk_<12 hex>_<43 base64url>`. The prefix is public and used for lookup; only the SHA-256 of the whole key is stored, compared with `timingSafeEqual`. The full key is returned once, at creation.
- Errors: throw `ApiError(status, code, message)` from `src/errors.ts`, and the app's `onError` renders the envelope. Add new codes to the `ErrorCode` union. Validate bodies with Zod (v4) via `parseBody`.

Work is planned in phases (PRD §9). Each phase has a hard exit criterion; check it before moving on:
0. Foundations: Docker Compose, CI, Postgres schema, tenant + API-key auth. Exit: keys hashed, tenant-isolation tests pass.
1. Core shortener. Exit: zero duplicate codes across 10k parallel creates.
2. Performance (cache, coalescing, rate limiting). Exit: redirect p99 targets met and documented.
3. Analytics pipeline (Kafka). Exit: a replay test shows no double counting.
4. Malicious-link detection. Exit: model metrics at target, SSRF tests pass, shadow-mode comparison works.
5. Bot detection. Exit: per-archetype precision/recall report published.
6. Hardening (OTel, chaos tests, admin endpoints, README). Exit: all failure-mode tests pass.

## Stack

TypeScript + Hono (API/redirect service), PostgreSQL (source of truth), Redis (cache, ID allocator, rate limits, blocklist), Kafka (click events), BullMQ (scan worker), ONNX Runtime in Node (URL classifier), Python (model training + bot-detection service), OpenTelemetry. Use Zod for input validation.

## Architecture invariants

Hold to these through every change. They are the core of the design:

- **The redirect path (`GET /:code`) is AI-free and never blocks on anything downstream.** It does a Redis lookup, falls back to Postgres on a miss, then fills the cache with a TTL. Negative results are cached briefly, and request coalescing means a missed hot key triggers only one DB read. Clicks go to Kafka fire-and-forget through a bounded in-memory buffer. On overflow the event is dropped and a counter goes up. Never add latency to the redirect.
- **Redirects must keep working when Kafka, the ML service or the LLM provider is down.** If Redis is down, read from Postgres, allocate IDs from a DB sequence, and rate-limit with a local in-memory limiter. If Postgres is down, serve from cache, and creation returns 503 with no partial writes. Every failure mode in PRD §7 needs an automated test or chaos script.
- **302 is the default** so every click is observable. 301 is opt-in per link, for creators who don't need analytics.
- **Code generation:** each instance leases ID blocks (about 1,000) from a Redis counter. A bijective scramble (multiply by a coprime modulo 62^7) plus Base62 turns each ID into a non-sequential 7-character code. ID gaps after a crash are acceptable.
- **Cache invalidation:** every update, block or delete deletes the cache key. Short TTLs are the safety net.
- **Link state machine:** `pending_scan → quarantined → active → blocked / expired / deleted`. Every transition is audited. Status codes: expired, deleted or blocked links return 410; unknown codes return 404; quarantined links get an interstitial warning page.
- **Write-path detection** has a 40 ms budget, excluding the deep scan. The order is: hard validation + SSRF protection (block private/loopback/link-local IPs *after* DNS resolution, and block shortener chains) → blocklist → lexical ONNX classifier (p99 < 15 ms). Score < 0.30 allows the link, 0.30–0.80 quarantines it and enqueues a deep scan, > 0.80 rejects it with a reason code. Every decision stores score, model version, top features and reason code in `link_scans`. The deep scan is an async BullMQ job: a sandboxed fetch, then an LLM classification with structured JSON output, under a daily cost cap and per-tenant limits. A periodic re-scan can flip active links to blocked.
- **Bot detection is fully async**, running off the `clicks.raw` topic (keyed by `link_id`, with a DLQ). A rules layer runs first, then an Isolation Forest per entity type with SHAP reason codes. Labels are `human`, `suspected_bot` and `known_crawler`. Every analytics metric must be available as `all`, `human` and `bot` views.
- **Idempotency:** click events are at-least-once and deduped by `event_id`, and rollup updates must be idempotent, because reprocessing a partition must not double-count. Link creation honors `Idempotency-Key`.
- **Multi-tenancy:** API keys are hashed at rest and shown once. Tenants can only see their own links and analytics. Per-tenant token-bucket rate limiting runs in Redis via Lua.
- **Privacy:** IPs are stored only as salted hashes. Raw click events are kept 90 days. Rollups are kept indefinitely.

## Conventions

- REST under `/v1` (see PRD §6.5). Errors use one consistent JSON envelope with machine-readable `code` values such as `URL_REJECTED_RISK`, `ALIAS_TAKEN` and `RATE_LIMITED`.
- Only http/https URLs, max length 2048. URLs are normalized before storage (PRD FR-7). Custom aliases are 3–32 characters of `[A-Za-z0-9_-]` and are checked against a reserved-word blocklist.
- Core tables: PRD §6.4. `click_events` is partitioned by day.
- ML: split train/test **by domain, not by random row**, to avoid leakage. Version models in `model_registry` and support shadow-mode scoring before promotion. Commit evaluation metrics to the repo.
- Non-goals: no full frontend (API first; a dashboard only after Phase 6), no billing, no custom-domain TLS at scale.

## Skills

**Rule:** before starting any task, check whether a skill below matches it. If one does, load it with the Skill tool and follow it instead of improvising. If a task spans several areas, combine the matching skills (e.g. `impeccable` + `tdd` for a tested UI change, `diagnosing-bugs` + `tdd` for a fix with a regression test). At the start of each task, tell the user which skill(s) you're using, or that none apply.

### Engineering workflow
- `tdd`: building a feature or fixing a bug test-first (red-green-refactor, integration tests).
- `diagnosing-bugs`: something is broken, throwing, failing or slow.
- `codebase-design`: designing a module's interface, choosing seams, making code more testable.
- `domain-modeling`: naming domain terms, editing GLOSSARY.md/CONTEXT.md, writing ADRs.
- `prototype`: building a throwaway to test a state model or UI idea.
- `research`: gathering docs/API facts from primary sources into a Markdown file.
- `grilling`: the user wants a plan or decision stress-tested.
- `wizard`: steps only a human can do (credentials, dashboards, cutovers) need a guided bash script.
- `writing-for-agents`: editing CLAUDE.md, AGENTS.md or a skill.
- `run`: launching the app to see a change work for real.
- `claude-api`: anything that calls Claude/Anthropic or another LLM (e.g. the deep-scan LLM classifier).

### Review, quality and security
- `code-review`: reviewing changes since a commit/branch against standards and spec.
- `ponytail`: the user wants the smallest change that solves it ("be lazy", "YAGNI").
- `ponytail-review`: reviewing one change for correctness, safety, load and bloat.
- `ponytail-audit`: auditing the whole repo; `ponytail-debt`: listing `shortcut:` comments; `ponytail-gain` / `ponytail-help`: ponytail stats and reference.
- `simplify`: cleaning up changed code (reuse, efficiency), not hunting bugs.
- `security-review`: security review of pending branch changes.
- `vibe-security`: auth, API keys, access control, user data or "is this safe?" (e.g. API-key hashing, SSRF, tenant isolation).
- `pr`: writing a PR body.

### Frontend and design (only after Phase 6; the API comes first)
- `impeccable`: designing, critiquing or polishing any UI.
- `ui-ux-pro-max`: UI/UX decisions such as styles, palettes, fonts and layout guidelines.
- `taste-skill`: landing pages or redesigns that shouldn't look templated.
- `ui-styling`: shadcn/ui + Tailwind components, theming, dark mode.
- `shadcn`: adding or fixing shadcn components, or a `components.json` project.
- `migrate-radix-to-base`: moving components from Radix UI to Base UI.
- `design`, `design-system`, `brand`, `banner-design`, `slides`: brand identity, design tokens, banners, HTML decks.
- `dataviz`: any chart, graph or dashboard (e.g. analytics views).

### Artifacts, documents and files
- `artifact-design`: before writing any Artifact page; `artifact-diagramming`: diagrams in one; `artifact-capabilities`: runtime features (shared state, data, files).
- `anthropic-skills:docs`: a shareable document (default for docs/reports); `docx` / `pdf` / `pptx` / `xlsx`: when that file format is involved.

### Tooling and environment
- `update-config`: settings.json, permissions, env vars, hooks ("whenever X, do Y").
- `fewer-permission-prompts`: cutting down repeated permission prompts.
- `keybindings-help`: keyboard shortcuts; `plugin-authoring`: building mods, panels or hooks.
- `loop`: running something on an interval; `schedule`: cron or one-off cloud agents.
- `anthropic-skills:built-in-browser` / `chrome-browser` / `computer-use`: before the first use of that tool.
- `reverse-engineer-anything`: analyzing shipped binaries or bundles (not source repos).
- `init`: creating CLAUDE.md; `anthropic-skills:consolidate-memory`: tidying memory; `anthropic-skills:explain-usage`: token usage breakdown; `anthropic-skills:setup-claude`: guided setup.

### User-invoked only (Claude can't load these; suggest the slash command when one fits)
- `/to-spec`, `/to-tickets`, `/implement-spec`, `/implement`: conversation → spec → tickets → code.
- `/wayfinder`: planning work too big for one session; `/triage`: moving issues through triage.
- `/grill-me`, `/grill-with-docs`: interviewing to sharpen a plan (the second also writes ADRs/glossary).
- `/improve-codebase-architecture`: an HTML report of refactoring opportunities.
- `/handoff`: compacting the session for another agent; `/retro`: session retrospective.
- `/teach`, `/to-questionnaire`, `/wait-what`, `/ask-matt`, `/setup-matt-pocock-skills` (run once before the engineering skills).
