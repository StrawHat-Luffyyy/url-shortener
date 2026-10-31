import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { createClickEmitter, createKafkaSink, type ClickEvent } from "../src/analytics/emitter";
import { createKafka } from "../src/analytics/kafka";
import { setupApp, type Link } from "./helpers";

// Nothing listens on port 1, so every send fails.
const deadKafka = () => createKafka(["127.0.0.1:1"]);

const event = (): ClickEvent => ({
  event_id: randomUUID(),
  link_id: "1",
  tenant_id: randomUUID(),
  ts: new Date().toISOString(),
  ip_hash: "abc",
  asn: null,
  country: null,
  user_agent: "test",
  referrer: null,
  accept_language: null,
  is_datacenter_asn: null,
  response_code: 302,
});

describe("click emitter with Kafka unreachable", () => {
  it("emit returns immediately, even for thousands of events", async () => {
    const emitter = createClickEmitter({ sink: createKafkaSink(deadKafka(), "clicks.raw"), maxBuffered: 100 });

    const started = performance.now();
    for (let i = 0; i < 5000; i++) emitter.emit(event());
    const elapsed = performance.now() - started;

    expect(elapsed).toBeLessThan(200);
    await emitter.close({ timeoutMs: 100 });
  });

  it("drops events past the buffer bound and counts them, instead of growing", async () => {
    const emitter = createClickEmitter({ sink: createKafkaSink(deadKafka(), "clicks.raw"), maxBuffered: 100 });

    for (let i = 0; i < 1000; i++) emitter.emit(event());

    const stats = emitter.stats();
    expect(stats.buffered + stats.inFlight).toBeLessThanOrEqual(100 + 500);
    expect(stats.dropped).toBeGreaterThanOrEqual(400);
    expect(stats.buffered + stats.inFlight + stats.dropped).toBe(1000);
    await emitter.close({ timeoutMs: 100 });
  });

  it("redirects stay fast and correct when Kafka is down", async () => {
    const clicks = createClickEmitter({ sink: createKafkaSink(deadKafka(), "clicks.raw"), maxBuffered: 100 });
    const { call, callOn, newInstance, createTenant } = setupApp();
    const app = newInstance({ clickTracking: { emitter: clicks, ipHashSalt: "salt" } });
    const { apiKey } = await createTenant();
    const res = await call("POST", "/v1/links", { token: apiKey.key, body: { url: "https://example.com/" } });
    const { link } = (await res.json()) as { link: Link };

    const timings: number[] = [];
    for (let i = 0; i < 200; i++) {
      const started = performance.now();
      const redirect = await callOn(app, "GET", `/${link.code}`);
      timings.push(performance.now() - started);
      expect(redirect.status).toBe(302);
    }

    timings.sort((a, b) => a - b);
    expect(timings[Math.floor(timings.length * 0.99)]).toBeLessThan(50);
    expect(clicks.stats().dropped).toBeGreaterThan(0);
    await clicks.close({ timeoutMs: 100 });
  });
});
