import { Redis } from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import { setupApp, type Link } from "./helpers";

const { callOn, newInstance, createTenant } = setupApp({ poolSize: 20 });

async function createMany(instances: ReturnType<typeof newInstance>[], token: string, count: number) {
  return Promise.all(
    Array.from({ length: count }, async (_, i) => {
      const res = await callOn(instances[i % instances.length]!, "POST", "/v1/links", {
        token,
        body: { url: `https://example.com/${i}` },
      });
      return { status: res.status, body: (await res.json()) as { link: Link } };
    }),
  );
}

describe("code generation under concurrency", () => {
  // Phase 1 exit criterion.
  it("10k parallel creates across 4 API instances produce zero duplicate codes", { timeout: 120_000 }, async () => {
    const { apiKey } = await createTenant();
    const instances = Array.from({ length: 4 }, () => newInstance());

    const results = await createMany(instances, apiKey.key, 10_000);

    expect(results.filter((r) => r.status !== 201)).toEqual([]);
    expect(new Set(results.map((r) => r.body.link.code)).size).toBe(10_000);
  });

  // Ids stay unique regardless: the primary key rejects a reused id and the
  // create retries with the next one. What this guards is availability. Once the
  // counter moves back over densely used ids, a request retrying alone walks a
  // run of used ids and gives up. (Under heavy concurrency the clashes are spread
  // across many requests and mostly absorbed, which is why creates here run one
  // at a time, with small blocks so the instances lease again quickly.)
  it("running instances keep creating links after Redis restores a stale ID counter", async () => {
    const { apiKey } = await createTenant();
    const redis = new Redis(process.env.TEST_REDIS_URL!);
    afterAll(() => redis.disconnect());
    const running = Array.from({ length: 2 }, () => newInstance({ idBlockSize: 10 }));
    await createMany(running, apiKey.key, 2); // first leases, which seed the counter
    const snapshot = (await redis.get("links:id_counter"))!;
    const before = await createMany(running, apiKey.key, 200);

    // Only the running instances create afterwards: a new instance would reseed
    // the counter at startup and hide the problem.
    await redis.set("links:id_counter", snapshot);
    const after = [];
    for (let i = 0; i < 40; i++) after.push(...(await createMany([running[i % 2]!], apiKey.key, 1)));

    const all = [...before, ...after];
    expect(all.filter((r) => r.status !== 201)).toEqual([]);
    expect(new Set(all.map((r) => r.body.link.code)).size).toBe(240);
  });
});
