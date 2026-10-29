import postgres from "postgres";
import { afterAll } from "vitest";
import { createApp } from "../src/app";

export const OPERATOR_TOKEN = "test-operator-token";

export function setupApp() {
  const sql = postgres(process.env.TEST_DATABASE_URL!, { max: 5, onnotice: () => {} });
  afterAll(() => sql.end());
  const app = createApp({ sql, operatorToken: OPERATOR_TOKEN });

  function call(method: string, path: string, opts: { token?: string; body?: unknown } = {}) {
    const headers: Record<string, string> = {};
    if (opts.token) headers.authorization = `Bearer ${opts.token}`;
    if (opts.body !== undefined) headers["content-type"] = "application/json";
    return app.request(path, {
      method,
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
    });
  }

  async function createTenant(name = "Acme") {
    const res = await call("POST", "/v1/operator/tenants", {
      token: OPERATOR_TOKEN,
      body: { name },
    });
    if (res.status !== 201) throw new Error(`createTenant failed: ${res.status} ${await res.text()}`);
    return (await res.json()) as { tenant: { id: string; name: string }; apiKey: { id: string; key: string } };
  }

  return { app, call, createTenant };
}
