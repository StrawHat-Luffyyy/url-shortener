import { serve } from "@hono/node-server";
import { Redis } from "ioredis";
import postgres from "postgres";
import { createApp } from "./app";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

const sql = postgres(requireEnv("DATABASE_URL"), { onnotice: () => {} });
const redis = new Redis(requireEnv("REDIS_URL"));
const app = createApp({
  sql,
  redis,
  operatorToken: requireEnv("OPERATOR_TOKEN"),
  baseUrl: requireEnv("BASE_URL").replace(/\/$/, ""),
});
const port = Number(process.env.PORT ?? 3000);

serve({ fetch: app.fetch, port }, () => console.log(`listening on :${port}`));
