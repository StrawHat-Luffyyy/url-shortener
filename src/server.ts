import { serve } from "@hono/node-server";
import postgres from "postgres";
import { createApp } from "./app";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

const sql = postgres(requireEnv("DATABASE_URL"), { onnotice: () => {} });
const app = createApp({ sql, operatorToken: requireEnv("OPERATOR_TOKEN") });
const port = Number(process.env.PORT ?? 3000);

serve({ fetch: app.fetch, port }, () => console.log(`listening on :${port}`));
