import { migrate } from "../src/db/migrate";

export default async function setup() {
  const url =
    process.env.TEST_DATABASE_URL ?? "postgres://shortener:shortener@localhost:5432/shortener_test";
  await migrate(url);
}
