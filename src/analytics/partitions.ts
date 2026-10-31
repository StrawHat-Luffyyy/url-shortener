import postgres, { type Sql } from "postgres";

export const RAW_RETENTION_DAYS = 90;
const NAME = /^click_events_(\d{4})(\d{2})(\d{2})$/;

const dayName = (day: string) => `click_events_${day.replaceAll("-", "")}`;
const nextDay = (day: string) => new Date(Date.parse(`${day}T00:00:00Z`) + 86_400_000).toISOString().slice(0, 10);

// click_events is partitioned by UTC day (PRD §6.4). A day's partition must
// exist before its rows arrive: once rows for a day sit in the default
// partition, that day's partition can no longer be created. Those rows stay in
// the default partition, which is never dropped by retention.
export function createPartitionManager(sql: Sql) {
  const known = new Set<string>();

  async function ensure(days: Iterable<string>) {
    for (const day of days) {
      if (known.has(day)) continue;
      try {
        await sql.begin(async (tx) => {
          // Serializes creation across consumers; IF NOT EXISTS handles the rest.
          await tx`SELECT pg_advisory_xact_lock(727002)`;
          await tx.unsafe(
            `CREATE TABLE IF NOT EXISTS ${dayName(day)} PARTITION OF click_events
             FOR VALUES FROM ('${day} 00:00:00+00') TO ('${nextDay(day)} 00:00:00+00')`,
          );
        });
      } catch (err) {
        // 23514: the default partition already holds rows for this day.
        if (!(err instanceof postgres.PostgresError && err.code === "23514")) throw err;
      }
      known.add(day);
    }
  }

  return {
    ensure,

    // Creates the next few days' partitions and drops those past retention (FR-14).
    async maintain({ aheadDays = 2, retentionDays = RAW_RETENTION_DAYS } = {}) {
      const today = new Date().toISOString().slice(0, 10);
      const days = [today];
      for (let i = 0; i < aheadDays; i++) days.push(nextDay(days.at(-1)!));
      await ensure(days);

      const cutoff = Date.parse(`${today}T00:00:00Z`) - retentionDays * 86_400_000;
      const partitions = await sql<{ name: string }[]>`
        SELECT c.relname AS name FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'click_events'::regclass`;
      for (const { name } of partitions) {
        const m = NAME.exec(name);
        if (m && Date.parse(`${m[1]}-${m[2]}-${m[3]}T00:00:00Z`) < cutoff) {
          await sql.unsafe(`DROP TABLE IF EXISTS ${name}`);
          known.delete(`${m[1]}-${m[2]}-${m[3]}`);
        }
      }
    },
  };
}
