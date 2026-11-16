import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/**
 * Fork builds recorded the cloud sandbox migrations under ids upstream later took (81, then
 * 88/89). Dropping those rows lets upstream's migrations at those ids run, and 97/98 then
 * rerun the cloud sandbox schema, which only creates what is missing.
 */
export const reconcileForkCloudSandboxMigrations = Effect.fn("reconcileForkCloudSandboxMigrations")(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    const tables = yield* sql`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
  `;
    if (tables.length === 0) return;
    yield* sql`
    DELETE FROM effect_sql_migrations
    WHERE migration_id < 97 AND name IN ('CloudSandboxes', 'CloudSandboxChats')
  `;
  },
);
