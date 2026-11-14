import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Earlier fork builds shipped this change under the 081 id, so only create
  // what a database that already ran it does not have yet.
  yield* sql`
    CREATE TABLE IF NOT EXISTS managed_cloud_sandboxes (
      resource_id TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS cloud_sandbox_workspaces (
      workspace_id TEXT PRIMARY KEY,
      source_thread_id TEXT NOT NULL,
      payload_json TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS cloud_sandbox_workspaces_source_idx
    ON cloud_sandbox_workspaces(source_thread_id)
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS sandbox_profiles (
      profile_id TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS cloud_sandbox_thread_setups (
      thread_id TEXT PRIMARY KEY,
      status TEXT NOT NULL
    )
  `;
});
