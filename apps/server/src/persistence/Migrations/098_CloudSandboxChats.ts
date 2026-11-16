import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // One row per chat thread and the follower it runs on; `settle` makes the keep-or-delete
  // question one answer per pairing.
  yield* sql`
    CREATE TABLE IF NOT EXISTS cloud_sandbox_chats (
      thread_id TEXT PRIMARY KEY,
      resource_id TEXT NOT NULL,
      target TEXT NOT NULL,
      sandbox_project_id TEXT,
      settle TEXT NOT NULL DEFAULT 'none'
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS cloud_sandbox_chats_resource_idx
    ON cloud_sandbox_chats(resource_id)
  `;
  // A project's kept follower per profile, and its remembered settle answer.
  yield* sql`
    CREATE TABLE IF NOT EXISTS cloud_sandbox_projects (
      project_id TEXT PRIMARY KEY,
      on_settle TEXT
    )
  `;
});
