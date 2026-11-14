import {
  EnvironmentId,
  ManagedCloudSandboxIssue,
  ManagedSandboxOwner,
  ManagedSandboxRetention,
  ProjectId,
  ManagedCloudSandboxResourceId,
  SandboxWorktreeHandoffResult,
  ThreadId,
  WorktreeMcpFailure,
  managedCloudSandboxProxyPath,
  type ManagedCloudSandboxConnection,
  type ManagedSandboxFollowerOwner,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../config.ts";
import { readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as Controller from "./CloudSandboxController.ts";
import * as Manager from "../cloudSandbox/CloudSandboxManager.ts";
export interface AcquiredWorkspace {
  readonly connection: ManagedCloudSandboxConnection;
  readonly pairingToken: string;
  readonly linkUrl: string;
  readonly prepareCheckout?: Effect.Effect<string, WorktreeMcpFailure>;
}

const BindingIntent = {
  owner: Schema.Union([ManagedSandboxOwner.members[1], ManagedSandboxOwner.members[2]]),
  retention: ManagedSandboxRetention,
};
const Binding = Schema.Union([
  Schema.TaggedStruct("pending", { ...BindingIntent, issue: ManagedCloudSandboxIssue }),
  Schema.TaggedStruct("clearing", BindingIntent),
  Schema.TaggedStruct("bound", BindingIntent),
]);

const Record = Schema.Struct({
  workspaceId: ManagedCloudSandboxResourceId,
  sourceThreadId: ThreadId,
  requestJson: Schema.String,
  phase: Schema.Literals(["creating", "running", "deleting", "resource-deleted", "deleted"]),
  childEnvironmentId: Schema.optionalKey(EnvironmentId),
  result: Schema.NullOr(SandboxWorktreeHandoffResult),
  binding: Schema.optionalKey(Binding),
  intent: Schema.optionalKey(
    Schema.Struct({
      retention: ManagedSandboxRetention,
      runSetupScript: Schema.Boolean,
    }),
  ),
  prepared: Schema.optionalKey(
    Schema.Struct({
      childProjectId: ProjectId,
      workspacePath: Schema.String,
    }),
  ),
});
const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(Record));
const encode = Schema.encodeEffect(Schema.fromJsonString(Record));
const decodeResourceId = Schema.decodeEffect(ManagedCloudSandboxResourceId);
const sameIssue = Schema.toEquivalence(ManagedCloudSandboxIssue);
const failure = (message: string) => new WorktreeMcpFailure({ code: "operation_failed", message });
const invalid = (message: string) => new WorktreeMcpFailure({ code: "invalid_request", message });

export const make = Effect.fn("CloudSandboxWorkspaceLifecycle.make")(function* ({
  tellTask,
}: {
  readonly tellTask: (
    sourceThreadId: ThreadId,
    taskId: string,
    issue: ManagedCloudSandboxIssue | undefined,
  ) => Effect.Effect<boolean>;
}) {
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const sql = yield* SqlClient.SqlClient;
  const threads = yield* Threads.ThreadManagementService;
  const links = yield* PeerLinks.PeerLinks;
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const controller = yield* Controller.CloudSandboxController;
  const manager = yield* Manager.CloudSandboxManager;
  const lock = yield* KeyedLock.make<string>();
  const localServerPort = readPersistedServerRuntimeState(config.serverRuntimeStatePath).pipe(
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.orElseSucceed(() => Option.none()),
    Effect.map((state) => (Option.isSome(state) ? state.value.port : config.port)),
  );
  const load = Effect.fn("CloudSandboxWorkspaces.load")(
    function* (workspaceId: ManagedCloudSandboxResourceId) {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM cloud_sandbox_workspaces WHERE workspace_id = ${workspaceId}`;
      return rows[0] ? yield* decode(rows[0].payload_json).pipe(Effect.asSome) : Option.none();
    },
    Effect.mapError(() => failure("Unable to read the cloud workspace journal.")),
  );
  const save = Effect.fn("CloudSandboxWorkspaces.save")(
    function* (record: typeof Record.Type) {
      const json = yield* encode(record);
      yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id, source_thread_id, payload_json)
      VALUES (${record.workspaceId}, ${record.sourceThreadId}, ${json})
      ON CONFLICT(workspace_id) DO UPDATE SET payload_json = excluded.payload_json`;
    },
    Effect.mapError(() => failure("Unable to persist the cloud workspace journal.")),
  );
  const releaseResource = Effect.fn("CloudSandboxWorkspaces.releaseResource")(function* (
    resourceId: ManagedCloudSandboxResourceId,
  ) {
    if (yield* controller.available) yield* controller.request({ _tag: "delete", resourceId });
    else
      yield* manager
        .release(resourceId)
        .pipe(Effect.mapError(() => failure("Unable to delete the sandbox workspace.")));
  });
  const finishRemoval = Effect.fn("CloudSandboxWorkspaces.finishRemoval")(function* (
    record: typeof Record.Type,
    beforeRelease: Effect.Effect<void> = Effect.void,
  ) {
    if (record.phase !== "resource-deleted") {
      yield* save({ ...record, phase: "deleting" });
      yield* beforeRelease;
      yield* releaseResource(record.workspaceId);
      record = { ...record, phase: "resource-deleted" };
      yield* save(record);
    }
    const childEnvironmentId = record.childEnvironmentId ?? record.result?.environmentId;
    if (childEnvironmentId !== undefined)
      yield* links
        .unlink(childEnvironmentId)
        .pipe(
          Effect.mapError(() =>
            failure(
              "The sandbox was deleted, but its peer link could not be removed. Retry cleanup.",
            ),
          ),
        );
    yield* save({ ...record, phase: "deleted" });
    return { workspaceId: record.workspaceId, deleted: true };
  });
  /** Records the owner of a delegated workspace's resource; the controller
   * derives the host id, so in-sandbox and root paths validate identically. */
  const bindResourceOwner = Effect.fn("CloudSandboxWorkspaces.bindResourceOwner")(function* (
    resourceId: ManagedCloudSandboxResourceId,
    owner: ManagedSandboxFollowerOwner,
    retention?: ManagedSandboxRetention | undefined,
  ) {
    if (yield* controller.available) {
      const result = yield* controller.request({
        _tag: "bindOwner",
        resourceId,
        owner,
        ...(retention === undefined ? {} : { retention }),
      });
      if (result._tag !== "resource" || result.resource.resourceId !== resourceId)
        return yield* failure("The controller did not confirm the sandbox owner.");
      return result.resource;
    }
    return yield* manager
      .bindOwner(resourceId, owner, retention)
      .pipe(Effect.mapError(() => failure("Unable to bind the sandbox owner.")));
  });
  const inspectResource = Effect.fn("CloudSandboxWorkspaces.inspectResource")(function* (
    resourceId: ManagedCloudSandboxResourceId,
  ) {
    if (!(yield* controller.available)) return yield* manager.get(resourceId);
    const result = yield* controller.request({ _tag: "get", resourceId });
    if (result._tag !== "resource" || result.resource.resourceId !== resourceId)
      return yield* failure("Unable to inspect the sandbox resource.");
    return result.resource;
  });
  const inspectPowerState = Effect.fn("CloudSandboxWorkspaces.inspectPowerState")(function* (
    resourceId: ManagedCloudSandboxResourceId,
  ) {
    if (!(yield* controller.available)) return yield* manager.execute(resourceId, { _tag: "get" });
    const result = yield* controller.request({ _tag: "status", resourceId });
    if (result._tag !== "result")
      return yield* failure("Unable to inspect the sandbox power state.");
    return result.result;
  });
  const publishBinding = Effect.fn("CloudSandboxWorkspaceLifecycle.publishBinding")(
    function* (record: typeof Record.Type, binding: typeof Binding.Type) {
      if (record.result === null) return false;
      const read = yield* threads.getThreadRecords(record.sourceThreadId, ["subagents"], {
        subagentIds: [record.result.taskId],
      });
      const task = read.subagents.find((task) => task.id === record.result?.taskId);
      if (task === undefined) return false;
      if (task.result !== null || doneStatuses.has(task.status)) return true;
      const current = task.remoteChild?.sandboxIssue;
      if (binding._tag !== "pending") {
        return (
          current?.cause !== "bind" || (yield* tellTask(record.sourceThreadId, task.id, undefined))
        );
      }
      if (current !== undefined && sameIssue(current, binding.issue)) return true;
      return yield* tellTask(record.sourceThreadId, task.id, binding.issue);
    },
    Effect.catchCause(() => Effect.succeed(false)),
  );

  const pendingIssue = Effect.fn("CloudSandboxWorkspaceLifecycle.pendingIssue")(function* (
    driverKind: string,
    profileId: string,
  ) {
    return {
      cause: "bind",
      driverKind,
      profileId,
      message: "Sandbox ownership is pending. The task keeps running while the server retries.",
      firstSeenAt: DateTime.formatIso(yield* DateTime.now),
    } satisfies ManagedCloudSandboxIssue;
  });

  const recoverBinding = Effect.fn("CloudSandboxWorkspaceLifecycle.recoverBinding")(function* (
    record: typeof Record.Type,
  ) {
    if (record.phase !== "running" || record.result === null) return false;
    if (record.binding?._tag === "bound") return true;
    const inspected = yield* inspectResource(record.workspaceId).pipe(Effect.option);
    if (
      Option.isSome(inspected) &&
      (inspected.value.phase === "deleted" || inspected.value.phase === "deleting")
    )
      return false;
    let binding = record.binding;
    if (binding === undefined) {
      if (Option.isNone(inspected)) return false;
      const resource = inspected.value;
      binding = {
        _tag: "pending",
        owner: {
          _tag: "task",
          hostEnvironmentId: resource.controllerEnvironmentId,
          sourceThreadId: record.sourceThreadId,
          taskId: record.result.taskId,
        },
        // Old journals never stored delete-on-completion intent; bound retention is authoritative.
        retention:
          resource.owner._tag === "user"
            ? (record.intent?.retention ?? "stop-and-retain")
            : resource.retention,
        issue: yield* pendingIssue(resource.driverKind, resource.profileId),
      };
      record = { ...record, binding };
      yield* save(record);
    }
    if (binding._tag === "pending") {
      const result = yield* bindResourceOwner(
        record.workspaceId,
        binding.owner,
        binding.retention,
      ).pipe(Effect.result);
      if (result._tag === "Failure") {
        yield* publishBinding(record, binding);
        return false;
      }
      binding = { _tag: "clearing", owner: binding.owner, retention: binding.retention };
      record = { ...record, binding };
      yield* save(record);
    }
    if (!(yield* publishBinding(record, binding))) return false;
    yield* save({ ...record, binding: { ...binding, _tag: "bound" } });
    return true;
  });

  const recoverOwnership = Effect.fn("CloudSandboxWorkspaceLifecycle.recoverOwnership")(
    function* () {
      const rows = yield* sql<{
        workspace_id: string;
      }>`SELECT workspace_id FROM cloud_sandbox_workspaces
      WHERE CASE WHEN json_valid(payload_json) THEN
        json_extract(payload_json, '$.phase') = 'running'
        AND COALESCE(json_extract(payload_json, '$.binding._tag'), '') <> 'bound'
        ELSE 0 END`.pipe(
        Effect.mapError(() => failure("Unable to read pending sandbox ownership.")),
      );
      let recovered = 0;
      for (const row of rows) {
        const complete = yield* lock
          .withLock(
            row.workspace_id,
            Effect.gen(function* () {
              const id = yield* decodeResourceId(row.workspace_id);
              const record = yield* load(id);
              return Option.isSome(record) ? yield* recoverBinding(record.value) : false;
            }),
          )
          .pipe(
            Effect.catchCause(() =>
              Effect.logWarning("Pending sandbox ownership will retry.").pipe(Effect.as(false)),
            ),
          );
        if (complete) recovered++;
      }
      return recovered;
    },
  );

  const pendingBindingIssues = Effect.fn("CloudSandboxWorkspaceLifecycle.pendingBindingIssues")(
    function* () {
      const rows = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM cloud_sandbox_workspaces
      WHERE CASE WHEN json_valid(payload_json) THEN
        json_extract(payload_json, '$.phase') = 'running'
        AND json_extract(payload_json, '$.binding._tag') = 'pending'
        ELSE 0 END`.pipe(
        Effect.mapError(() => failure("Unable to read pending sandbox ownership.")),
      );
      const issues: Array<{
        readonly sourceThreadId: ThreadId;
        readonly taskId: string;
        readonly issue: ManagedCloudSandboxIssue;
      }> = [];
      for (const row of rows) {
        const record = yield* decode(row.payload_json).pipe(Effect.option);
        if (
          Option.isSome(record) &&
          record.value.binding?._tag === "pending" &&
          record.value.result !== null
        )
          issues.push({
            sourceThreadId: record.value.sourceThreadId,
            taskId: record.value.result.taskId,
            issue: record.value.binding.issue,
          });
      }
      return issues;
    },
  );

  const reportControllerIssues = Effect.fn("CloudSandboxWorkspaceLifecycle.reportControllerIssues")(
    function* () {
      if (!(yield* controller.available)) return 0;
      const rows = yield* sql<{
        readonly workspace_id: string;
      }>`SELECT workspace_id FROM cloud_sandbox_workspaces
      WHERE CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.phase') = 'running' ELSE 0 END`.pipe(
        Effect.mapError(() => failure("Unable to read the cloud workspace journal.")),
      );
      let failing = 0;
      for (const row of rows) {
        failing += yield* lock
          .withLock(
            row.workspace_id,
            Effect.gen(function* () {
              const record = yield* load(yield* decodeResourceId(row.workspace_id));
              if (
                Option.isNone(record) ||
                record.value.phase !== "running" ||
                record.value.result === null ||
                record.value.binding?._tag === "pending" ||
                record.value.binding?._tag === "clearing"
              )
                return 0;
              const resource = yield* inspectResource(record.value.workspaceId);
              const owner = resource.owner;
              if (
                owner._tag !== "task" ||
                owner.hostEnvironmentId === resource.controllerEnvironmentId ||
                owner.sourceThreadId !== record.value.sourceThreadId ||
                owner.taskId !== record.value.result.taskId
              )
                return 0;
              const read = yield* threads.getThreadRecords(
                record.value.sourceThreadId,
                ["subagents"],
                { subagentIds: [record.value.result.taskId] },
              );
              const task = read.subagents.find((task) => task.id === owner.taskId);
              if (task === undefined || task.result !== null || doneStatuses.has(task.status))
                return 0;
              const issue = resource.phase === "deleted" ? undefined : resource.issue;
              const current = task.remoteChild?.sandboxIssue;
              // The orchestrator preserves the first issue within each failure class.
              const unchanged =
                current === undefined
                  ? issue === undefined
                  : issue !== undefined &&
                    current.cause === issue.cause &&
                    current.driverKind === issue.driverKind &&
                    current.profileId === issue.profileId;
              if (!unchanged) yield* tellTask(record.value.sourceThreadId, task.id, issue);
              return issue === undefined ? 0 : 1;
            }),
          )
          .pipe(Effect.catchCause(() => Effect.succeed(0)));
      }
      return failing;
    },
  );

  const doneStatuses = new Set(["completed", "failed", "cancelled", "interrupted"]);
  /**
   * Latest known delegated-task status from the parent thread's stored events.
   * Unreadable events mean unknown, and an unknown task is never "settled".
   */
  const delegatedSettled = Effect.fn("CloudSandboxWorkspaces.delegatedSettled")(function* (
    sourceThreadId: ThreadId,
    taskId: string,
  ) {
    const last = yield* threads
      .streamStoredEventsFrom({
        threadId: sourceThreadId,
        afterSequence: 0,
        eventType: "subagent.updated",
      })
      .pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "subagent.updated" && stored.event.payload.id === taskId,
        ),
        Stream.map((stored) =>
          stored.event.type === "subagent.updated" ? stored.event.payload.status : null,
        ),
        Stream.runLast,
        Effect.catch(() => Effect.succeedNone),
      );
    return Option.isSome(last) && last.value !== null && doneStatuses.has(last.value);
  });
  /**
   * Deletes sandbox workspaces whose VM has already stopped and that are
   * orphaned: their source thread is gone, or they never finished creating.
   * A follower bound to delete-on-completion also goes once its task settled.
   * Active children and live VMs are never touched.
   */
  const sweepOnce = Effect.fn("CloudSandboxWorkspaces.sweep")(function* () {
    const rows = yield* sql<{
      readonly workspace_id: string;
      readonly payload_json: string;
    }>`SELECT workspace_id, payload_json FROM cloud_sandbox_workspaces`.pipe(
      Effect.mapError(() => failure("Unable to read the cloud workspace journal.")),
    );
    let reclaimed = 0;
    for (const row of rows) {
      const didReclaim = yield* lock
        .withLock(
          row.workspace_id,
          Effect.gen(function* () {
            const id = yield* decodeResourceId(row.workspace_id);
            const record = yield* load(id);
            if (Option.isNone(record) || record.value.phase === "deleted") return false;
            if (record.value.phase === "deleting" || record.value.phase === "resource-deleted") {
              yield* finishRemoval(record.value);
              return true;
            }
            const shell = yield* threads
              .getThreadShell(record.value.sourceThreadId)
              .pipe(Effect.mapError(() => failure("Unable to inspect the source thread.")));
            const orphaned = shell === null || shell.deletedAt !== null;
            const resourceId = record.value.workspaceId;
            const resource = yield* inspectResource(resourceId).pipe(Effect.option);
            if (Option.isNone(resource)) return false;
            if (resource.value.phase !== "deleted") {
              const settlesIt =
                resource.value.retention === "delete-on-completion" &&
                record.value.phase === "running" &&
                record.value.result !== null &&
                (yield* delegatedSettled(record.value.sourceThreadId, record.value.result.taskId));
              if (!settlesIt && !orphaned && record.value.phase !== "creating") return false;
              const status = yield* inspectPowerState(resourceId).pipe(Effect.option);
              if (
                Option.isNone(status) ||
                status.value._tag !== "status" ||
                status.value.sandbox.state !== "stopped"
              ) {
                return false;
              }
              const deleted = yield* releaseResource(resourceId).pipe(Effect.result);
              if (deleted._tag === "Failure") return false;
            }
            yield* finishRemoval({ ...record.value, phase: "resource-deleted" });
            return true;
          }),
        )
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to reclaim cloud workspace; continuing sweep.", {
              workspaceId: row.workspace_id,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );
      if (didReclaim) reclaimed++;
    }
    return reclaimed;
  });
  /**
   * A managed child reaches this server through the port its runtime state
   * file last recorded, which changes across restarts.
   */
  const repointLinks = Effect.fn("CloudSandboxWorkspaces.repointLinks")(function* () {
    if (yield* controller.available) return 0;
    const port = yield* localServerPort;
    if (port <= 0) return 0;
    const rows = yield* sql<{
      readonly workspace_id: string;
      readonly payload_json: string;
    }>`SELECT workspace_id, payload_json FROM cloud_sandbox_workspaces`.pipe(
      Effect.mapError(() => failure("Unable to read the cloud workspace journal.")),
    );
    let repointed = 0;
    for (const row of rows) {
      const didRepoint = yield* Effect.gen(function* () {
        const record = yield* decode(row.payload_json);
        if (
          record.phase === "deleted" ||
          record.phase === "resource-deleted" ||
          record.childEnvironmentId === undefined
        )
          return false;
        yield* links.repoint(
          record.childEnvironmentId,
          `http://127.0.0.1:${port}${managedCloudSandboxProxyPath(record.workspaceId)}`,
        );
        return true;
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to repoint cloud workspace link; continuing.", {
            workspaceId: row.workspace_id,
            cause,
          }).pipe(Effect.as(false)),
        ),
      );
      if (didRepoint) repointed++;
    }
    return repointed;
  });
  /**
   * The journal is the source of truth for one workspace id: a retry with the
   * same request returns the stored result, and any other request is refused.
   */
  const createWorkspace = Effect.fn("CloudSandboxWorkspaces.createWorkspace")(function* (
    scope: McpThreadInvocationScope,
    input: {
      readonly workspaceId: ManagedCloudSandboxResourceId;
      readonly requestJson: string;
      readonly branch: string;
      readonly note: string;
      readonly retention: ManagedSandboxRetention;
      readonly runSetupScript: boolean;
      readonly acquire: Effect.Effect<AcquiredWorkspace, WorktreeMcpFailure>;
      readonly prepare: (
        acquired: AcquiredWorkspace,
      ) => Effect.Effect<
        { readonly childProjectId: ProjectId; readonly workspacePath: string },
        WorktreeMcpFailure
      >;
      readonly delegate: (acquired: {
        readonly connection: ManagedCloudSandboxConnection;
        readonly childProjectId: ProjectId;
      }) => Effect.Effect<
        Pick<SandboxWorktreeHandoffResult, "taskId" | "threadId" | "projectId">,
        WorktreeMcpFailure
      >;
    },
  ) {
    const { workspaceId, requestJson } = input;
    const intent = { retention: input.retention, runSetupScript: input.runSetupScript };
    return yield* lock.withLock(
      workspaceId,
      Effect.gen(function* () {
        if (!(yield* links.enabled))
          return yield* invalid(
            "Enable Linked environments in Settings → Labs before sandbox isolation.",
          );
        const previous = yield* load(workspaceId);
        if (Option.isSome(previous)) {
          if (
            previous.value.sourceThreadId !== scope.thread.threadId ||
            previous.value.requestJson !== requestJson ||
            (previous.value.intent !== undefined &&
              (previous.value.intent.retention !== intent.retention ||
                previous.value.intent.runSetupScript !== intent.runSetupScript))
          )
            return yield* invalid("This workspace id already belongs to another request.");
          if (
            previous.value.phase === "deleted" ||
            previous.value.phase === "deleting" ||
            previous.value.phase === "resource-deleted"
          )
            return yield* invalid("This cloud workspace is being deleted or has been deleted.");
          if (previous.value.result) {
            yield* recoverBinding(previous.value).pipe(
              Effect.catchCause(() => Effect.logWarning("Pending sandbox ownership will retry.")),
            );
            return previous.value.result;
          }
        }
        let record: typeof Record.Type = {
          ...(Option.isSome(previous) ? previous.value : {}),
          workspaceId,
          sourceThreadId: scope.thread.threadId,
          requestJson,
          phase: "creating",
          result: null,
          intent,
        };
        yield* save(record);
        const acquired = yield* input.acquire;
        if (
          record.prepared !== undefined &&
          record.childEnvironmentId !== acquired.connection.environmentId
        )
          return yield* failure("The prepared workspace belongs to another environment.");
        record = { ...record, childEnvironmentId: acquired.connection.environmentId };
        yield* save(record);
        const attached = record.prepared ?? (yield* input.prepare(acquired));
        record = { ...record, prepared: attached };
        yield* save(record);
        const delegated = yield* input.delegate({
          connection: acquired.connection,
          childProjectId: attached.childProjectId,
        });
        const result: SandboxWorktreeHandoffResult = {
          kind: "sandbox",
          workspaceId,
          resourceId: acquired.connection.resourceId,
          environmentId: acquired.connection.environmentId,
          projectId: delegated.projectId,
          threadId: delegated.threadId,
          taskId: delegated.taskId,
          workspacePath: attached.workspacePath,
          branch: input.branch,
          note: input.note,
        };
        const resource = yield* inspectResource(record.workspaceId).pipe(Effect.option);
        record = {
          ...record,
          phase: "running",
          result,
          binding: {
            _tag: "pending",
            owner: {
              _tag: "task",
              hostEnvironmentId: scope.environmentId,
              sourceThreadId: record.sourceThreadId,
              taskId: delegated.taskId,
            },
            retention: intent.retention,
            issue: yield* pendingIssue(
              acquired.connection.driverKind,
              Option.isSome(resource) ? resource.value.profileId : "",
            ),
          },
        };
        yield* save(record);
        yield* recoverBinding(record).pipe(
          Effect.catchCause(() => Effect.logWarning("Pending sandbox ownership will retry.")),
        );
        return result;
      }),
    );
  });
  const remove = Effect.fn("CloudSandboxWorkspaces.remove")(function* (
    scope: McpThreadInvocationScope,
    workspaceId: string,
  ) {
    const resourceId = yield* decodeResourceId(workspaceId).pipe(
      Effect.mapError(() => invalid("Invalid owned sandbox resource ID.")),
    );
    return yield* lock.withLock(
      resourceId,
      Effect.gen(function* () {
        const saved = yield* load(resourceId);
        if (Option.isNone(saved) || saved.value.sourceThreadId !== scope.thread.threadId)
          return yield* invalid("The calling thread does not own this cloud workspace.");
        const record = saved.value;
        if (record.phase === "deleted") return { workspaceId, deleted: true };
        return yield* finishRemoval(
          record,
          record.result === null
            ? Effect.void
            : forwarding
                .interruptThread(scope, record.result.environmentId, {
                  threadId: record.result.threadId,
                })
                .pipe(
                  Effect.timeout("10 seconds"),
                  Effect.catch((cause) =>
                    Effect.logWarning("Unable to stop active work in the child workspace.", {
                      workspaceId,
                      cause,
                    }),
                  ),
                ),
        );
      }),
    );
  });
  return {
    createWorkspace,
    remove,
    recoverOwnership: recoverOwnership(),
    pendingBindingIssues: pendingBindingIssues(),
    reportControllerIssues: reportControllerIssues(),
    sweep: sweepOnce(),
    repointLinks: repointLinks(),
    localServerPort,
  };
});
