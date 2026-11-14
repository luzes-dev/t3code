import * as ServerConfig from "../config.ts";
import {
  CommandId,
  ManagedCloudSandboxIssue,
  ManagedCloudSandboxResourceId,
  MANAGED_CLOUD_SOURCE_PORT,
  NodeId,
  SandboxWorktreeHandoffResult,
  ThreadId,
  ProjectId,
  WorktreeMcpFailure,
  WorktreeMcpHandoffInput,
  type ModelSelection,
  type RuntimeMode,
  type ProviderInteractionMode,
  type ManagedSandboxRetention,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as SqlClient from "effect/sql/SqlClient";
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";
import { ProjectToolkit } from "../mcp/toolkits/project/tools.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Projects from "../project/ProjectService.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as Git from "../git/GitWorkflowService.ts";
import * as GitDriver from "../vcs/GitVcsDriver.ts";
import { PrepareSandboxBranchTool } from "./CloudSandboxWorkspaceTools.ts";
import * as Setup from "../project/ProjectSetupScriptRunner.ts";
import * as Controller from "./CloudSandboxController.ts";
import * as Manager from "../cloudSandbox/CloudSandboxManager.ts";
import type { ManagedCloudSandboxConnection, SandboxProfileId } from "@t3tools/contracts";
import * as Profiles from "../cloudSandbox/CloudSandboxProfiles.ts";
import * as ProjectTransfer from "./CloudSandboxProjectTransfer.ts";
import * as WorkspaceLifecycle from "./CloudSandboxWorkspaceLifecycle.ts";
import type { AcquiredWorkspace } from "./CloudSandboxWorkspaceLifecycle.ts";

const SetupStatus = Schema.Literals(["started", "completed"]);
const decodeSetupStatus = Schema.decodeUnknownEffect(SetupStatus);
const decodeResourceId = Schema.decodeEffect(ManagedCloudSandboxResourceId);
const isWorktreeFailure = Schema.is(WorktreeMcpFailure);
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(WorktreeMcpHandoffInput));
const failure = (message: string) => new WorktreeMcpFailure({ code: "operation_failed", message });
const invalid = (message: string) => new WorktreeMcpFailure({ code: "invalid_request", message });
const encodeTaskRequest = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const workspaceScope = (scope: McpThreadInvocationScope, workspaceId: string) =>
  ({
    ...scope,
    requestNamespace: `cloud-workspace:${workspaceId}`,
  }) satisfies McpThreadInvocationScope;

export class CloudSandboxWorkspaces extends Context.Service<
  CloudSandboxWorkspaces,
  {
    readonly setupThread: (
      projectId: ProjectId,
      threadId: ThreadId,
    ) => Effect.Effect<{ readonly completed: true }, WorktreeMcpFailure>;
    readonly prepareBranch: (
      projectId: ProjectId,
      branch: string,
    ) => Effect.Effect<{ readonly branch: string }, WorktreeMcpFailure>;
    readonly handoff: (
      scope: McpThreadInvocationScope,
      input: WorktreeMcpHandoffInput,
    ) => Effect.Effect<SandboxWorktreeHandoffResult, WorktreeMcpFailure>;
    readonly remove: (
      scope: McpThreadInvocationScope,
      workspaceId: string,
    ) => Effect.Effect<
      { readonly workspaceId: string; readonly deleted: boolean },
      WorktreeMcpFailure
    >;
    readonly sharePreview: (
      port: number,
    ) => Effect.Effect<
      { readonly url: string; readonly port: number; readonly isPublic: boolean },
      WorktreeMcpFailure
    >;
    /**
     * Delegates one task into a fresh ephemeral sandbox workspace provisioned
     * from a configured profile. The fleet primitive: each call gets its own
     * disposable sandbox.
     */
    readonly delegateSandboxTask: (
      scope: McpThreadInvocationScope,
      input: {
        readonly task: string;
        readonly modelSelection: ModelSelection;
        readonly runtimeMode: RuntimeMode;
        readonly interactionMode: ProviderInteractionMode;
        readonly title?: string | undefined;
        readonly branch: string;
        readonly workspaceId: string;
        readonly size?: string | undefined;
        readonly autoStopMinutes?: number | undefined;
        readonly profile?: SandboxProfileId | undefined;
        readonly runSetupScript: boolean;
        readonly deleteOnCompletion?: boolean | undefined;
      },
    ) => Effect.Effect<SandboxWorktreeHandoffResult, WorktreeMcpFailure>;
    /**
     * Reclaims sandbox workspaces whose source thread is gone and whose VM has
     * already stopped, deleting their disk and peer link. A workspace still
     * being created is also reclaimed once its VM has stopped, even while its
     * source thread lives.
     */
    readonly sweep: Effect.Effect<number, WorktreeMcpFailure>;
    readonly recoverOwnership: Effect.Effect<number, WorktreeMcpFailure>;
    /**
     * Points each managed child's peer link at this server's current port.
     * Does nothing where the controller reaches children directly.
     */
    readonly repointLinks: Effect.Effect<number, WorktreeMcpFailure>;
    /**
     * Reports each task-owned follower's degraded state, as the controller
     * stamped it, onto the owning task's card, and clears the card state when
     * the resource recovers. Only changes dispatch; repeated reports are
     * idempotent.
     */
    readonly reportIssues: Effect.Effect<number, WorktreeMcpFailure>;
    /** Forks the periodic sweep, link repointing and issue reporting; call once per server start. */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/workspace/CloudSandboxWorkspaces") {}

export const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const sql = yield* SqlClient.SqlClient;
  const random = yield* SecureRandom.SecureRandom;
  const git = yield* Git.GitWorkflowService;
  const driver = yield* GitDriver.GitVcsDriver;
  const controller = yield* Controller.CloudSandboxController;
  const links = yield* PeerLinks.PeerLinks;
  const forwarding = yield* PeerForwarding.PeerForwarding;
  const delegation = yield* RemoteDelegation.RemoteDelegation;
  const threads = yield* Threads.ThreadManagementService;
  const projects = yield* Projects.ProjectService;
  const setup = yield* Setup.ProjectSetupScriptRunner;
  const manager = yield* Manager.CloudSandboxManager;
  const profiles = yield* Profiles.CloudSandboxProfiles;
  const transfer = yield* ProjectTransfer.CloudSandboxProjectTransfer;
  const lock = yield* KeyedLock.make<string>();
  const loadSetupStatus = Effect.fn("CloudSandboxWorkspaces.loadSetupStatus")(
    function* (threadId: ThreadId) {
      const rows = yield* sql<{
        readonly status: string;
      }>`SELECT status FROM cloud_sandbox_thread_setups WHERE thread_id = ${threadId}`;
      return rows[0] ? yield* decodeSetupStatus(rows[0].status).pipe(Effect.asSome) : Option.none();
    },
    Effect.mapError(() => failure("Unable to read the child setup journal.")),
  );
  const saveSetupStatus = Effect.fn("CloudSandboxWorkspaces.saveSetupStatus")(
    function* (threadId: ThreadId, status: typeof SetupStatus.Type) {
      yield* sql`INSERT INTO cloud_sandbox_thread_setups (thread_id, status)
      VALUES (${threadId}, ${status})
      ON CONFLICT(thread_id) DO UPDATE SET status = excluded.status`;
    },
    Effect.mapError(() => failure("Unable to persist the child setup journal.")),
  );
  /**
   * Pairs a fresh sandbox connection into this environment, mirrors the source
   * project there, and prepares the workspace branch. Shared by the in-sandbox
   * handoff and the root-side ephemeral fleet.
   */
  const attachProject = Effect.fn("CloudSandboxWorkspaces.attachProject")(function* (
    scope: McpThreadInvocationScope,
    input: {
      readonly connection: ManagedCloudSandboxConnection;
      readonly pairingToken: string;
      readonly linkUrl: string;
      readonly sourceProjectId: ProjectId;
      readonly branch: string;
      readonly workspacePath?: string;
    },
  ) {
    const parent = yield* threads
      .getThreadRecords(scope.thread.threadId, [])
      .pipe(Effect.mapError(() => failure("Unable to read the source thread.")));
    const project = yield* projects
      .getById(input.sourceProjectId)
      .pipe(Effect.mapError(() => failure("Unable to read the source project.")));
    if (Option.isNone(project)) return yield* invalid("The source project no longer exists.");
    const linked = yield* links
      .get(input.connection.environmentId)
      .pipe(Effect.mapError(() => failure("Unable to inspect the child peer link.")));
    if (Option.isNone(linked)) {
      yield* links
        .link({
          url: input.linkUrl,
          pairingCode: input.pairingToken,
          access: parent.thread.runtimeMode,
          expectedEnvironmentId: input.connection.environmentId,
          expectedLabel: input.connection.label,
        })
        .pipe(
          Effect.mapError(() =>
            failure("Unable to pair the sandbox. Enable Linked environments on both environments."),
          ),
        );
    }
    const workspacePath =
      input.workspacePath ?? parent.thread.worktreePath ?? project.value.workspaceRoot;
    let childProjectId: ProjectId | undefined;
    let cursor: number | null = 0;
    while (cursor !== null) {
      const listed: import("effect/ai/Tool").Success<typeof ProjectToolkit.tools.t3_project_list> =
        yield* forwarding
          .call(scope, ProjectToolkit.tools.t3_project_list, input.connection.environmentId, {
            cursor,
            limit: 100,
          })
          .pipe(Effect.mapError(() => failure("Unable to inspect projects in the sandbox.")));
      childProjectId =
        listed.projects.find((candidate) => candidate.workspaceRoot === workspacePath)?.id ??
        childProjectId;
      cursor = listed.nextCursor;
    }
    if (!childProjectId) {
      const created = yield* forwarding
        .call(scope, ProjectToolkit.tools.t3_project_create, input.connection.environmentId, {
          title: project.value.title,
          scripts: project.value.scripts,
          workspaceRoot: workspacePath,
        })
        .pipe(Effect.mapError(() => failure("Unable to register the project in the sandbox.")));
      childProjectId = created.id;
    }
    yield* forwarding
      .call(scope, PrepareSandboxBranchTool, input.connection.environmentId, {
        projectId: childProjectId,
        branch: input.branch,
      })
      .pipe(Effect.mapError(() => failure("Unable to prepare the branch in the sandbox.")));
    return { childProjectId, workspacePath };
  });
  /** The in-sandbox handoff and the root fleet share one resource resolution. */
  const provisionWorkspace = Effect.fn("CloudSandboxWorkspaces.provisionWorkspace")(
    function* (
      workspaceId: string,
      input: {
        readonly name: string;
        readonly size?: string | undefined;
        readonly autoStopMinutes?: number | undefined;
        readonly profile?: SandboxProfileId | undefined;
        readonly ownership: {
          readonly sourceThreadId: ThreadId;
          readonly retention: ManagedSandboxRetention;
        };
      },
    ) {
      const decoded = yield* decodeResourceId(workspaceId).pipe(
        Effect.mapError(() => invalid("Invalid workspace id.")),
      );
      const listed = yield* profiles.list.pipe(
        Effect.mapError((cause) => invalid(`Unable to read sandbox profiles: ${cause.message}`)),
      );
      const profile = Option.fromNullishOr(
        input.profile !== undefined
          ? listed.find(({ profile }) => profile.profileId === input.profile)?.profile
          : listed[0]?.profile,
      );
      if (Option.isNone(profile))
        return yield* invalid(
          input.profile === undefined
            ? "No sandbox profile is configured. Add one in Settings → Environments → Add environment."
            : "The requested sandbox profile does not exist.",
        );
      const route = yield* manager.provision({
        operationId: decoded,
        profile: profile.value.profileId,
        sandbox: {
          requestId: workspaceId,
          ...(profile.value.template === null ? {} : { template: profile.value.template }),
          name: input.name,
          size: input.size ?? profile.value.defaultSize ?? "default",
          autoStopMinutes: input.autoStopMinutes ?? profile.value.autoStopMinutes,
        },
        enableLinkedEnvironments: true,
        ownership: input.ownership,
      });
      const controllerPort = yield* lifecycle.localServerPort;
      if (controllerPort <= 0) return yield* failure("Unable to determine the local server port.");
      return { route, base: `http://127.0.0.1:${controllerPort}` };
    },
    Effect.mapError(
      (
        cause:
          | WorktreeMcpFailure
          | import("../cloudSandbox/CloudSandboxStore.ts").CloudSandboxManagerError,
      ) =>
        isWorktreeFailure(cause) ? cause : invalid(`Sandbox allocation failed: ${cause.message}`),
    ),
  );
  /** A disk fork of the sandbox this controller runs in; the fork is the child's own sandbox. */
  const prepareWorkspace = Effect.fn("CloudSandboxWorkspaces.prepareWorkspace")(function* (
    scope: McpThreadInvocationScope,
    branch: string,
    acquired: AcquiredWorkspace,
  ) {
    const workspacePath =
      acquired.prepareCheckout === undefined ? undefined : yield* acquired.prepareCheckout;
    const parent = yield* threads
      .getThreadRecords(scope.thread.threadId, [])
      .pipe(Effect.mapError(() => failure("Unable to read the source thread.")));
    return yield* attachProject(scope, {
      connection: acquired.connection,
      pairingToken: acquired.pairingToken,
      linkUrl: acquired.linkUrl,
      sourceProjectId: parent.thread.projectId,
      branch,
      ...(workspacePath === undefined ? {} : { workspacePath }),
    });
  });
  const forkFromController = Effect.fn("CloudSandboxWorkspaces.forkFromController")(function* (
    workspaceId: ManagedCloudSandboxResourceId,
    branch: string,
    size: string | undefined,
    autoStopMinutes: number | undefined,
    ownership: { readonly sourceThreadId: ThreadId; readonly retention: ManagedSandboxRetention },
  ) {
    const fork = yield* controller.request({
      _tag: "fork",
      enableLinkedEnvironments: true,
      operationId: workspaceId,
      name: branch,
      ownership,
      ...(size === undefined ? {} : { size }),
      ...(autoStopMinutes === undefined ? {} : { autoStopMinutes }),
    });
    if (fork._tag !== "connection")
      return yield* failure("The controller did not return the forked environment.");
    return {
      connection: fork.connection,
      pairingToken: fork.pairingToken,
      linkUrl: fork.connection.routePrefix,
    } satisfies AcquiredWorkspace;
  });
  /**
   * Tells each delegated task's card when its follower sandbox is degraded, and
   * clears it when the resource recovers, by reading the issue the controller
   * stamped on the record. Presence is state: only changes dispatch, and a
   * repeated identical report replays the same command id.
   */
  const sameIssue = (
    left: ManagedCloudSandboxIssue | undefined,
    right: ManagedCloudSandboxIssue | undefined,
  ) =>
    left !== undefined &&
    right !== undefined &&
    left.cause === right.cause &&
    left.driverKind === right.driverKind &&
    left.profileId === right.profileId &&
    left.message === right.message &&
    left.firstSeenAt === right.firstSeenAt;

  /**
   * Reports one issue attempt as its own command, so a failed one never blocks
   * the next. True when the card cannot need another (delivered, or refused
   * because it already reflects the report); false when a transient failure
   * left it possibly untold, to retry next tick.
   */
  const tellTask = Effect.fn("CloudSandboxWorkspaces.tellTask")(function* (
    sourceThreadId: ThreadId,
    taskId: string,
    issue: ManagedCloudSandboxIssue | undefined,
  ) {
    const attempt = yield* random.uuidV4();
    return yield* threads
      .dispatch({
        type: "delegated_task.remote.issue",
        commandId: CommandId.make(`command:remote-task-issue:${taskId}:${attempt}`),
        parentThreadId: sourceThreadId,
        taskId: NodeId.make(taskId),
        ...(issue === undefined ? {} : { issue }),
      })
      .pipe(
        Effect.as(true),
        Effect.catch((cause) =>
          cause._tag === "OrchestratorDispatchError" &&
          cause.cause === "Command produced no domain events."
            ? Effect.succeed(true)
            : Effect.logWarning("Unable to report a follower sandbox issue to its task.", {
                sourceThreadId,
                taskId,
                cause,
              }).pipe(Effect.as(false)),
        ),
      );
  });

  const lifecycle = yield* WorkspaceLifecycle.make({ tellTask });

  const reportedIssues = new Map<
    string,
    {
      readonly sourceThreadId: ThreadId;
      readonly taskId: string;
      readonly issue: ManagedCloudSandboxIssue;
    }
  >();
  // Owners whose card this process already reconciled against its resource;
  // checked once per process, since a tick cannot see a stale card again.
  const reconciledOwners = new Set<string>();
  const reportIssues = Effect.fn("CloudSandboxWorkspaces.reportIssues")(function* () {
    const resources = yield* manager.list.pipe(
      Effect.mapError((cause) => failure(`Unable to list the managed sandboxes: ${cause.message}`)),
    );
    // Only task-owned followers of this controller have a card to tell here;
    // the host id was re-derived at bind, so the owner cannot name another.
    const failing = new Map<
      string,
      {
        readonly sourceThreadId: ThreadId;
        readonly taskId: string;
        readonly issue: ManagedCloudSandboxIssue;
      }
    >();
    const healthy = new Map<
      string,
      {
        readonly sourceThreadId: ThreadId;
        readonly taskId: string;
      }
    >();
    for (const issue of yield* lifecycle.pendingBindingIssues)
      failing.set(`${issue.sourceThreadId}\u0000${issue.taskId}`, issue);
    for (const resource of resources) {
      const { owner } = resource;
      if (owner._tag !== "task" || owner.hostEnvironmentId !== resource.controllerEnvironmentId) {
        continue;
      }
      const row = { sourceThreadId: owner.sourceThreadId, taskId: owner.taskId };
      if (failing.has(`${owner.sourceThreadId}\u0000${owner.taskId}`)) continue;
      if (resource.issue === undefined)
        healthy.set(`${owner.sourceThreadId}\u0000${owner.taskId}`, row);
      else
        failing.set(`${owner.sourceThreadId}\u0000${owner.taskId}`, {
          ...row,
          issue: resource.issue,
        });
    }
    for (const [key, next] of failing) {
      const previous = reportedIssues.get(key);
      if (sameIssue(previous?.issue, next.issue)) continue;
      // A settled card would refuse the report every tick; track it no longer,
      // since a terminal card ignores issues anyway.
      const read = yield* threads
        .getThreadRecords(next.sourceThreadId, ["subagents"], {
          subagentIds: [NodeId.make(next.taskId)],
        })
        .pipe(Effect.option);
      const task = Option.isSome(read) && read.value ? read.value.subagents.at(-1) : undefined;
      if (task !== undefined && task.result !== null) continue;
      const told = yield* tellTask(next.sourceThreadId, next.taskId, next.issue);
      if (told) reportedIssues.set(key, { ...next });
    }
    for (const [key, told] of reportedIssues) {
      if (failing.has(key)) continue;
      if (!(yield* tellTask(told.sourceThreadId, told.taskId, undefined))) continue;
      reportedIssues.delete(key);
    }
    // The told-map is process memory, so a card may still show an issue its
    // record recovered from while the server was down. Reconcile each owner
    // once: a card that is already quiet is done for good; a stale one goes
    // unmarked until its clear lands, so a failed dispatch retries.
    for (const [key, row] of healthy) {
      if (reportedIssues.has(key) || reconciledOwners.has(key)) continue;
      const read = yield* threads
        .getThreadRecords(row.sourceThreadId, ["subagents"], {
          subagentIds: [NodeId.make(row.taskId)],
        })
        .pipe(Effect.option);
      const task = Option.isSome(read) && read.value ? read.value.subagents.at(-1) : undefined;
      if (task === undefined) {
        reconciledOwners.add(key);
        continue;
      }
      const cardIssue = task.result === null ? task.remoteChild?.sandboxIssue : undefined;
      if (cardIssue === undefined || (yield* tellTask(row.sourceThreadId, row.taskId, undefined))) {
        reconciledOwners.add(key);
      }
    }
    return failing.size + (yield* lifecycle.reportControllerIssues);
  });
  const every = <A, E>(interval: Duration.Input, tick: Effect.Effect<A, E>, stopped: string) =>
    tick.pipe(
      Effect.ignore,
      Effect.andThen(Effect.sleep(interval)),
      Effect.forever,
      Effect.catchCause((cause) => Effect.logWarning(stopped, cause)),
      Effect.forkScoped,
      Effect.asVoid,
    );
  return CloudSandboxWorkspaces.of({
    setupThread: Effect.fn("CloudSandboxWorkspaces.setupThread")(function* (projectId, threadId) {
      if (!(yield* controller.available))
        return yield* invalid("Setup requires a managed sandbox.");
      const project = yield* projects
        .getById(projectId)
        .pipe(Effect.mapError(() => failure("Unable to read child project.")));
      const child = yield* threads
        .getThreadRecords(threadId, [])
        .pipe(Effect.mapError(() => failure("Unable to read child thread.")));
      if (Option.isNone(project) || child.thread.projectId !== projectId)
        return yield* invalid("The child thread must belong to this project.");
      return yield* lock.withLock(
        `setup:${threadId}`,
        Effect.gen(function* () {
          const previous = yield* loadSetupStatus(threadId);
          if (Option.isSome(previous)) {
            if (previous.value === "completed") return { completed: true as const };
            return yield* failure(
              "Child setup failed or was interrupted. Inspect its setup terminal before continuing; setup will not run twice.",
            );
          }
          yield* saveSetupStatus(threadId, "started");
          const started = yield* setup
            .runForThread({
              threadId,
              projectId,
              worktreePath: project.value.workspaceRoot,
              observeCompletion: {},
            })
            .pipe(
              Effect.mapError(() =>
                failure("Child setup could not start. Inspect the child thread."),
              ),
            );
          if (started.status === "skipped-for-link")
            return yield* failure("The child link does not permit project setup scripts.");
          if (started.status === "started") {
            if (!started.completion)
              return yield* failure("Child setup completion could not be observed.");
            const completion = yield* started.completion;
            if (completion.exitCode !== 0)
              return yield* failure(
                "Child setup failed. Inspect its setup terminal; continuation was not started.",
              );
          }
          yield* saveSetupStatus(threadId, "completed");
          return { completed: true as const };
        }),
      );
    }),
    prepareBranch: Effect.fn("CloudSandboxWorkspaces.prepareBranch")(function* (projectId, branch) {
      if (!(yield* controller.available))
        return yield* invalid("Branch preparation requires a managed sandbox.");
      const project = yield* projects
        .getById(projectId)
        .pipe(Effect.mapError(() => failure("Unable to inspect the sandbox project.")));
      if (Option.isNone(project))
        return yield* invalid("The project does not exist in this sandbox.");
      const cwd = project.value.workspaceRoot;
      return yield* lock.withLock(
        cwd,
        Effect.gen(function* () {
          const current = yield* git
            .currentBranch(cwd)
            .pipe(Effect.mapError(() => failure("Unable to inspect the sandbox checkout.")));
          if (current === branch) return { branch };
          const exists = yield* git
            .hasRefNamed({ cwd, refName: branch })
            .pipe(Effect.mapError(() => failure("Unable to inspect sandbox branches.")));
          if (exists) return yield* invalid("The requested sandbox branch already exists.");
          yield* driver
            .execute({
              operation: "CloudSandboxWorkspaces.prepareBranch",
              cwd,
              args: ["switch", "-c", branch, "--"],
            })
            .pipe(Effect.mapError(() => failure("Unable to create the sandbox branch.")));
          yield* git.invalidateLocalStatus(cwd);
          return { branch };
        }),
      );
    }),
    handoff: Effect.fn("CloudSandboxWorkspaces.handoff")(function* (scope, input) {
      if (input.isolation?.kind !== "sandbox")
        return yield* invalid("Sandbox isolation must be requested explicitly.");
      const isolation = input.isolation;
      const workspaceId = isolation.operationId;
      if (!(yield* controller.available))
        return yield* invalid("Sandbox isolation requires a managed cloud environment.");
      if (input.baseRef !== undefined || input.startFromOrigin === true)
        return yield* invalid(
          "A disk fork preserves the current checkout, including uncommitted files. Use filesystem isolation to start from another ref.",
        );
      const requestJson = yield* encodeRequest(input).pipe(
        Effect.mapError(() => invalid("The workspace request is invalid.")),
      );
      return yield* lifecycle.createWorkspace(scope, {
        workspaceId,
        requestJson,
        branch: input.branch,
        retention: "stop-and-retain",
        runSetupScript: input.runSetupScript !== false,
        note: "The continuation runs in a separate disk-forked environment. The source thread history stays here. Deleting this owned workspace deletes its sandbox; archiving the thread does not.",
        acquire: forkFromController(
          workspaceId,
          input.branch,
          isolation.size,
          isolation.autoStopMinutes,
          { sourceThreadId: scope.thread.threadId, retention: "stop-and-retain" },
        ),
        prepare: (acquired) =>
          prepareWorkspace(workspaceScope(scope, workspaceId), input.branch, acquired),
        delegate: ({ connection, childProjectId }) =>
          delegation
            .delegateWorkspace(
              workspaceScope(scope, workspaceId),
              {
                task:
                  input.continuationPrompt ??
                  "Continue work in this isolated sandbox workspace. Inspect the copied checkout before making changes.",
                target: {
                  environmentId: connection.environmentId,
                  projectId: childProjectId,
                },
                clientRequestId: `cloud-workspace:${workspaceId}`,
                role: "implementation",
              },
              { type: "root", branch: input.branch },
              input.runSetupScript !== false,
            )
            .pipe(
              Effect.mapError((cause) =>
                failure(
                  `Unable to start the continuation in the fork: ${cause.message} Retry the same operation ID.`,
                ),
              ),
            ),
      });
    }),
    remove: lifecycle.remove,
    sharePreview: Effect.fn("CloudSandboxWorkspaces.sharePreview")(function* (port) {
      if (!(yield* controller.available))
        return yield* invalid("Preview sharing requires a managed cloud environment.");
      const controllerPort = yield* lifecycle.localServerPort;
      if (port === controllerPort || port === config.port || port === MANAGED_CLOUD_SOURCE_PORT)
        return yield* invalid("The environment control server cannot be shared as a preview.");
      const hosted = yield* controller.request({ _tag: "host", port });
      if (hosted._tag !== "result" || hosted.result._tag !== "host")
        return yield* failure("The controller did not return a hosted preview URL.");
      return hosted.result.port;
    }),
    delegateSandboxTask: Effect.fn("CloudSandboxWorkspaces.delegateSandboxTask")(
      function* (scope, input) {
        const workspaceId = yield* decodeResourceId(input.workspaceId).pipe(
          Effect.mapError(() => invalid("Invalid owned sandbox resource ID.")),
        );
        // Inside a managed sandbox, fork its own controller; on the root server,
        // provision from the profile the way a worktree is created.
        const inSandbox = yield* controller.available;
        const acquire = inSandbox
          ? forkFromController(workspaceId, input.branch, input.size, input.autoStopMinutes, {
              sourceThreadId: scope.thread.threadId,
              retention:
                input.deleteOnCompletion === true ? "delete-on-completion" : "stop-and-retain",
            })
          : Effect.gen(function* () {
              const parent = yield* threads
                .getThreadRecords(scope.thread.threadId, [])
                .pipe(Effect.mapError(() => failure("Unable to read the source thread.")));
              const sourceProject = yield* projects
                .getById(parent.thread.projectId)
                .pipe(Effect.mapError(() => failure("Unable to inspect the source project.")));
              if (Option.isNone(sourceProject))
                return yield* invalid("The source project no longer exists.");
              const allocated = yield* provisionWorkspace(workspaceId, {
                name: input.branch,
                ownership: {
                  sourceThreadId: scope.thread.threadId,
                  retention:
                    input.deleteOnCompletion === true ? "delete-on-completion" : "stop-and-retain",
                },
                ...(input.size === undefined ? {} : { size: input.size }),
                ...(input.autoStopMinutes === undefined
                  ? {}
                  : { autoStopMinutes: input.autoStopMinutes }),
                ...(input.profile === undefined ? {} : { profile: input.profile }),
              });
              const connection = allocated.route.connection;
              return {
                connection,
                pairingToken: allocated.route.pairingToken,
                linkUrl: `${allocated.base}${connection.routePrefix}`,
                prepareCheckout: transfer.transfer({
                  resourceId: connection.resourceId,
                  cwd: parent.thread.worktreePath ?? sourceProject.value.workspaceRoot,
                }),
              } satisfies AcquiredWorkspace;
            });
        const requestJson = yield* encodeTaskRequest({
          task: input.task,
          modelSelection: input.modelSelection,
          runtimeMode: input.runtimeMode,
          interactionMode: input.interactionMode,
          title: input.title,
          branch: input.branch,
          size: input.size,
          autoStopMinutes: input.autoStopMinutes,
          profile: input.profile,
        }).pipe(Effect.mapError(() => invalid("The workspace request is invalid.")));
        return yield* lifecycle.createWorkspace(scope, {
          workspaceId,
          requestJson,
          branch: input.branch,
          retention: input.deleteOnCompletion === true ? "delete-on-completion" : "stop-and-retain",
          runSetupScript: input.runSetupScript,
          note: "The task runs in a disposable sandbox workspace. It is reclaimed automatically once its source thread is deleted and the sandbox has stopped; deleting it earlier removes the sandbox.",
          acquire,
          prepare: (acquired) =>
            prepareWorkspace(workspaceScope(scope, workspaceId), input.branch, acquired),
          delegate: ({ connection, childProjectId }) =>
            delegation
              .delegateWorkspace(
                workspaceScope(scope, input.workspaceId),
                {
                  task: input.task,
                  target: {
                    environmentId: connection.environmentId,
                    projectId: childProjectId,
                    providerInstanceId: input.modelSelection.instanceId,
                    model: input.modelSelection.model,
                    ...(input.modelSelection.options === undefined
                      ? {}
                      : { options: input.modelSelection.options }),
                  },
                  runtimeMode: input.runtimeMode,
                  interactionMode: input.interactionMode,
                  ...(input.title === undefined ? {} : { title: input.title }),
                  clientRequestId: `cloud-workspace:${input.workspaceId}`,
                  role: "implementation",
                },
                { type: "root", branch: input.branch },
                input.runSetupScript,
              )
              .pipe(
                Effect.mapError((cause) =>
                  failure(
                    `Unable to start the task in the sandbox: ${cause.message} Retry the same workspace id.`,
                  ),
                ),
              ),
        });
      },
    ),
    recoverOwnership: lifecycle.recoverOwnership,
    sweep: lifecycle.sweep,
    repointLinks: lifecycle.repointLinks,
    reportIssues: reportIssues(),
    /**
     * Reclaim-safe on failure: a sweep that cannot read the journal tries
     * again at the next tick. Link repointing retries each minute because the
     * runtime state file may still hold the previous port when start() runs.
     */
    start: Effect.fn("CloudSandboxWorkspaces.start")(() =>
      Effect.all(
        [
          every("30 minutes", lifecycle.sweep, "Cloud workspace sweep stopped."),
          every("1 minute", lifecycle.repointLinks, "Cloud workspace link repointing stopped."),
          every(
            "30 seconds",
            lifecycle.recoverOwnership.pipe(Effect.ignore, Effect.andThen(reportIssues())),
            "Cloud sandbox issue reporting stopped.",
          ),
        ],
        { discard: true },
      ),
    ),
  });
});
export const layer = Layer.effect(CloudSandboxWorkspaces, make).pipe(
  Layer.provide(ProjectTransfer.layer),
);
