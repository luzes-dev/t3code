import * as ServerConfig from "../config.ts";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  ProjectId,
  ManagedCloudSandboxResourceId,
  ManagedCloudSandboxSourceOperation,
  type ManagedCloudSandboxIssue,
  type ManagedCloudSandboxResource,
  type Project,
  ProviderDriverKind,
  ProviderInstanceId,
  SandboxProfileId,
  ThreadId,
  WorktreeMcpFailure,
  NodeId,
  OrchestratorMcpFailure,
  type OrchestrationV2Subagent,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import { HttpClient, HttpClientResponse } from "effect/http";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import { OrchestratorDispatchError } from "../orchestration-v2/Orchestrator.ts";
import * as Projects from "../project/ProjectService.ts";
import * as Links from "../peer/PeerLinks.ts";
import * as Forwarding from "../peer/PeerForwarding.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import * as Delegation from "../peer/RemoteDelegation.ts";
import * as Git from "../git/GitWorkflowService.ts";
import * as GitDriver from "../vcs/GitVcsDriver.ts";
import * as Controller from "./CloudSandboxController.ts";
import * as Manager from "../cloudSandbox/CloudSandboxManager.ts";
import { CloudSandboxManagerError } from "../cloudSandbox/CloudSandboxStore.ts";
import * as Profiles from "../cloudSandbox/CloudSandboxProfiles.ts";
import { descriptorOf, layerLinkingEnvironment } from "../peer/PeerLinks.testkit.ts";
import * as Settings from "../serverSettings.ts";
import * as Setup from "../project/ProjectSetupScriptRunner.ts";
import * as ProjectTransfer from "./CloudSandboxProjectTransfer.ts";
import * as Workspaces from "./CloudSandboxWorkspaces.ts";
import type { McpThreadInvocationScope } from "../mcp/McpInvocationContext.ts";

const decodeSourceOperation = Schema.decodeUnknownSync(
  Schema.fromJsonString(ManagedCloudSandboxSourceOperation),
);

const scope: McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("source"),
  requestNamespace: "session",
  issuedAt: 1,
  thread: {
    threadId: ThreadId.make("owner"),
    providerSessionId: "session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  capabilities: new Set(["orchestration", "worktree"]),
  client: undefined,
};

// Deterministic ids: the reporter's command ids must not repeat, but tests
// never assert them, so one filler byte is enough for every uuid it requests.
const randomLayer = Layer.succeed(
  SecureRandom.SecureRandom,
  SecureRandom.make((size) => new Uint8Array(size).fill(8)),
);

/** A running app-owned task card as the reporter reads it, optionally degraded. */
const remoteTaskCard = (sandboxIssue?: ManagedCloudSandboxIssue): OrchestrationV2Subagent => ({
  id: NodeId.make("task-1"),
  threadId: ThreadId.make("owner"),
  runId: null,
  parentNodeId: NodeId.make("node-1"),
  origin: "app_owned",
  createdBy: "agent",
  driver: ProviderDriverKind.make("boat"),
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerThreadId: null,
  childThreadId: null,
  ...(sandboxIssue === undefined
    ? {}
    : {
        remoteChild: {
          environmentId: EnvironmentId.make("environment:box"),
          threadId: ThreadId.make("thread-on-the-box"),
          label: "Box",
          sandboxIssue,
        },
      }),
  nativeTaskRef: null,
  prompt: "Run the suite there.",
  title: null,
  model: null,
  status: "running",
  result: null,
  startedAt: null,
  completedAt: null,
  updatedAt: DateTime.makeUnsafe("2026-01-01T00:00:00.000Z"),
});

it.effect.each([true, false])(
  "keeps failed deletion journaled and retries on either host (in sandbox: %s)",
  (inSandbox) => {
    let deletions = 0;
    const context = Layer.mergeAll(
      Sqlite.layerMemory,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-sandbox-workspaces-" }),
      Layer.mock(Controller.CloudSandboxController)({
        available: Effect.succeed(inSandbox),
        request: () => {
          if (!inSandbox)
            return Effect.die("A root workspace must delete through its local manager.");
          deletions++;
          return deletions === 1
            ? Effect.fail(
                new WorktreeMcpFailure({
                  code: "operation_failed",
                  message: "controller unavailable",
                }),
              )
            : Effect.succeed({ _tag: "deleted" } as const);
        },
      }),
      Layer.mock(Setup.ProjectSetupScriptRunner)({}),
      Layer.mock(Threads.ThreadManagementService)({}),
      Layer.mock(Projects.ProjectService)({}),
      Layer.mock(Links.PeerLinks)({}),
      Layer.mock(Forwarding.PeerForwarding)({}),
      Layer.mock(Delegation.RemoteDelegation)({}),
      Layer.mock(Manager.CloudSandboxManager)({
        list: Effect.succeed([]),
        release: () => {
          deletions++;
          return deletions === 1
            ? Effect.fail(new CloudSandboxManagerError({ reason: "provider" }))
            : Effect.void;
        },
      }),
      Layer.mock(Profiles.CloudSandboxProfiles)({ list: Effect.succeed([]) }),
      Layer.mock(Git.GitWorkflowService)({}),
      Layer.mock(GitDriver.GitVcsDriver)({}),
    );
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json) VALUES
      ('owned','owner','{"workspaceId":"owned","sourceThreadId":"owner","requestJson":"{}","phase":"creating","result":null}'),
      ('foreign','another','{"workspaceId":"foreign","sourceThreadId":"another","requestJson":"{}","phase":"creating","result":null}')`;
      const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
      const config = yield* ServerConfig.ServerConfig;
      expect((yield* workspaces.sharePreview(config.port).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      expect(deletions).toBe(0);
      const refused = yield* workspaces.remove(scope, "foreign").pipe(Effect.result);
      expect(refused._tag).toBe("Failure");
      expect(deletions).toBe(0);
      expect((yield* workspaces.remove(scope, "owned").pipe(Effect.result))._tag).toBe("Failure");
      const failed = yield* sql<{
        readonly payload_json: string;
      }>`SELECT payload_json FROM cloud_sandbox_workspaces WHERE workspace_id='owned'`;
      expect(failed[0]?.payload_json).toContain('"phase":"deleting"');
      expect(yield* workspaces.sweep).toBe(1);
      expect(yield* workspaces.remove(scope, "owned")).toEqual({
        workspaceId: "owned",
        deleted: true,
      });
      expect(yield* workspaces.remove(scope, "owned")).toEqual({
        workspaceId: "owned",
        deleted: true,
      });
      expect(deletions).toBe(2);
      yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json) VALUES
      ('cleanup','owner','{"workspaceId":"cleanup","sourceThreadId":"owner","requestJson":"{}","phase":"resource-deleted","result":null}')`;
      expect(yield* workspaces.remove(scope, "cleanup")).toEqual({
        workspaceId: "cleanup",
        deleted: true,
      });
      expect(deletions).toBe(2);
    }).pipe(
      Effect.provide(
        Workspaces.layer.pipe(
          Layer.provideMerge(context),
          Layer.provideMerge(randomLayer),
          Layer.provide(NodeServices.layer),
        ),
      ),
      Effect.scoped,
    );
  },
);

it.effect.each([false, true])("runs child setup once across retries (failure: %s)", (fails) => {
  let starts = 0;
  const projectId = ProjectId.make("child-project");
  const project: Project = {
    id: projectId,
    title: "child",
    workspaceRoot: "/child/repo",
    scripts: [],
    repositoryIdentity: null,
    defaultModelSelection: null,
    createdAt: "2026-01-01T00:00:00Z",
    updatedAt: "2026-01-01T00:00:00Z",
    deletedAt: null,
  };
  const context = Layer.mergeAll(
    Sqlite.layerMemory,
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-child-setup-" }),
    Layer.mock(Controller.CloudSandboxController)({ available: Effect.succeed(true) }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadRecords: () =>
        Effect.succeed({ thread: { projectId } } as unknown as Effect.Success<
          ReturnType<Threads.ThreadManagementService["Service"]["getThreadRecords"]>
        >),
    }),
    Layer.mock(Projects.ProjectService)({ getById: () => Effect.succeedSome(project) }),
    Layer.mock(Links.PeerLinks)({}),
    Layer.mock(Forwarding.PeerForwarding)({}),
    Layer.mock(Delegation.RemoteDelegation)({}),
    Layer.mock(Manager.CloudSandboxManager)({ list: Effect.succeed([]) }),
    Layer.mock(Profiles.CloudSandboxProfiles)({ list: Effect.succeed([]) }),
    Layer.mock(Git.GitWorkflowService)({}),
    Layer.mock(GitDriver.GitVcsDriver)({}),
    Layer.mock(Setup.ProjectSetupScriptRunner)({
      runForThread: (input) => {
        starts++;
        expect(input).toMatchObject({
          threadId: "child-thread",
          projectId,
          worktreePath: "/child/repo",
          observeCompletion: {},
        });
        return Effect.succeed({
          status: "started",
          scriptId: "setup",
          scriptName: "Setup",
          scriptCommand: "setup",
          terminalId: "setup",
          cwd: input.worktreePath,
          async: true,
          completion: Effect.succeed({ exitCode: fails ? 1 : 0, durationMs: 0 }),
        });
      },
    }),
  );
  return Effect.gen(function* () {
    const workspace = yield* Workspaces.CloudSandboxWorkspaces;
    const run = workspace.setupThread(projectId, ThreadId.make("child-thread")).pipe(Effect.result);
    expect((yield* run)._tag).toBe(fails ? "Failure" : "Success");
    expect((yield* run)._tag).toBe(fails ? "Failure" : "Success");
    expect(starts).toBe(1);
  }).pipe(
    Effect.provide(
      Workspaces.layer.pipe(
        Layer.provideMerge(context),
        Layer.provideMerge(randomLayer),
        Layer.provide(NodeServices.layer),
      ),
    ),
    Effect.scoped,
  );
});

it.effect("checks the real source gate before allocation and propagates child consent", () => {
  const requests: unknown[] = [];
  const projectId = ProjectId.make("source-project");
  const context = Layer.mergeAll(
    Layer.mock(Controller.CloudSandboxController)({
      available: Effect.succeed(true),
      request: (input) => {
        requests.push(input);
        return Effect.fail(
          new WorktreeMcpFailure({
            code: "operation_failed",
            message: "allocation fixture stops here",
          }),
        );
      },
    }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadRecords: () =>
        Effect.succeed({ thread: { projectId, runtimeMode: "auto" } } as unknown as Effect.Success<
          ReturnType<Threads.ThreadManagementService["Service"]["getThreadRecords"]>
        >),
    }),
    Layer.mock(Projects.ProjectService)({
      getById: () =>
        Effect.succeedSome({
          id: projectId,
          title: "source",
          workspaceRoot: "/source/repo",
          scripts: [],
        } as unknown as Project),
    }),
    Layer.mock(Forwarding.PeerForwarding)({}),
    Layer.mock(Delegation.RemoteDelegation)({}),
    Layer.mock(Manager.CloudSandboxManager)({ list: Effect.succeed([]) }),
    Layer.mock(Profiles.CloudSandboxProfiles)({ list: Effect.succeed([]) }),
    Layer.mock(Git.GitWorkflowService)({}),
    Layer.mock(GitDriver.GitVcsDriver)({}),
    Layer.mock(Setup.ProjectSetupScriptRunner)({}),
  );
  return Effect.gen(function* () {
    const workspace = yield* Workspaces.CloudSandboxWorkspaces;
    const settings = yield* Settings.ServerSettingsService;
    const input = {
      branch: "child",
      isolation: {
        kind: "sandbox",
        operationId: ManagedCloudSandboxResourceId.make("owned-child"),
      },
    } as const;
    yield* settings.updateSettings({ linkedEnvironmentsEnabled: false });
    expect((yield* workspace.handoff(scope, input).pipe(Effect.result))._tag).toBe("Failure");
    expect(requests).toEqual([]);
    yield* settings.updateSettings({ linkedEnvironmentsEnabled: true });
    yield* workspace.handoff(scope, input).pipe(Effect.result);
    expect(requests).toEqual([
      {
        _tag: "fork",
        operationId: "owned-child",
        name: "child",
        enableLinkedEnvironments: true,
        ownership: { sourceThreadId: "owner", retention: "stop-and-retain" },
      },
    ]);
  }).pipe(
    Effect.provide(
      Workspaces.layer.pipe(
        Layer.provideMerge(context),
        Layer.provideMerge(layerLinkingEnvironment(descriptorOf("source", "Source"))),
        Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-workspace-gate-" })),
        Layer.provideMerge(randomLayer),
        Layer.provide(NodeServices.layer),
      ),
    ),
    Effect.scoped,
  );
});

it.effect.each([
  {
    record: "running",
    threadLive: false,
    phase: "connected",
    state: "stopped",
    fails: false,
    attempts: 1,
    reclaimed: 1,
  },
  {
    record: "running",
    threadLive: false,
    phase: "failed",
    state: "ready",
    fails: false,
    attempts: 0,
    reclaimed: 0,
  },
  {
    record: "running",
    threadLive: false,
    phase: "stopped",
    state: "stopped",
    fails: true,
    attempts: 1,
    reclaimed: 0,
  },
  {
    record: "creating",
    threadLive: true,
    phase: "connected",
    state: "stopped",
    fails: false,
    attempts: 1,
    reclaimed: 1,
  },
  {
    record: "creating",
    threadLive: true,
    phase: "connected",
    state: "ready",
    fails: false,
    attempts: 0,
    reclaimed: 0,
  },
  {
    record: "running",
    threadLive: true,
    phase: "connected",
    state: "stopped",
    fails: false,
    attempts: 0,
    reclaimed: 0,
  },
  {
    record: "running",
    threadLive: true,
    phase: "deleted",
    state: "stopped",
    fails: false,
    attempts: 0,
    reclaimed: 1,
  },
  {
    record: "running",
    threadLive: true,
    retention: "delete-on-completion",
    taskSettled: true,
    phase: "connected",
    state: "stopped",
    fails: false,
    attempts: 1,
    reclaimed: 1,
  },
  {
    record: "running",
    threadLive: true,
    retention: "delete-on-completion",
    taskSettled: false,
    phase: "connected",
    state: "stopped",
    fails: false,
    attempts: 0,
    reclaimed: 0,
  },
  {
    record: "running",
    threadLive: true,
    retention: "delete-on-completion",
    taskSettled: true,
    phase: "connected",
    state: "ready",
    fails: false,
    attempts: 0,
    reclaimed: 0,
  },
] as const)("reclaims only stopped VMs after confirmed deletion (%s)", (scenario) => {
  const resource = {
    resourceId: ManagedCloudSandboxResourceId.make("owned"),
    controllerEnvironmentId: EnvironmentId.make("controller"),
    driverKind: "boat" as const,
    profileId: SandboxProfileId.make("boat"),
    sandboxId: "sandbox",
    parentResourceId: null,
    phase: scenario.phase as "connected" | "failed" | "stopped" | "deleted",
    owner: { _tag: "user" } as const,
    retention: (scenario.retention ?? "manual") as "manual" | "delete-on-completion",
    environmentId: EnvironmentId.make("child"),
    label: "Child",
    recordingIds: [],
  };
  let attempts = 0;
  let unlinks = 0;
  const dependencies = Layer.mergeAll(
    Sqlite.layerMemory,
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-workspace-sweep-" }),
    Layer.mock(Controller.CloudSandboxController)({ available: Effect.succeed(false) }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadShell: () =>
        Effect.succeed(scenario.threadLive ? liveThreadShell(ThreadId.make("owner")) : null),
      streamStoredEventsFrom: (input) =>
        input?.eventType !== "subagent.updated"
          ? Stream.empty
          : Stream.fromIterable([
              {
                sequence: 1,
                event: {
                  type: "subagent.updated",
                  payload: {
                    id: "task-1",
                    status: scenario.taskSettled === true ? "completed" : "running",
                  },
                },
              } as unknown as OrchestrationV2StoredEvent,
            ]),
    }),
    Layer.mock(Projects.ProjectService)({}),
    Layer.mock(Links.PeerLinks)({
      unlink: () =>
        Effect.sync(() => {
          unlinks++;
          return true;
        }),
    }),
    Layer.mock(Forwarding.PeerForwarding)({}),
    Layer.mock(Delegation.RemoteDelegation)({}),
    Layer.mock(Git.GitWorkflowService)({}),
    Layer.mock(GitDriver.GitVcsDriver)({}),
    Layer.mock(Setup.ProjectSetupScriptRunner)({}),
    Layer.mock(Profiles.CloudSandboxProfiles)({}),
    Layer.mock(Manager.CloudSandboxManager)({
      get: () => Effect.succeed(resource),
      resolve: () => Effect.succeedSome(resource),
      execute: () =>
        Effect.succeed({
          _tag: "status",
          sandbox: {
            sandboxId: "sandbox",
            driverKind: "boat",
            name: "Child",
            state: scenario.state,
            size: "small",
            autoStopDeadline: null,
            createdAt: "2026-01-01T00:00:00Z",
          },
          capabilities: {
            diskForking: false,
            desktopStreaming: false,
            autoStopKeepalive: false,
            publicPortHosting: false,
            hotResize: false,
            rootAccess: false,
          },
        }),
      release: () =>
        Effect.suspend(() => {
          attempts++;
          return scenario.fails
            ? Effect.fail(new CloudSandboxManagerError({ reason: "provider" }))
            : Effect.void;
        }),
    }),
  );
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const payload = JSON.stringify({
      workspaceId: "owned",
      sourceThreadId: "owner",
      requestJson: "{}",
      phase: scenario.record,
      childEnvironmentId: "child",
      result:
        "retention" in scenario && scenario.retention === "delete-on-completion"
          ? {
              kind: "sandbox",
              workspaceId: "owned",
              resourceId: "owned",
              environmentId: "child",
              projectId: "project-1",
              threadId: "child-thread-1",
              taskId: "task-1",
              workspacePath: "/source/repo",
              branch: "sandbox/owned",
              note: "test",
            }
          : null,
    });
    yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json)
      VALUES ('owned','owner',${payload})`;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    expect(yield* workspaces.sweep).toBe(scenario.reclaimed);
    expect(attempts).toBe(scenario.attempts);
    expect(unlinks).toBe(scenario.reclaimed);
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM cloud_sandbox_workspaces WHERE workspace_id='owned'`;
    expect(rows[0]?.payload_json.includes('"phase":"deleted"')).toBe(scenario.reclaimed === 1);
  }).pipe(
    Effect.provide(
      Workspaces.layer.pipe(
        Layer.provideMerge(dependencies),
        Layer.provideMerge(randomLayer),
        Layer.provide(NodeServices.layer),
      ),
    ),
    Effect.scoped,
  );
});

it.effect(
  "delegates through the controller's expanded route and preserves the requested execution settings",
  () => {
    const shell = liveThreadShell(scope.thread.threadId);
    const parent = idleThreadProjection(shell);
    const projectId = parent.thread.projectId;
    const project: Project = {
      id: projectId,
      title: "Source",
      workspaceRoot: "/source/repo",
      scripts: [],
      repositoryIdentity: null,
      defaultModelSelection: null,
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-01T00:00:00Z",
      deletedAt: null,
    };
    const input = {
      task: "Inspect the checkout",
      branch: "sandbox/task",
      workspaceId: "workspace",
      modelSelection: {
        instanceId: ProviderInstanceId.make("other-codex"),
        model: "other-model",
        options: [{ id: "reasoningEffort", value: "low" }],
      },
      runtimeMode: "approval-required",
      interactionMode: "plan",
      runSetupScript: true,
    } as const;
    const connection = {
      sandboxId: "sandbox",
      driverKind: "boat",
      resourceId: "workspace",
      controllerEnvironmentId: "controller",
      environmentId: "child",
      label: "Child",
      routePrefix: "/api/cloud-sandboxes/managed/workspace/proxy",
      bearerToken: "fixture",
    };
    let receivedRoute: string | undefined;
    let receivedExecution:
      | Parameters<Delegation.RemoteDelegation["Service"]["delegateWorkspace"]>[1]
      | undefined;
    let bindAvailable = false;
    let allocations = 0;
    let issue: ManagedCloudSandboxIssue | undefined;
    const resource = {
      ...connection,
      profileId: "boat",
      parentResourceId: "source",
      owner: { _tag: "user" },
      retention: "manual",
      phase: "connected",
      recordingIds: [],
    };
    const client = HttpClient.make((request) =>
      Effect.sync(() => {
        if (request.body._tag !== "Uint8Array") throw new Error("Missing controller request");
        const operation = decodeSourceOperation(new TextDecoder().decode(request.body.body));
        if (operation._tag === "get")
          return HttpClientResponse.fromWeb(request, Response.json({ _tag: "resource", resource }));
        if (operation._tag === "bindOwner")
          return HttpClientResponse.fromWeb(
            request,
            bindAvailable
              ? Response.json({
                  _tag: "resource",
                  resource: { ...resource, owner: operation.owner, retention: operation.retention },
                })
              : Response.json({ error: "unavailable" }, { status: 503 }),
          );
        allocations++;
        return HttpClientResponse.fromWeb(
          request,
          Response.json({ _tag: "connection", connection, pairingToken: "fresh" }),
        );
      }),
    );
    const peerCalls = Layer.mock(PeerMcpClient.PeerMcpClient)({
      call: ({ tool, success }) =>
        Schema.decodeEffect(Schema.fromJsonString(success))(
          JSON.stringify(
            tool === "t3_project_list"
              ? { projects: [], nextCursor: null }
              : tool === "t3_project_create"
                ? { ...project, id: "child-project" }
                : { branch: input.branch },
          ),
        ).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({ code: "invalid_request", message: "fixture response" }),
          ),
        ),
    });
    const baseDependencies = Layer.mergeAll(
      Sqlite.layerMemory,
      Layer.mock(Threads.ThreadManagementService)({
        getThreadRecords: () =>
          Effect.succeed({
            ...parent,
            subagents: [{ ...remoteTaskCard(issue), id: NodeId.make("task") }],
          }),
        dispatch: (command) =>
          Effect.sync(() => {
            if (command.type === "delegated_task.remote.issue") issue = command.issue;
            return { sequence: 1, storedEvents: [] };
          }),
        getThreadShell: () => Effect.succeed(shell),
      }),
      Layer.mock(Projects.ProjectService)({ getById: () => Effect.succeedSome(project) }),
      Layer.mock(Links.PeerLinks)({
        enabled: Effect.succeed(true),
        get: () => Effect.succeedNone,
        link: (request) =>
          Effect.sync(() => {
            receivedRoute = request.url;
            return {
              environmentId: EnvironmentId.make("child"),
              label: "Child",
              urls: [request.url],
              access: "auto" as const,
              linkedAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
              expiresAt: DateTime.makeUnsafe("2026-02-01T00:00:00Z"),
              lastReachedAt: null,
              lastError: null,
            };
          }),
      }),
      Layer.mock(Delegation.RemoteDelegation)({
        delegateWorkspace: (_scope, request) =>
          Effect.sync(() => {
            receivedExecution = request;
            return {
              taskId: NodeId.make("task"),
              threadId: ThreadId.make("child-thread"),
              projectId: ProjectId.make("child-project"),
            };
          }),
      }),
      Layer.mock(Git.GitWorkflowService)({}),
      Layer.mock(GitDriver.GitVcsDriver)({}),
      Layer.mock(Setup.ProjectSetupScriptRunner)({}),
      Layer.mock(Profiles.CloudSandboxProfiles)({}),
      Layer.mock(Manager.CloudSandboxManager)({}),
    );
    const dependencies = Forwarding.layer.pipe(
      Layer.provideMerge(Layer.mergeAll(baseDependencies, peerCalls)),
    );
    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(
        `${config.baseDir}/managed-cloud.json`,
        JSON.stringify({
          resourceId: "source",
          controllerEnvironmentId: "controller",
          controllerBaseUrl: "http://127.0.0.1:47773",
          grantToken: "fixture",
        }),
      );
      return yield* Effect.gen(function* () {
        const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
        const result = yield* workspaces.delegateSandboxTask(scope, input);
        expect(result.environmentId).toBe("child");
        expect(receivedRoute).toBe(
          "http://127.0.0.1:47773/api/cloud-sandboxes/managed/workspace/proxy",
        );
        expect(receivedExecution?.target).toMatchObject({
          providerInstanceId: input.modelSelection.instanceId,
          model: input.modelSelection.model,
          options: input.modelSelection.options,
        });
        expect(receivedExecution?.runtimeMode).toBe(input.runtimeMode);
        expect(receivedExecution?.interactionMode).toBe(input.interactionMode);
        expect(issue?.cause).toBe("bind");
        bindAvailable = true;
        expect(yield* workspaces.delegateSandboxTask(scope, input)).toEqual(result);
        expect(issue).toBeUndefined();
        expect(allocations).toBe(1);
        const reused = yield* workspaces
          .delegateSandboxTask(scope, { ...input, task: "Inspect something else" })
          .pipe(Effect.flip);
        expect(reused.message).toContain("already belongs to another request");
      }).pipe(
        Effect.provide(
          Workspaces.layer.pipe(Layer.provideMerge(dependencies), Layer.provide(Controller.layer)),
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-expanded-controller-" }),
          Layer.succeed(HttpClient.HttpClient, client),
        ).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
      ),
      Effect.scoped,
    );
  },
);

it.effect(
  "provisions root fleet sandboxes with linked environments, transfers the source checkout, and links via persisted runtime port",
  () => {
    const shell = liveThreadShell(scope.thread.threadId);
    const parent = idleThreadProjection(shell);
    const projectId = parent.thread.projectId;
    const project: Project = {
      id: projectId,
      title: "Source",
      workspaceRoot: "/source/repo",
      scripts: [],
      repositoryIdentity: null,
      defaultModelSelection: null,
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-01T00:00:00Z",
      deletedAt: null,
    };
    const input = {
      task: "Implement feature",
      title: "Sandbox task",
      branch: "sandbox/root-task",
      workspaceId: "root-workspace",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      runSetupScript: false,
    } as const;
    let provisionedLinkedEnvironments: boolean | undefined;
    let transferredCwd: string | undefined;
    let linkedUrl: string | undefined;
    let linkedToken: string | undefined;
    let createdWorkspaceRoot: string | undefined;
    const peerCalls = Layer.mock(PeerMcpClient.PeerMcpClient)({
      call: ({ tool, arguments: args, success }) => {
        if (tool === "t3_project_create" && typeof args.workspaceRoot === "string") {
          createdWorkspaceRoot = args.workspaceRoot;
        }
        return Schema.decodeEffect(Schema.fromJsonString(success))(
          JSON.stringify(
            tool === "t3_project_list"
              ? { projects: [], nextCursor: null }
              : tool === "t3_project_create"
                ? { ...project, id: "child-project", workspaceRoot: String(args.workspaceRoot) }
                : { branch: input.branch },
          ),
        ).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({ code: "invalid_request", message: "fixture response" }),
          ),
        );
      },
    });
    const baseDependencies = Layer.mergeAll(
      Sqlite.layerMemory,
      Layer.mock(Controller.CloudSandboxController)({ available: Effect.succeed(false) }),
      Layer.mock(Threads.ThreadManagementService)({
        getThreadRecords: () => Effect.succeed(parent),
        getThreadShell: () => Effect.succeed(shell),
      }),
      Layer.mock(Projects.ProjectService)({ getById: () => Effect.succeedSome(project) }),
      Layer.mock(Links.PeerLinks)({
        enabled: Effect.succeed(true),
        get: () => Effect.succeedNone,
        link: (request) =>
          Effect.sync(() => {
            linkedUrl = request.url;
            linkedToken = request.pairingCode;
            return {
              environmentId: EnvironmentId.make("child"),
              label: "Child",
              urls: [request.url],
              access: "auto" as const,
              linkedAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
              expiresAt: DateTime.makeUnsafe("2026-02-01T00:00:00Z"),
              lastReachedAt: null,
              lastError: null,
            };
          }),
      }),
      Layer.mock(Delegation.RemoteDelegation)({
        delegateWorkspace: () =>
          Effect.succeed({
            taskId: NodeId.make("task"),
            threadId: ThreadId.make("child-thread"),
            projectId: ProjectId.make("child-project"),
          }),
      }),
      Layer.mock(Git.GitWorkflowService)({}),
      Layer.mock(GitDriver.GitVcsDriver)({}),
      Layer.mock(Setup.ProjectSetupScriptRunner)({}),
      Layer.mock(Profiles.CloudSandboxProfiles)({
        list: Effect.succeed([
          {
            profile: {
              profileId: SandboxProfileId.make("boat"),
              name: "Boat",
              driverKind: "boat",
              apiKeySet: true,
              defaultSize: "small",
              autoStopMinutes: 60,
              template: null,
              monthlyBudgetUsd: 25,
              maxConcurrentSandboxes: 3,
            },
            spend: null,
          },
        ]),
      }),
      Layer.mock(Manager.CloudSandboxManager)({
        get: () => Effect.fail(new CloudSandboxManagerError({ reason: "not-found" })),
        provision: (request) =>
          Effect.sync(() => {
            provisionedLinkedEnvironments = request.enableLinkedEnvironments;
            return {
              httpBaseUrl: "http://127.0.0.1:40100",
              pairingToken: "fresh-pairing-token",
              connection: {
                sandboxId: "sandbox",
                driverKind: "boat" as const,
                resourceId: ManagedCloudSandboxResourceId.make("root-workspace"),
                controllerEnvironmentId: EnvironmentId.make("controller"),
                environmentId: EnvironmentId.make("child"),
                label: "Child",
                routePrefix: "/api/cloud-sandboxes/managed/root-workspace/proxy",
                bearerToken: "transport-bearer",
              },
            };
          }),
      }),
      Layer.succeed(ProjectTransfer.CloudSandboxProjectTransfer, {
        transfer: ({ cwd }) =>
          Effect.sync(() => {
            transferredCwd = cwd;
            return "/home/sandbox/.t3/cloud-workspaces/root-workspace/repo";
          }),
      }),
    );
    const dependencies = Forwarding.layer.pipe(
      Layer.provideMerge(Layer.mergeAll(baseDependencies, peerCalls)),
    );
    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(
        config.serverRuntimeStatePath,
        JSON.stringify({
          version: 1,
          pid: process.pid,
          port: 43123,
          origin: "http://127.0.0.1:43123",
          startedAt: "2026-01-01T00:00:00.000Z",
        }),
      );
      return yield* Effect.gen(function* () {
        const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
        const result = yield* workspaces.delegateSandboxTask(scope, input);
        expect(provisionedLinkedEnvironments).toBe(true);
        expect(transferredCwd).toBe("/source/repo");
        expect(linkedUrl).toBe(
          "http://127.0.0.1:43123/api/cloud-sandboxes/managed/root-workspace/proxy",
        );
        expect(linkedToken).toBe("fresh-pairing-token");
        expect(createdWorkspaceRoot).toBe("/home/sandbox/.t3/cloud-workspaces/root-workspace/repo");
        expect(result.workspacePath).toBe("/home/sandbox/.t3/cloud-workspaces/root-workspace/repo");
      }).pipe(
        Effect.provide(
          Layer.effect(Workspaces.CloudSandboxWorkspaces, Workspaces.make).pipe(
            Layer.provideMerge(dependencies),
          ),
        ),
      );
    }).pipe(
      Effect.provide(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-root-fleet-" }).pipe(
          Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
      Effect.scoped,
    );
  },
);

it.effect(
  "keeps setupThread receipts out of workspace reclamation and isolates per-record sweep failures",
  () => {
    const childThreadId = ThreadId.make("thread:child-setup");
    const childShell = liveThreadShell(childThreadId);
    const childProjection = idleThreadProjection(childShell);
    const project: Project = {
      id: childProjection.thread.projectId,
      title: "Child project",
      workspaceRoot: "/workspace/repo",
      scripts: [
        {
          id: "setup",
          name: "Setup",
          command: "bun install",
          icon: "play",
          runOnWorktreeCreate: true,
        },
      ],
      repositoryIdentity: null,
      defaultModelSelection: null,
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-01T00:00:00Z",
      deletedAt: null,
    };
    const healthyResource = {
      owner: { _tag: "user" } as const,
      retention: "manual" as const,
      resourceId: ManagedCloudSandboxResourceId.make("owned-healthy"),
      controllerEnvironmentId: EnvironmentId.make("controller"),
      driverKind: "boat" as const,
      profileId: SandboxProfileId.make("boat"),
      sandboxId: "sandbox-healthy",
      parentResourceId: null,
      phase: "connected" as const,
      environmentId: EnvironmentId.make("child-healthy"),
      label: "Healthy Child",
      recordingIds: [],
    };
    const brokenResource = {
      ...healthyResource,
      resourceId: ManagedCloudSandboxResourceId.make("owned-broken"),
      sandboxId: "sandbox-broken",
      environmentId: EnvironmentId.make("child-broken"),
    };
    let inSandbox = true;
    let setupRuns = 0;
    const released: string[] = [];
    const unlinked: string[] = [];
    const dependencies = Layer.mergeAll(
      Sqlite.layerMemory,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-workspace-setup-sweep-" }),
      Layer.mock(Controller.CloudSandboxController)({
        available: Effect.sync(() => inSandbox),
      }),
      Layer.mock(Threads.ThreadManagementService)({
        getThreadRecords: (id) =>
          id === childThreadId
            ? Effect.succeed(childProjection)
            : Effect.fail(new Error(`unexpected thread ${id}`) as never),
        getThreadShell: () => Effect.succeed(null),
      }),
      Layer.mock(Projects.ProjectService)({
        getById: (id) => (id === project.id ? Effect.succeedSome(project) : Effect.succeedNone),
      }),
      Layer.mock(Links.PeerLinks)({
        unlink: (envId) =>
          envId === brokenResource.environmentId
            ? Effect.fail(new Error("unlink failed") as never)
            : Effect.sync(() => {
                unlinked.push(envId);
                return true;
              }),
      }),
      Layer.mock(Forwarding.PeerForwarding)({}),
      Layer.mock(Delegation.RemoteDelegation)({}),
      Layer.mock(Git.GitWorkflowService)({}),
      Layer.mock(GitDriver.GitVcsDriver)({}),
      Layer.mock(Setup.ProjectSetupScriptRunner)({
        runForThread: (input) =>
          Effect.sync(() => {
            setupRuns++;
            return {
              status: "started" as const,
              scriptId: "setup",
              scriptName: "Setup",
              scriptCommand: "bun install",
              terminalId: "term-1",
              cwd: input.worktreePath,
              async: true,
              completion: Effect.succeed({ exitCode: 0, durationMs: 0 }),
            };
          }),
      }),
      Layer.mock(Profiles.CloudSandboxProfiles)({}),
      Layer.mock(Manager.CloudSandboxManager)({
        get: (id) =>
          id === healthyResource.resourceId
            ? Effect.succeed(healthyResource)
            : id === brokenResource.resourceId
              ? Effect.succeed(brokenResource)
              : Effect.fail(new CloudSandboxManagerError({ reason: "not-found" })),
        resolve: () => Effect.succeedNone,
        execute: (resourceId) =>
          Effect.succeed({
            _tag: "status" as const,
            sandbox: {
              sandboxId: resourceId,
              driverKind: "boat" as const,
              name: resourceId,
              state: "stopped" as const,
              size: "small" as const,
              autoStopDeadline: null,
              createdAt: "2026-01-01T00:00:00Z",
            },
            capabilities: {
              diskForking: false,
              desktopStreaming: false,
              autoStopKeepalive: false,
              publicPortHosting: false,
              hotResize: false,
              rootAccess: false,
            },
          }),
        release: (id) =>
          Effect.sync(() => {
            released.push(id);
          }),
      }),
    );

    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const workspaces = yield* Workspaces.CloudSandboxWorkspaces;

      yield* workspaces.setupThread(project.id, childThreadId);
      yield* workspaces.setupThread(project.id, childThreadId);
      expect(setupRuns).toBe(1);

      yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json)
        VALUES ('corrupt:id','owner','{"workspaceId":"corrupt:id","sourceThreadId":"owner","requestJson":"{}","phase":"running","childEnvironmentId":"child-corrupt","result":null}')`;
      yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json)
        VALUES ('owned-broken','owner','{"workspaceId":"owned-broken","sourceThreadId":"owner","requestJson":"{}","phase":"running","childEnvironmentId":"child-broken","result":null}')`;
      yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json)
        VALUES ('owned-healthy','owner','{"workspaceId":"owned-healthy","sourceThreadId":"owner","requestJson":"{}","phase":"running","childEnvironmentId":"child-healthy","result":null}')`;

      inSandbox = false;
      expect(yield* workspaces.sweep).toBe(1);
      expect(released).toEqual(["owned-broken", "owned-healthy"]);
      expect(unlinked).toEqual(["child-healthy"]);
    }).pipe(
      Effect.provide(
        Workspaces.layer.pipe(
          Layer.provideMerge(dependencies),
          Layer.provideMerge(randomLayer),
          Layer.provide(NodeServices.layer),
        ),
      ),
      Effect.scoped,
    );
  },
);

const rootInput = {
  task: "Implement feature",
  title: "Sandbox task",
  branch: "sandbox/root-task",
  workspaceId: "root-workspace",
  modelSelection: {
    instanceId: ProviderInstanceId.make("codex"),
    model: "gpt-5.4",
  },
  runtimeMode: "full-access",
  interactionMode: "default",
  runSetupScript: false,
} as const;

const writeControllerPort = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  yield* fs.writeFileString(
    config.serverRuntimeStatePath,
    JSON.stringify({
      version: 1,
      pid: process.pid,
      port: 43123,
      origin: "http://127.0.0.1:43123",
      startedAt: "2026-01-01T00:00:00.000Z",
    }),
  );
});

/**
 * A root server that provisions a profile sandbox per workspace id. Its child
 * refuses interrupts the way a dead proxy does; every provision, release and
 * interrupt is counted.
 */
const rootFleetWorkspaces = (
  counts: {
    provisions: number;
    releases: number;
    interrupts: number;
  },
  overrides: {
    readonly manager?: Partial<Manager.CloudSandboxManager["Service"]>;
    readonly threads?: Partial<Threads.ThreadManagementService["Service"]>;
    readonly delegated?: () => void;
    readonly transfer?: ProjectTransfer.CloudSandboxProjectTransfer["Service"]["transfer"];
  } = {},
) => {
  const shell = liveThreadShell(scope.thread.threadId);
  const parent = idleThreadProjection(shell);
  const project: Project = {
    id: parent.thread.projectId,
    title: "Source",
    workspaceRoot: "/source/repo",
    scripts: [],
    repositoryIdentity: null,
    defaultModelSelection: null,
    createdAt: "2026-01-01T00:00:00Z",
    updatedAt: "2026-01-01T00:00:00Z",
    deletedAt: null,
  };
  const peerCalls = Layer.mock(PeerMcpClient.PeerMcpClient)({
    call: ({ tool, arguments: args, success }) => {
      if (tool === "t3_thread_interrupt") {
        return Effect.suspend(() => {
          counts.interrupts++;
          return Effect.fail(
            new OrchestratorMcpFailure({ code: "thread_not_found", message: "The child is gone." }),
          );
        });
      }
      return Schema.decodeEffect(Schema.fromJsonString(success))(
        JSON.stringify(
          tool === "t3_project_list"
            ? { projects: [], nextCursor: null }
            : tool === "t3_project_create"
              ? { ...project, id: "child-project", workspaceRoot: String(args.workspaceRoot) }
              : { branch: rootInput.branch },
        ),
      ).pipe(
        Effect.mapError(
          () =>
            new OrchestratorMcpFailure({ code: "invalid_request", message: "fixture response" }),
        ),
      );
    },
  });
  const baseDependencies = Layer.mergeAll(
    Sqlite.layerMemory,
    Layer.mock(Controller.CloudSandboxController)({ available: Effect.succeed(false) }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadRecords: () => Effect.succeed(parent),
      getThreadShell: () => Effect.succeed(shell),
      ...overrides.threads,
    }),
    Layer.mock(Projects.ProjectService)({ getById: () => Effect.succeedSome(project) }),
    Layer.mock(Links.PeerLinks)({
      enabled: Effect.succeed(true),
      get: () => Effect.succeedNone,
      link: (request) =>
        Effect.succeed({
          environmentId: EnvironmentId.make("child"),
          label: "Child",
          urls: [request.url],
          access: "auto" as const,
          linkedAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z"),
          expiresAt: DateTime.makeUnsafe("2026-02-01T00:00:00Z"),
          lastReachedAt: null,
          lastError: null,
        }),
      unlink: () => Effect.succeed(true),
    }),
    Layer.mock(Delegation.RemoteDelegation)({
      delegateWorkspace: () =>
        Effect.sync(() => {
          overrides.delegated?.();
          return {
            taskId: NodeId.make("task"),
            threadId: ThreadId.make("child-thread"),
            projectId: ProjectId.make("child-project"),
          };
        }),
    }),
    Layer.mock(Git.GitWorkflowService)({}),
    Layer.mock(GitDriver.GitVcsDriver)({}),
    Layer.mock(Setup.ProjectSetupScriptRunner)({}),
    Layer.mock(Profiles.CloudSandboxProfiles)({
      list: Effect.succeed([
        {
          profile: {
            profileId: SandboxProfileId.make("boat"),
            name: "Boat",
            driverKind: "boat",
            apiKeySet: true,
            defaultSize: "small",
            autoStopMinutes: 60,
            template: null,
            monthlyBudgetUsd: 25,
            maxConcurrentSandboxes: 3,
          },
          spend: null,
        },
      ]),
    }),
    Layer.mock(Manager.CloudSandboxManager)({
      get: () => Effect.fail(new CloudSandboxManagerError({ reason: "not-found" })),
      provision: () =>
        Effect.sync(() => {
          counts.provisions++;
          return {
            httpBaseUrl: "http://127.0.0.1:40100",
            pairingToken: "fresh-pairing-token",
            connection: {
              sandboxId: "sandbox",
              driverKind: "boat" as const,
              resourceId: ManagedCloudSandboxResourceId.make("root-workspace"),
              controllerEnvironmentId: EnvironmentId.make("controller"),
              environmentId: EnvironmentId.make("child"),
              label: "Child",
              routePrefix: "/api/cloud-sandboxes/managed/root-workspace/proxy",
              bearerToken: "transport-bearer",
            },
          };
        }),
      release: () =>
        Effect.sync(() => {
          counts.releases++;
        }),
      ...overrides.manager,
    }),
    Layer.succeed(ProjectTransfer.CloudSandboxProjectTransfer, {
      transfer:
        overrides.transfer ??
        (() => Effect.succeed("/home/sandbox/.t3/cloud-workspaces/root-workspace/repo")),
    }),
  );
  return Layer.effect(Workspaces.CloudSandboxWorkspaces, Workspaces.make).pipe(
    Layer.provideMerge(
      Forwarding.layer.pipe(Layer.provideMerge(Layer.mergeAll(baseDependencies, peerCalls))),
    ),
  );
};

it.effect("retries delegation after restart without overwriting the prepared checkout", () => {
  const counts = { provisions: 0, releases: 0, interrupts: 0 };
  let transfers = 0;
  let attempts = 0;
  const layer = rootFleetWorkspaces(counts, {
    transfer: () =>
      Effect.sync(() => {
        transfers++;
        return "/home/sandbox/.t3/cloud-workspaces/root-workspace/repo";
      }),
    delegated: () => {
      attempts++;
      if (attempts === 1) throw new Error("Child started but its response was lost.");
    },
  }).pipe(
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-checkout-recovery-" }).pipe(
        Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      ),
    ),
  );
  return Effect.gen(function* () {
    yield* writeControllerPort;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    expect((yield* workspaces.delegateSandboxTask(scope, rootInput).pipe(Effect.exit))._tag).toBe(
      "Failure",
    );
    const restarted = yield* Workspaces.make;
    const result = yield* restarted.delegateSandboxTask(scope, rootInput);
    expect(result.taskId).toBe("task");
    expect(attempts).toBe(2);
    expect(transfers).toBe(1);
  }).pipe(Effect.provide(layer), Effect.scoped);
});

it.effect("remove still deletes a root sandbox whose child refuses the interrupt", () => {
  const counts = { provisions: 0, releases: 0, interrupts: 0 };
  return Effect.gen(function* () {
    yield* writeControllerPort;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    yield* workspaces.delegateSandboxTask(scope, rootInput);
    expect(yield* workspaces.remove(scope, rootInput.workspaceId)).toEqual({
      workspaceId: rootInput.workspaceId,
      deleted: true,
    });
    expect(counts.interrupts).toBe(1);
    expect(counts.releases).toBe(1);
  }).pipe(
    Effect.provide(
      rootFleetWorkspaces(counts).pipe(
        Layer.provideMerge(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-root-remove-interrupt-" }).pipe(
            Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
    ),
    Effect.scoped,
  );
});

it.effect(
  "a retry after its root sandbox was removed fails instead of returning the stored result",
  () => {
    const counts = { provisions: 0, releases: 0, interrupts: 0 };
    return Effect.gen(function* () {
      yield* writeControllerPort;
      const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
      yield* workspaces.delegateSandboxTask(scope, rootInput);
      yield* workspaces.remove(scope, rootInput.workspaceId);
      const retry = yield* workspaces.delegateSandboxTask(scope, rootInput).pipe(Effect.flip);
      expect(retry.message).toContain("has been deleted");
      expect(counts.provisions).toBe(1);
    }).pipe(
      Effect.provide(
        rootFleetWorkspaces(counts).pipe(
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-root-retry-deleted-" }).pipe(
              Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
            ),
          ),
        ),
      ),
      Effect.scoped,
    );
  },
);

it.effect("repoints live children's links to this server's port and skips the rest", () => {
  const repoints: Array<readonly [string, string]> = [];
  const dependencies = Layer.mergeAll(
    Sqlite.layerMemory,
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-workspace-repoint-" }),
    Layer.mock(Controller.CloudSandboxController)({ available: Effect.succeed(false) }),
    Layer.mock(Threads.ThreadManagementService)({}),
    Layer.mock(Projects.ProjectService)({}),
    Layer.mock(Links.PeerLinks)({
      repoint: (environmentId, url) =>
        environmentId === EnvironmentId.make("child-broken")
          ? Effect.fail(new Error("repoint failed") as never)
          : Effect.sync(() => {
              repoints.push([environmentId, url]);
            }),
    }),
    Layer.mock(Forwarding.PeerForwarding)({}),
    Layer.mock(Delegation.RemoteDelegation)({}),
    Layer.mock(Git.GitWorkflowService)({}),
    Layer.mock(GitDriver.GitVcsDriver)({}),
    Layer.mock(Setup.ProjectSetupScriptRunner)({}),
    Layer.mock(Profiles.CloudSandboxProfiles)({}),
    Layer.mock(Manager.CloudSandboxManager)({}),
  );
  return Effect.gen(function* () {
    yield* writeControllerPort;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json) VALUES
      ('owned','owner','{"workspaceId":"owned","sourceThreadId":"owner","requestJson":"{}","phase":"running","childEnvironmentId":"child","result":null}'),
      ('owned-broken','owner','{"workspaceId":"owned-broken","sourceThreadId":"owner","requestJson":"{}","phase":"running","childEnvironmentId":"child-broken","result":null}'),
      ('retired','owner','{"workspaceId":"retired","sourceThreadId":"owner","requestJson":"{}","phase":"deleted","childEnvironmentId":"child-retired","result":null}'),
      ('no-child','owner','{"workspaceId":"no-child","sourceThreadId":"owner","requestJson":"{}","phase":"creating","result":null}')`;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    expect(yield* workspaces.repointLinks).toBe(1);
    expect(repoints).toEqual([
      ["child", "http://127.0.0.1:43123/api/cloud-sandboxes/managed/owned/proxy"],
    ]);
  }).pipe(
    Effect.provide(
      Workspaces.layer.pipe(
        Layer.provideMerge(dependencies),
        Layer.provideMerge(randomLayer),
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
    Effect.scoped,
  );
});

it.effect.each(["local", "nested"] as const)(
  "reports a %s follower's degraded state and clears it on recovery",
  (route) => {
    let unavailable = false;
    let cardIssue: ManagedCloudSandboxIssue | undefined;
    let listed: readonly ManagedCloudSandboxResource[] = [];
    const dispatches: Array<{
      readonly commandId: string;
      readonly issue?: ManagedCloudSandboxIssue;
    }> = [];
    const context = Layer.mergeAll(
      Sqlite.layerMemory,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-sandbox-issues-" }),
      Layer.mock(Controller.CloudSandboxController)({
        available: Effect.succeed(route === "nested"),
        request: () =>
          unavailable || listed[0] === undefined
            ? Effect.fail(
                new WorktreeMcpFailure({
                  code: "operation_failed",
                  message: "Controller unavailable",
                }),
              )
            : Effect.succeed({ _tag: "resource", resource: listed[0] }),
      }),
      Layer.mock(Setup.ProjectSetupScriptRunner)({}),
      Layer.mock(Threads.ThreadManagementService)({
        getThreadRecords: () =>
          Effect.succeed({
            ...idleThreadProjection(liveThreadShell(ThreadId.make("owner"))),
            subagents: [remoteTaskCard(cardIssue)],
          }),
        dispatch: (
          command: Parameters<Threads.ThreadManagementService["Service"]["dispatch"]>[0],
        ) => {
          if (command.type === "delegated_task.remote.issue") {
            cardIssue = command.issue;
            dispatches.push({
              commandId: command.commandId,
              ...(command.issue === undefined ? {} : { issue: command.issue }),
            });
          }
          return Effect.succeed({ sequence: 1, storedEvents: [] });
        },
      }),
      Layer.mock(Projects.ProjectService)({}),
      Layer.mock(Links.PeerLinks)({}),
      Layer.mock(Forwarding.PeerForwarding)({}),
      Layer.mock(Delegation.RemoteDelegation)({}),
      Layer.mock(Manager.CloudSandboxManager)({
        list: Effect.sync(() => (route === "local" ? listed : [])),
      }),
      Layer.mock(Profiles.CloudSandboxProfiles)({ list: Effect.succeed([]) }),
      Layer.mock(Git.GitWorkflowService)({}),
      Layer.mock(GitDriver.GitVcsDriver)({}),
    );
    const issue: ManagedCloudSandboxIssue = {
      cause: "connectivity",
      driverKind: "boat",
      profileId: "boat-main",
      message: "The managed cloud connection could not be established.",
      firstSeenAt: "2026-02-09T10:00:00Z",
    };
    const follower = (withIssue: boolean): ManagedCloudSandboxResource => ({
      resourceId: ManagedCloudSandboxResourceId.make("follower-1"),
      controllerEnvironmentId: EnvironmentId.make("source"),
      driverKind: "boat",
      profileId: SandboxProfileId.make("boat-main"),
      sandboxId: "sbx-1",
      parentResourceId: null,
      owner: {
        _tag: "task",
        hostEnvironmentId: EnvironmentId.make(route === "local" ? "source" : "intermediate"),
        sourceThreadId: ThreadId.make("owner"),
        taskId: "task-1",
      },
      retention: "stop-and-retain",
      phase: "connected",
      environmentId: null,
      label: "follower",
      recordingIds: [],
      ...(withIssue ? { issue } : {}),
    });
    return Effect.gen(function* () {
      const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
      listed = [follower(true)];
      if (route === "nested") {
        const sql = yield* SqlClient.SqlClient;
        const record = JSON.stringify({
          workspaceId: "follower-1",
          sourceThreadId: "owner",
          requestJson: "{}",
          phase: "running",
          result: {
            kind: "sandbox",
            workspaceId: "follower-1",
            resourceId: "follower-1",
            environmentId: "child",
            projectId: "child-project",
            threadId: "child-thread",
            taskId: "task-1",
            workspacePath: "/repo",
            branch: "sandbox/task",
            note: "test",
          },
        });
        yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json) VALUES ('follower-1','owner',${record})`;
      }
      expect(yield* workspaces.reportIssues).toBe(1);
      expect(dispatches).toHaveLength(1);
      expect(dispatches[0]?.issue?.message).toBe(issue.message);
      // An unchanged issue reports nothing more.
      expect(yield* workspaces.reportIssues).toBe(1);
      expect(dispatches).toHaveLength(1);
      if (route === "nested") {
        unavailable = true;
        yield* workspaces.reportIssues;
        expect(dispatches).toHaveLength(1);
        unavailable = false;
      }
      // Recovery clears the card state exactly once.
      listed = [follower(false)];
      expect(yield* workspaces.reportIssues).toBe(0);
      expect(dispatches).toHaveLength(2);
      expect(dispatches[1]?.issue).toBeUndefined();
    }).pipe(
      Effect.provide(
        Workspaces.layer.pipe(
          Layer.provideMerge(context),
          Layer.provideMerge(randomLayer),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
      Effect.scoped,
    );
  },
);

it.effect.each(["local", "nested"] as const)(
  "retries a %s card clear after restart without re-reporting healthy state",
  (route) => {
    let clearFails = true;
    let cardIssue: ManagedCloudSandboxIssue | undefined;
    let listed: readonly ManagedCloudSandboxResource[] = [];
    const dispatches: Array<{
      readonly commandId: string;
      readonly issue?: ManagedCloudSandboxIssue;
    }> = [];
    const context = Layer.mergeAll(
      Sqlite.layerMemory,
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-sandbox-issues-redrive-" }),
      Layer.mock(Controller.CloudSandboxController)({
        available: Effect.succeed(route === "nested"),
        request: () =>
          listed[0] === undefined
            ? Effect.fail(
                new WorktreeMcpFailure({
                  code: "operation_failed",
                  message: "Controller unavailable",
                }),
              )
            : Effect.succeed({ _tag: "resource", resource: listed[0] }),
      }),
      Layer.mock(Setup.ProjectSetupScriptRunner)({}),
      Layer.mock(Threads.ThreadManagementService)({
        getThreadRecords: () =>
          Effect.succeed({
            ...idleThreadProjection(liveThreadShell(ThreadId.make("owner"))),
            subagents: [remoteTaskCard(cardIssue)],
          }),
        dispatch: (
          command: Parameters<Threads.ThreadManagementService["Service"]["dispatch"]>[0],
        ) => {
          if (command.type === "delegated_task.remote.issue") {
            if (clearFails)
              return Effect.fail(
                new OrchestratorDispatchError({
                  commandId: command.commandId,
                  commandType: command.type,
                  cause: "Storage unavailable",
                }),
              );
            cardIssue = command.issue;
            dispatches.push({
              commandId: command.commandId,
              ...(command.issue === undefined ? {} : { issue: command.issue }),
            });
          }
          return Effect.succeed({ sequence: 1, storedEvents: [] });
        },
      }),
      Layer.mock(Projects.ProjectService)({}),
      Layer.mock(Links.PeerLinks)({}),
      Layer.mock(Forwarding.PeerForwarding)({}),
      Layer.mock(Delegation.RemoteDelegation)({}),
      Layer.mock(Manager.CloudSandboxManager)({
        list: Effect.sync(() => (route === "local" ? listed : [])),
      }),
      Layer.mock(Profiles.CloudSandboxProfiles)({ list: Effect.succeed([]) }),
      Layer.mock(Git.GitWorkflowService)({}),
      Layer.mock(GitDriver.GitVcsDriver)({}),
    );
    const issue: ManagedCloudSandboxIssue = {
      cause: "connectivity",
      driverKind: "boat",
      profileId: "boat-main",
      message: "The managed cloud connection could not be established.",
      firstSeenAt: "2026-02-09T10:00:00Z",
    };
    const follower = (): ManagedCloudSandboxResource => ({
      resourceId: ManagedCloudSandboxResourceId.make("follower-1"),
      controllerEnvironmentId: EnvironmentId.make("source"),
      driverKind: "boat",
      profileId: SandboxProfileId.make("boat-main"),
      sandboxId: "sbx-1",
      parentResourceId: null,
      owner: {
        _tag: "task",
        hostEnvironmentId: EnvironmentId.make(route === "local" ? "source" : "intermediate"),
        sourceThreadId: ThreadId.make("owner"),
        taskId: "task-1",
      },
      retention: "stop-and-retain",
      phase: "connected",
      environmentId: null,
      label: "follower",
      recordingIds: [],
    });
    return Effect.gen(function* () {
      const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
      // The follower recovered while the server was down, so this process has
      // told nothing, yet the card still shows the recovered span.
      listed = [follower()];
      cardIssue = issue;
      if (route === "nested") {
        const sql = yield* SqlClient.SqlClient;
        const record = JSON.stringify({
          workspaceId: "follower-1",
          sourceThreadId: "owner",
          requestJson: "{}",
          phase: "running",
          result: {
            kind: "sandbox",
            workspaceId: "follower-1",
            resourceId: "follower-1",
            environmentId: "child",
            projectId: "child-project",
            threadId: "child-thread",
            taskId: "task-1",
            workspacePath: "/repo",
            branch: "sandbox/task",
            note: "test",
          },
        });
        yield* sql`INSERT INTO cloud_sandbox_workspaces (workspace_id,source_thread_id,payload_json) VALUES ('follower-1','owner',${record})`;
      }
      expect(yield* workspaces.reportIssues).toBe(0);
      expect(cardIssue).toEqual(issue);
      expect(dispatches).toHaveLength(0);
      clearFails = false;
      expect(yield* workspaces.reportIssues).toBe(0);
      expect(dispatches).toHaveLength(1);
      expect(dispatches[0]?.issue).toBeUndefined();
      // The reconciled owner reports nothing further.
      expect(yield* workspaces.reportIssues).toBe(0);
      expect(dispatches).toHaveLength(1);
    }).pipe(
      Effect.provide(
        Workspaces.layer.pipe(
          Layer.provideMerge(context),
          Layer.provideMerge(randomLayer),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
      Effect.scoped,
    );
  },
);

const ownershipFixture = () => {
  const counts = { provisions: 0, releases: 0, interrupts: 0, delegations: 0, bindings: 0 };
  const state: {
    available: boolean;
    failClear: boolean;
    beforeBind: Effect.Effect<void>;
    resource: ManagedCloudSandboxResource;
    issue: ManagedCloudSandboxIssue | undefined;
  } = {
    available: false,
    failClear: false,
    beforeBind: Effect.void,
    resource: {
      resourceId: ManagedCloudSandboxResourceId.make("root-workspace"),
      controllerEnvironmentId: scope.environmentId,
      driverKind: "boat",
      profileId: SandboxProfileId.make("boat"),
      sandboxId: "sandbox",
      parentResourceId: null,
      owner: { _tag: "user" },
      retention: "manual",
      phase: "connected",
      environmentId: EnvironmentId.make("child"),
      label: "Child",
      recordingIds: [],
    },
    issue: undefined,
  };
  const layer = rootFleetWorkspaces(counts, {
    manager: {
      list: Effect.sync(() => [state.resource]),
      get: () => Effect.sync(() => state.resource),
      bindOwner: (_id, owner, retention) =>
        Effect.gen(function* () {
          counts.bindings++;
          if (!state.available) return yield* new CloudSandboxManagerError({ reason: "transport" });
          yield* state.beforeBind;
          const current = state.resource.owner;
          if (
            current._tag !== "user" &&
            (current._tag !== "task" || owner._tag !== "task" || current.taskId !== owner.taskId)
          )
            return yield* new CloudSandboxManagerError({ reason: "ownership" });
          state.resource = {
            ...state.resource,
            owner: { ...owner, hostEnvironmentId: scope.environmentId },
            retention: retention ?? "stop-and-retain",
          };
          return state.resource;
        }),
    },
    threads: {
      getThreadRecords: () =>
        Effect.succeed({
          ...idleThreadProjection(liveThreadShell(scope.thread.threadId)),
          subagents: [{ ...remoteTaskCard(state.issue), id: NodeId.make("task") }],
        }),
      dispatch: (command) =>
        Effect.gen(function* () {
          if (command.type === "delegated_task.remote.issue") {
            if (state.failClear && command.issue === undefined)
              return yield* Effect.die("Task issue projection is unavailable.");
            state.issue = command.issue;
          }
          return { sequence: 1, storedEvents: [] };
        }),
    },
    delegated: () => {
      counts.delegations++;
    },
  }).pipe(
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-ownership-recovery-" }).pipe(
        Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      ),
    ),
  );
  return { counts, state, layer };
};

it.effect.each(["request", "restart"] as const)(
  "recovers a pending ownership bind on %s without creating another child",
  (recovery) => {
    const fixture = ownershipFixture();
    return Effect.gen(function* () {
      yield* writeControllerPort;
      const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
      const input = { ...rootInput, deleteOnCompletion: true };
      const first = yield* workspaces.delegateSandboxTask(scope, input);
      const sql = yield* SqlClient.SqlClient;
      const pending = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM cloud_sandbox_workspaces`;
      expect(JSON.parse(pending[0]!.payload_json)).toMatchObject({
        phase: "running",
        result: { taskId: first.taskId },
        binding: { _tag: "pending", retention: "delete-on-completion" },
      });
      expect(fixture.state.issue?.cause).toBe("bind");
      expect(fixture.counts.bindings).toBe(1);
      fixture.state.available = true;
      if (recovery === "request") {
        expect(yield* workspaces.delegateSandboxTask(scope, input)).toEqual(first);
      } else {
        const restarted = yield* Workspaces.make;
        expect(yield* restarted.recoverOwnership).toBe(1);
      }
      expect(fixture.state.resource.owner).toMatchObject({ _tag: "task", taskId: first.taskId });
      expect(fixture.state.resource.retention).toBe("delete-on-completion");
      expect(fixture.state.issue).toBeUndefined();
      expect(fixture.counts.provisions).toBe(1);
      expect(fixture.counts.delegations).toBe(1);
      const bound = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM cloud_sandbox_workspaces`;
      expect(JSON.parse(bound[0]!.payload_json).binding._tag).toBe("bound");
    }).pipe(Effect.provide(fixture.layer), Effect.scoped);
  },
);

it.effect.each(["user", "bound", "foreign", "deleted"] as const)(
  "repairs a legacy %s resource conservatively",
  (kind) => {
    const fixture = ownershipFixture();
    return Effect.gen(function* () {
      yield* writeControllerPort;
      const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
      yield* workspaces.delegateSandboxTask(scope, rootInput);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE cloud_sandbox_workspaces SET payload_json = json_remove(payload_json, '$.binding')`;
      fixture.state.available = true;
      if (kind === "bound" || kind === "foreign")
        fixture.state.resource = {
          ...fixture.state.resource,
          owner: {
            _tag: "task",
            hostEnvironmentId: scope.environmentId,
            sourceThreadId: scope.thread.threadId,
            taskId: kind === "bound" ? "task" : "another-task",
          },
          retention: "delete-on-completion",
        };
      if (kind === "deleted")
        fixture.state.resource = { ...fixture.state.resource, phase: "deleted" };
      const before = fixture.counts.bindings;
      yield* workspaces.recoverOwnership;
      if (kind === "user") {
        expect(fixture.state.resource.owner).toMatchObject({ _tag: "task", taskId: "task" });
        expect(fixture.state.resource.retention).toBe("stop-and-retain");
      } else if (kind === "bound") {
        expect(fixture.state.resource.retention).toBe("delete-on-completion");
      } else if (kind === "foreign") {
        expect(fixture.state.resource.owner).toMatchObject({ taskId: "another-task" });
        expect(fixture.state.issue?.cause).toBe("bind");
      } else {
        expect(fixture.counts.bindings).toBe(before);
      }
      expect(fixture.counts.provisions).toBe(1);
      expect(fixture.counts.delegations).toBe(1);
    }).pipe(Effect.provide(fixture.layer), Effect.scoped);
  },
);

it.effect("retries a durable issue clear after binding without binding again", () => {
  const fixture = ownershipFixture();
  return Effect.gen(function* () {
    yield* writeControllerPort;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    yield* workspaces.delegateSandboxTask(scope, rootInput);
    fixture.state.available = true;
    fixture.state.failClear = true;
    expect(yield* workspaces.recoverOwnership).toBe(0);
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM cloud_sandbox_workspaces`;
    expect(JSON.parse(rows[0]!.payload_json).binding._tag).toBe("clearing");
    expect(fixture.state.issue?.cause).toBe("bind");
    const bindings = fixture.counts.bindings;
    fixture.state.failClear = false;
    const restarted = yield* Workspaces.make;
    expect(yield* restarted.recoverOwnership).toBe(1);
    expect(fixture.state.issue).toBeUndefined();
    expect(fixture.counts.bindings).toBe(bindings);
  }).pipe(Effect.provide(fixture.layer), Effect.scoped);
});

it.effect("does not recover ownership after workspace deletion", () => {
  const fixture = ownershipFixture();
  return Effect.gen(function* () {
    yield* writeControllerPort;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    yield* workspaces.delegateSandboxTask(scope, rootInput);
    const bindings = fixture.counts.bindings;
    yield* workspaces.remove(scope, rootInput.workspaceId);
    fixture.state.available = true;
    expect(yield* workspaces.recoverOwnership).toBe(0);
    const restarted = yield* Workspaces.make;
    expect(yield* restarted.recoverOwnership).toBe(0);
    expect(fixture.counts.bindings).toBe(bindings);
    expect(fixture.counts.provisions).toBe(1);
    expect(fixture.counts.releases).toBe(1);
  }).pipe(Effect.provide(fixture.layer), Effect.scoped);
});

it.effect("serializes ownership recovery with workspace deletion", () => {
  const fixture = ownershipFixture();
  return Effect.gen(function* () {
    yield* writeControllerPort;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    yield* workspaces.delegateSandboxTask(scope, rootInput);
    const entered = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    fixture.state.available = true;
    fixture.state.beforeBind = Deferred.succeed(entered, undefined).pipe(
      Effect.andThen(Deferred.await(resume)),
    );
    const recovering = yield* workspaces.recoverOwnership.pipe(
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(entered);
    const deleting = yield* workspaces
      .remove(scope, rootInput.workspaceId)
      .pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.succeed(resume, undefined);
    yield* Fiber.join(recovering);
    yield* Fiber.join(deleting);
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM cloud_sandbox_workspaces`;
    expect(JSON.parse(rows[0]!.payload_json).phase).toBe("deleted");
    expect(yield* workspaces.recoverOwnership).toBe(0);
    expect(fixture.counts.releases).toBe(1);
  }).pipe(Effect.provide(fixture.layer), Effect.scoped);
});

it.effect("keeps pending ownership visible while resource diagnostics report healthy", () => {
  const fixture = ownershipFixture();
  return Effect.gen(function* () {
    yield* writeControllerPort;
    const workspaces = yield* Workspaces.CloudSandboxWorkspaces;
    yield* workspaces.delegateSandboxTask(scope, rootInput);
    fixture.state.resource = {
      ...fixture.state.resource,
      owner: {
        _tag: "task",
        hostEnvironmentId: scope.environmentId,
        sourceThreadId: scope.thread.threadId,
        taskId: "task",
      },
    };
    yield* workspaces.reportIssues;
    expect(fixture.state.issue?.cause).toBe("bind");
    fixture.state.available = true;
    yield* workspaces.recoverOwnership;
    yield* workspaces.reportIssues;
    expect(fixture.state.issue).toBeUndefined();
  }).pipe(Effect.provide(fixture.layer), Effect.scoped);
});
