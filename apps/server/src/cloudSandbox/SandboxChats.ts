/**
 * Chats on a cloud sandbox: the host thread runs on the `sandboxes` bridge instance, whose
 * `t3 sandbox-bridge` forwards each turn to the thread with the same id on the follower.
 */
import {
  CloudSandboxChatError,
  type CloudSandboxChatResolveInput,
  type CloudSandboxChatStartInput,
  type CloudSandboxChatState,
  type CloudSandboxSettleChoice,
  CommandId,
  ManagedCloudSandboxResourceId,
  type ModelSelection,
  ProjectId,
  type ProviderInstanceConfig,
  ProviderDriverKind,
  ProviderInstanceId,
  SANDBOX_BRIDGE_INSTANCE_ID,
  SANDBOX_BRIDGE_MODEL,
  ThreadId,
} from "@t3tools/contracts";
import { createSandboxProject } from "@t3tools/sandbox-bridge";
import {
  bridgeLinkSecretName,
  SANDBOX_BRIDGE_ATTACHMENTS_DIR_ENV,
  SANDBOX_BRIDGE_SECRETS_DIR_ENV,
  SANDBOX_BRIDGE_TRACE_FILE_ENV,
  SandboxBridgeLinkJson,
} from "@t3tools/sandbox-bridge/link";
import { resolveSelfInvocation, selfInvocationArgs } from "@t3tools/shared/nodeRuntime";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import { FetchHttpClient } from "effect/http";

import { persistChatAttachments } from "../assets/PersistChatAttachments.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ThreadMessageIntake from "../orchestration-v2/ThreadMessageIntake.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Projects from "../project/ProjectService.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectTransfer from "../workspace/CloudSandboxProjectTransfer.ts";
import * as Manager from "./CloudSandboxManager.ts";
import * as Profiles from "./CloudSandboxProfiles.ts";

const instanceId = ProviderInstanceId.make(SANDBOX_BRIDGE_INSTANCE_ID);
const bridgeModelSelection = { instanceId, model: SANDBOX_BRIDGE_MODEL } as ModelSelection;
const encodeLink = Schema.encodeEffect(SandboxBridgeLinkJson);
const SettleState = Schema.Literals(["none", "pending", "kept", "deleted"]);
const Target = Schema.Literals(["new", "project", "current"]);
const ChatRow = Schema.Struct({
  thread_id: ThreadId,
  resource_id: ManagedCloudSandboxResourceId,
  target: Target,
  sandbox_project_id: Schema.NullOr(Schema.String),
  settle: SettleState,
});
type ChatRow = typeof ChatRow.Type;
const decodeRow = Schema.decodeUnknownEffect(ChatRow);

// Runs about to need their sandbox, so a resume starts before the bridge waits on it.
const STARTING_RUN = new Set(["queued", "starting", "running"]);
const BRIDGE_ENVIRONMENT_NAMES = new Set([
  SANDBOX_BRIDGE_SECRETS_DIR_ENV,
  SANDBOX_BRIDGE_ATTACHMENTS_DIR_ENV,
  SANDBOX_BRIDGE_TRACE_FILE_ENV,
  "ELECTRON_RUN_AS_NODE",
]);

const chatError = (message: string) => (cause?: unknown) =>
  new CloudSandboxChatError({
    message:
      cause !== null && typeof cause === "object" && "message" in cause
        ? `${message} ${String(cause.message)}`
        : message,
  });

/** Registers the copied checkout as a project on the sandbox; tests replace the network call. */
export class SandboxProjectRegistrar extends Context.Reference<typeof createSandboxProject>(
  "t3/cloudSandbox/SandboxChats/SandboxProjectRegistrar",
  { defaultValue: () => createSandboxProject },
) {}

export class SandboxChats extends Context.Service<
  SandboxChats,
  {
    /**
     * Creates the chat's host thread with its first message held `preparing`, records which
     * follower it runs on, and readies that follower in the background; the message goes out
     * through the bridge once the link is written, or fails with a retryable error.
     */
    readonly startChat: (
      input: CloudSandboxChatStartInput,
    ) => Effect.Effect<{ readonly threadId: ThreadId }, CloudSandboxChatError>;
    readonly state: (
      threadId: ThreadId,
    ) => Effect.Effect<CloudSandboxChatState | null, CloudSandboxChatError>;
    /** Applies the user's keep-or-delete answer to a settled chat's disposable sandbox. */
    readonly resolveSettle: (
      input: CloudSandboxChatResolveInput,
    ) => Effect.Effect<CloudSandboxChatState, CloudSandboxChatError>;
    /** Applies a settle event for a sandbox chat; exposed for the event follower and tests. */
    readonly onSettled: (threadId: ThreadId) => Effect.Effect<void, CloudSandboxChatError>;
    /** Opening a chat is an explicit action, so its stopped sandbox resumes now. */
    readonly open: (threadId: ThreadId) => Effect.Effect<void, CloudSandboxChatError>;
    /** Seeds the bridge instance and follows run and settle events; call once per server start. */
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/cloudSandbox/SandboxChats") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const config = yield* ServerConfig.ServerConfig;
  const path = yield* Path.Path;
  const digest = yield* Digest.Digest;
  const random = yield* SecureRandom.SecureRandom;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const settings = yield* ServerSettings.ServerSettingsService;
  const threads = yield* Threads.ThreadManagementService;
  const projects = yield* Projects.ProjectService;
  const manager = yield* Manager.CloudSandboxManager;
  const profiles = yield* Profiles.CloudSandboxProfiles;
  const transfer = yield* ProjectTransfer.CloudSandboxProjectTransfer;
  const registerProject = yield* SandboxProjectRegistrar;
  const attachmentServices = yield* Effect.context<
    FileSystem.FileSystem | Path.Path | ServerConfig.ServerConfig
  >();
  const fileSystem = yield* FileSystem.FileSystem;
  const documentText = yield* DocumentText.DocumentText;
  // First sends go through the intake claim layer like the composer's other sends, so a web
  // client's pending uploads are copied to their final ids before the provider reads them.
  const intakeServices = Context.make(Threads.ThreadManagementService, threads).pipe(
    Context.add(ServerConfig.ServerConfig, config),
    Context.add(FileSystem.FileSystem, fileSystem),
    Context.add(DocumentText.DocumentText, documentText),
  );

  const hash = (value: string) =>
    digest
      .digest(Digest.sha256, new TextEncoder().encode(value))
      .pipe(Effect.map((bytes) => Hex.encode(bytes).slice(0, 40)));
  const commandId = (kind: string) =>
    random.uuidV4().pipe(Effect.map((id) => CommandId.make(`cloud-sandbox-chat:${kind}:${id}`)));

  const readRow = (threadId: ThreadId) =>
    sql`SELECT thread_id, resource_id, target, sandbox_project_id, settle
        FROM cloud_sandbox_chats WHERE thread_id = ${threadId}`.pipe(
      Effect.flatMap((rows) =>
        rows[0] === undefined ? Effect.succeedNone : decodeRow(rows[0]).pipe(Effect.asSome),
      ),
      Effect.mapError(chatError("Could not read this chat's sandbox.")),
    );
  const setSettle = (threadId: ThreadId, settle: typeof SettleState.Type) =>
    sql`UPDATE cloud_sandbox_chats SET settle = ${settle} WHERE thread_id = ${threadId}`.pipe(
      Effect.asVoid,
      Effect.mapError(chatError("Could not save this chat's sandbox answer.")),
    );
  const projectDefault = (projectId: ProjectId) =>
    sql<{
      readonly on_settle: string | null;
    }>`SELECT on_settle FROM cloud_sandbox_projects WHERE project_id = ${projectId}`.pipe(
      Effect.map((rows): CloudSandboxSettleChoice | null => {
        const value = rows[0]?.on_settle;
        return value === "keep" || value === "delete" ? value : null;
      }),
      Effect.mapError(chatError("Could not read the project's sandbox default.")),
    );

  const stateOf = Effect.fn("SandboxChats.stateOf")(function* (row: ChatRow) {
    const { thread } = yield* threads
      .getThreadRecords(row.thread_id, [])
      .pipe(Effect.mapError(chatError("Could not read the chat.")));
    return {
      threadId: thread.id,
      resourceId: row.resource_id,
      target: row.target,
      settle: row.settle,
      projectDefault: yield* projectDefault(thread.projectId),
    } satisfies CloudSandboxChatState;
  });

  /** Upserts the bridge instance, off machine-wide; projects that host a sandbox chat show it. */
  const seedInstance = Effect.gen(function* () {
    const traceFile = path.join(config.logsDir, "sandbox-bridge.trace.ndjson");
    const self = yield* resolveSelfInvocation().pipe(Effect.provideService(Path.Path, path));
    const commandArgs = selfInvocationArgs(self, ["sandbox-bridge"]);
    const existing = (yield* settings.getSettings).providerInstances[instanceId];
    const existingConfig = existing?.config as Record<string, unknown> | undefined;
    const environmentValue = (name: string) =>
      existing?.environment?.find((variable) => variable.name === name)?.value;
    const configuredArgs = existingConfig?.commandArgs;
    if (
      existing?.driver === "acpRegistry" &&
      existingConfig?.source === "local" &&
      existingConfig.commandPath === self.command &&
      Array.isArray(configuredArgs) &&
      configuredArgs.length === commandArgs.length &&
      configuredArgs.every((value, index) => value === commandArgs[index]) &&
      environmentValue(SANDBOX_BRIDGE_SECRETS_DIR_ENV) === config.secretsDir &&
      environmentValue(SANDBOX_BRIDGE_ATTACHMENTS_DIR_ENV) === config.attachmentsDir &&
      environmentValue(SANDBOX_BRIDGE_TRACE_FILE_ENV) === traceFile
    )
      return;
    const instance: ProviderInstanceConfig = {
      displayName: "Sandboxes",
      enabled: false,
      ...existing,
      driver: ProviderDriverKind.make("acpRegistry"),
      environment: [
        ...(existing?.environment ?? []).filter(
          (variable) => !BRIDGE_ENVIRONMENT_NAMES.has(variable.name),
        ),
        // Never a credential: the bridge reads each thread's link from the secret store.
        { name: SANDBOX_BRIDGE_SECRETS_DIR_ENV, value: config.secretsDir, sensitive: false },
        {
          name: SANDBOX_BRIDGE_ATTACHMENTS_DIR_ENV,
          value: config.attachmentsDir,
          sensitive: false,
        },
        { name: SANDBOX_BRIDGE_TRACE_FILE_ENV, value: traceFile, sensitive: false },
        // Desktop runs this server on Electron's binary, which acts as Node only with this set.
        { name: "ELECTRON_RUN_AS_NODE", value: "1", sensitive: false },
      ],
      config: { ...existingConfig, source: "local", commandPath: self.command, commandArgs },
    };
    yield* settings.updateProviderInstance({ operation: "upsert", instanceId, instance });
  });

  const showInProject = Effect.fn("SandboxChats.showInProject")(function* (projectId: ProjectId) {
    const current = (yield* settings.getSettings).projectSettingsOverrides[projectId] ?? {};
    if (current.providerInstances?.[instanceId]?.enabled === true) return;
    yield* settings.updateSettings({
      projectSettingsOverrides: {
        [projectId]: {
          ...current,
          providerInstances: { ...current.providerInstances, [instanceId]: { enabled: true } },
        },
      },
    });
  });

  /** The follower a target names, provisioned with this chat as owner when it is new. */
  const acquire = Effect.fn("SandboxChats.acquire")(function* (
    row: ChatRow,
    input: { readonly projectId: ProjectId; readonly title: string },
  ) {
    const existing = yield* manager.get(row.resource_id).pipe(Effect.option);
    if (Option.isSome(existing) && existing.value.phase !== "failed")
      return yield* manager.connect(row.resource_id, { resume: true });
    if (row.target === "current")
      return yield* new CloudSandboxChatError({ message: "That sandbox no longer exists." });
    const listed = yield* profiles.list.pipe(
      Effect.mapError(chatError("Could not read sandbox profiles.")),
    );
    const profile = listed[0]?.profile;
    if (profile === undefined)
      return yield* new CloudSandboxChatError({
        message: "No sandbox profile is configured. Add one in Settings → Environments.",
      });
    return yield* manager.provision({
      operationId: row.resource_id,
      profile: profile.profileId,
      sandbox: {
        requestId: row.resource_id,
        ...(profile.template === null ? {} : { template: profile.template }),
        name: input.title.slice(0, 60),
        size: profile.defaultSize ?? "default",
        autoStopMinutes: profile.autoStopMinutes,
      },
      // The chat owns its follower from the first record write, before the provider answers.
      ownership: {
        sourceThreadId: row.thread_id,
        retention: row.target === "new" ? "delete-on-completion" : "stop-and-retain",
      },
    });
  });

  const writeLink = Effect.fn("SandboxChats.writeLink")(function* (
    threadId: ThreadId,
    route: Manager.ManagedCloudSandboxRoute,
    sandboxProjectId: string,
  ) {
    const json = yield* encodeLink({
      threadId,
      instanceId: route.connection.resourceId,
      sandboxId: route.connection.sandboxId,
      label: route.connection.label,
      httpBaseUrl: route.httpBaseUrl,
      sandboxProjectId: ProjectId.make(sandboxProjectId),
      token: route.connection.bearerToken,
    });
    yield* secrets.set(bridgeLinkSecretName(threadId), new TextEncoder().encode(json));
  });

  /**
   * Readies one chat's follower and writes its link. A follower that already holds this
   * project keeps it: copying the checkout again could overwrite work started there.
   */
  const prepare = Effect.fn("SandboxChats.prepare")(function* (threadId: ThreadId) {
    const row = yield* readRow(threadId).pipe(
      Effect.flatMap((found) =>
        Effect.fromOption(found).pipe(
          Effect.mapError(() => chatError("This chat has no sandbox.")()),
        ),
      ),
    );
    const { thread } = yield* threads
      .getThreadRecords(threadId, [])
      .pipe(Effect.mapError(chatError("Could not read the chat.")));
    const route = yield* acquire(row, { projectId: thread.projectId, title: thread.title }).pipe(
      Effect.mapError(chatError("The sandbox could not start.")),
    );
    let sandboxProjectId = row.sandbox_project_id;
    if (sandboxProjectId === null) {
      const shared = yield* sql<{ readonly sandbox_project_id: string }>`
        SELECT sandbox_project_id FROM cloud_sandbox_chats
        WHERE resource_id = ${row.resource_id} AND sandbox_project_id IS NOT NULL LIMIT 1`.pipe(
        Effect.mapError(chatError("Could not read this chat's sandbox.")),
      );
      sandboxProjectId = shared[0]?.sandbox_project_id ?? null;
    }
    if (sandboxProjectId === null) {
      const project = yield* projects
        .getById(thread.projectId)
        .pipe(Effect.mapError(chatError("Could not read the project.")));
      if (Option.isNone(project))
        return yield* new CloudSandboxChatError({ message: "The project no longer exists." });
      const workspaceRoot = yield* transfer
        .transfer({ resourceId: row.resource_id, cwd: project.value.workspaceRoot })
        .pipe(Effect.mapError(chatError("Could not copy the project to the sandbox.")));
      const created = yield* registerProject(
        {
          instanceId: route.connection.resourceId,
          sandboxId: route.connection.sandboxId,
          label: route.connection.label,
          httpBaseUrl: route.httpBaseUrl,
          token: route.connection.bearerToken,
        },
        {
          type: "project.create",
          commandId: yield* commandId("project"),
          projectId: ProjectId.make(
            `sandbox-${yield* hash(`${row.resource_id}\u0000${thread.projectId}`)}`,
          ),
          title: project.value.title,
          workspaceRoot,
        },
      ).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.mapError(chatError("Could not register the project in the sandbox.")),
      );
      sandboxProjectId = created.id;
      yield* sql`UPDATE cloud_sandbox_chats SET sandbox_project_id = ${sandboxProjectId}
        WHERE thread_id = ${threadId}`.pipe(
        Effect.mapError(chatError("Could not save this chat's sandbox project.")),
      );
    }
    yield* writeLink(threadId, route, sandboxProjectId).pipe(
      Effect.mapError(chatError("Could not save this chat's sandbox link.")),
    );
  });

  const preparingRuns = (threadId: ThreadId) =>
    threads.getThreadRecords(threadId, ["runs"]).pipe(
      Effect.map(({ runs }) => runs.filter((run) => run.status === "preparing")),
      Effect.orElseSucceed(() => []),
    );

  /** Readies the follower, then lets the held first message go, or fails it with the reason. */
  const launch = Effect.fn("SandboxChats.launch")(function* (threadId: ThreadId) {
    const prepared = yield* prepare(threadId).pipe(Effect.result);
    for (const run of yield* preparingRuns(threadId)) {
      yield* threads
        .dispatch(
          prepared._tag === "Success"
            ? {
                type: "prepared-run.release",
                commandId: yield* commandId("release"),
                threadId,
                runId: run.id,
              }
            : {
                type: "prepared-run.fail",
                commandId: yield* commandId("fail"),
                threadId,
                runId: run.id,
                failure: {
                  class: "provider_error",
                  message: prepared.failure.message,
                  code: null,
                  retryable: true,
                },
              },
        )
        .pipe(Effect.ignore);
    }
  });

  const storedAttachments = Effect.fn("SandboxChats.storedAttachments")(function* (
    input: CloudSandboxChatStartInput,
  ) {
    const attachments = input.message.attachments ?? [];
    const uploads = attachments.filter((attachment) => "dataUrl" in attachment);
    const persisted = yield* persistChatAttachments({
      threadId: input.threadId,
      messageId: input.message.messageId,
      attachments: uploads,
    }).pipe(
      Effect.provide(attachmentServices),
      Effect.mapError((error) => new CloudSandboxChatError({ message: error.message })),
    );
    let next = 0;
    return attachments.map((attachment) =>
      "dataUrl" in attachment ? persisted[next++]! : attachment,
    );
  });

  const startChat = Effect.fn("SandboxChats.startChat")(function* (
    input: CloudSandboxChatStartInput,
  ) {
    const existing = yield* readRow(input.threadId);
    if (Option.isSome(existing)) return { threadId: input.threadId };
    const listed = yield* profiles.list.pipe(
      Effect.mapError(chatError("Could not read sandbox profiles.")),
    );
    const profileId = input.profile ?? listed[0]?.profile.profileId;
    if (profileId === undefined)
      return yield* new CloudSandboxChatError({
        message: "No sandbox profile is configured. Add one in Settings → Environments.",
      });
    const resourceId = yield* (() => {
      switch (input.target._tag) {
        case "current":
          return Effect.succeed(input.target.resourceId);
        case "new":
          return hash(input.threadId).pipe(Effect.map((id) => `chat-${id}`));
        case "project":
          // One kept follower per project and profile: the id is derived, so a second chat finds
          // the same record instead of provisioning another. A deleted one starts a new generation.
          return Effect.gen(function* () {
            const base = `project-${yield* hash(`${input.projectId}\u0000${profileId}`)}`;
            for (let generation = 0; ; generation++) {
              const id = generation === 0 ? base : `${base}-${generation}`;
              const found = yield* manager
                .get(ManagedCloudSandboxResourceId.make(id))
                .pipe(Effect.option);
              if (Option.isNone(found) || found.value.phase !== "deleted") return id;
            }
          });
      }
    })().pipe(Effect.map(ManagedCloudSandboxResourceId.make));
    yield* sql`INSERT INTO cloud_sandbox_chats (thread_id, resource_id, target)
      VALUES (${input.threadId}, ${resourceId}, ${input.target._tag})
      ON CONFLICT(thread_id) DO NOTHING`.pipe(
      Effect.mapError(chatError("Could not record this chat's sandbox.")),
    );
    yield* showInProject(input.projectId).pipe(
      Effect.mapError(chatError("Could not show the Sandboxes provider in this project.")),
    );
    yield* threads
      .dispatch({
        type: "thread.create",
        commandId: CommandId.make(`cloud-sandbox-chat:thread:${input.message.messageId}`),
        threadId: input.threadId,
        projectId: input.projectId,
        title: input.message.title,
        modelSelection: bridgeModelSelection,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      })
      .pipe(Effect.mapError(chatError("Could not create the chat.")));
    yield* ThreadMessageIntake.dispatchCommand({
      type: "message.dispatch",
      commandId: CommandId.make(`cloud-sandbox-chat:message:${input.message.messageId}`),
      threadId: input.threadId,
      messageId: input.message.messageId,
      text: input.message.text,
      attachments: yield* storedAttachments(input),
      titleSeed: input.message.title,
      modelSelection: bridgeModelSelection,
      dispatchMode: { type: "defer_start" },
      createdBy: "user",
      creationSource: "web",
    }).pipe(
      Effect.provide(intakeServices),
      Effect.mapError(chatError("Could not send the first message.")),
    );
    yield* launch(input.threadId).pipe(
      Effect.catchCause((cause) => Effect.logWarning("Sandbox chat launch failed", { cause })),
      Effect.forkDetach,
    );
    return { threadId: input.threadId };
  });

  /** A turn on a stopped follower resumes it now, and the bridge gets the new address. */
  const resumeFor = Effect.fn("SandboxChats.resumeFor")(function* (threadId: ThreadId) {
    const row = yield* readRow(threadId);
    if (Option.isNone(row) || row.value.sandbox_project_id === null) return;
    const route = yield* manager
      .connect(row.value.resource_id, { resume: true })
      .pipe(Effect.mapError(chatError("The sandbox could not resume.")));
    yield* writeLink(threadId, route, row.value.sandbox_project_id).pipe(
      Effect.mapError(chatError("Could not save this chat's sandbox link.")),
    );
  });

  const apply = Effect.fn("SandboxChats.apply")(function* (
    row: ChatRow,
    choice: CloudSandboxSettleChoice,
  ) {
    const threadId = row.thread_id;
    const resource = yield* manager
      .get(row.resource_id)
      .pipe(Effect.mapError(chatError("Could not read this chat's sandbox.")));
    if (choice === "delete") {
      yield* manager
        .release(row.resource_id)
        .pipe(Effect.mapError(chatError("The sandbox could not be deleted.")));
      yield* secrets.remove(bridgeLinkSecretName(threadId)).pipe(Effect.ignore);
      yield* setSettle(threadId, "deleted");
      return;
    }
    yield* manager
      .bindOwner(
        row.resource_id,
        { _tag: "thread", hostEnvironmentId: resource.controllerEnvironmentId, threadId },
        "stop-and-retain",
      )
      .pipe(Effect.mapError(chatError("The sandbox could not be kept.")));
    yield* stop(row);
    yield* setSettle(threadId, "kept");
  });

  const stop = (row: ChatRow) =>
    manager.execute(row.resource_id, { _tag: "stop" }).pipe(
      Effect.catchIf(
        (error) => error.reason === "transport" || error.reason === "not-found",
        () => Effect.void,
      ),
      Effect.asVoid,
      Effect.mapError(chatError("The sandbox could not be stopped.")),
    );

  /**
   * A settled chat on its own disposable follower: the project's remembered answer applies at
   * once; without one the follower stops and the chat asks once. Kept, project, and current
   * followers are never asked about.
   */
  const onSettled = Effect.fn("SandboxChats.onSettled")(function* (threadId: ThreadId) {
    const row = yield* readRow(threadId);
    if (Option.isNone(row) || row.value.target !== "new" || row.value.settle !== "none") return;
    const { thread } = yield* threads
      .getThreadRecords(threadId, [])
      .pipe(Effect.mapError(chatError("Could not read the chat.")));
    const remembered = yield* projectDefault(thread.projectId);
    if (remembered !== null) return yield* apply(row.value, remembered);
    // Pending first, so a client opening the settled chat asks while the follower stops.
    yield* setSettle(threadId, "pending");
    yield* stop(row.value);
  });

  const resolveSettle = Effect.fn("SandboxChats.resolveSettle")(function* (
    input: CloudSandboxChatResolveInput,
  ) {
    const row = yield* readRow(input.threadId);
    if (Option.isNone(row))
      return yield* new CloudSandboxChatError({ message: "This chat has no sandbox." });
    if (row.value.settle === "pending" || row.value.settle === "none")
      yield* apply(row.value, input.choice);
    if (input.remember) {
      const { thread } = yield* threads
        .getThreadRecords(input.threadId, [])
        .pipe(Effect.mapError(chatError("Could not read the chat.")));
      yield* sql`INSERT INTO cloud_sandbox_projects (project_id, on_settle)
        VALUES (${thread.projectId}, ${input.choice})
        ON CONFLICT(project_id) DO UPDATE SET on_settle = excluded.on_settle`.pipe(
        Effect.mapError(chatError("Could not remember the project's sandbox answer.")),
      );
    }
    const updated = yield* readRow(input.threadId);
    return yield* stateOf(Option.getOrElse(updated, () => row.value));
  });

  // A restart drops the launch fiber; a chat whose first message is still held launches again.
  // Provisioning is idempotent by resource id, and a recorded sandbox project skips the copy.
  const recoverLaunches = Effect.gen(function* () {
    const rows = yield* sql`SELECT thread_id, resource_id, target, sandbox_project_id, settle
      FROM cloud_sandbox_chats WHERE settle = 'none'`.pipe(
      Effect.flatMap((found) => Effect.forEach(found, (row) => decodeRow(row))),
    );
    yield* Effect.forEach(
      rows,
      (row) =>
        preparingRuns(row.thread_id).pipe(
          Effect.flatMap((runs) => (runs.length === 0 ? Effect.void : launch(row.thread_id))),
        ),
      { discard: true },
    );
  });

  // One resume per run: a run's later updates must not reconnect the follower again.
  const resumingRuns = new Set<string>();
  const followEvents = Stream.runForEach(threads.streamDomainEvents, (event) => {
    if (event.type === "thread.settled")
      return onSettled(event.threadId).pipe(
        Effect.catchCause((cause) => Effect.logWarning("Sandbox chat settle failed", { cause })),
        Effect.forkDetach,
        Effect.asVoid,
      );
    if (
      (event.type === "run.created" || event.type === "run.updated") &&
      event.payload.providerInstanceId === instanceId
    ) {
      const run = event.payload;
      if (!STARTING_RUN.has(run.status)) {
        resumingRuns.delete(run.id);
        return Effect.void;
      }
      if (resumingRuns.has(run.id)) return Effect.void;
      resumingRuns.add(run.id);
      return resumeFor(run.threadId).pipe(
        Effect.catchCause((cause) => Effect.logWarning("Sandbox chat resume failed", { cause })),
        Effect.forkDetach,
        Effect.asVoid,
      );
    }
    return Effect.void;
  });

  return SandboxChats.of({
    startChat,
    state: Effect.fn("SandboxChats.state")(function* (threadId) {
      const row = yield* readRow(threadId);
      return Option.isNone(row) ? null : yield* stateOf(row.value);
    }),
    resolveSettle,
    onSettled,
    open: Effect.fn("SandboxChats.open")(function* (threadId) {
      const row = yield* readRow(threadId);
      // A pending answer asks first; a deleted sandbox has nothing to resume.
      if (Option.isNone(row) || row.value.settle === "pending" || row.value.settle === "deleted")
        return;
      yield* resumeFor(threadId);
    }),
    start: Effect.fn("SandboxChats.start")(function* () {
      yield* seedInstance.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not seed the Sandboxes provider instance", { cause }),
        ),
      );
      yield* recoverLaunches.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not recover sandbox chat launches", { cause }),
        ),
        Effect.forkScoped,
      );
      yield* followEvents.pipe(
        Effect.catchCause((cause) => Effect.logWarning("Sandbox chat follower stopped", { cause })),
        Effect.forkScoped,
      );
    }),
  });
});

export const layer = Layer.effect(SandboxChats, make);
