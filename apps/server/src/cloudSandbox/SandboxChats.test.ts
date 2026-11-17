import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { expect, it } from "@effect/vitest";
import {
  type CloudSandboxChatStartInput,
  EnvironmentId,
  type ManagedCloudSandboxControlOperation,
  type ManagedCloudSandboxProvisionInput,
  ManagedCloudSandboxResourceId,
  type ManagedCloudSandboxResource,
  MessageId,
  type OrchestrationV2ServerCommand,
  ProjectId,
  ProjectId as ProjectIdSchema,
  RunId,
  SandboxProfileId,
  ThreadId,
  ChatAttachmentId,
  type WorktreeSetupSnapshot,
} from "@t3tools/contracts";
import { bridgeLinkSecretName } from "@t3tools/sandbox-bridge/link";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Projects from "../project/ProjectService.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import * as Settings from "../serverSettings.ts";
import * as ProjectTransfer from "../workspace/CloudSandboxProjectTransfer.ts";
import * as Manager from "./CloudSandboxManager.ts";
import * as Profiles from "./CloudSandboxProfiles.ts";
import { CloudSandboxManagerError } from "./CloudSandboxStore.ts";
import * as SandboxChats from "./SandboxChats.ts";

const projectId = ProjectId.make("project-1");
const controller = EnvironmentId.make("controller");

const harness = ({
  pauseProvisionOn,
}: {
  /** Park the launch inside provisioning; `entered` fires at the gate, `release` opens it. */
  readonly pauseProvisionOn?: {
    readonly entered: Deferred.Deferred<void>;
    readonly release: Deferred.Deferred<void>;
  };
} = {}) => {
  const resources = new Map<string, ManagedCloudSandboxResource>();
  const provisions: Array<ManagedCloudSandboxProvisionInput> = [];
  const resumes: Array<string> = [];
  const controls: Array<{ id: string; operation: ManagedCloudSandboxControlOperation }> = [];
  const binds: Array<{ id: string; retention: string | undefined }> = [];
  const released: Array<string> = [];
  const transfers: Array<string> = [];
  const commands: Array<OrchestrationV2ServerCommand> = [];
  const secrets = new Map<string, Uint8Array>();
  const threadProjects = new Map<string, ProjectId>();
  const launched = new Map<string, Deferred.Deferred<void>>();

  const route = (id: string): Manager.ManagedCloudSandboxRoute => ({
    httpBaseUrl: "http://127.0.0.1:9999",
    pairingToken: "pairing",
    connection: {
      sandboxId: `vm-${id}`,
      driverKind: "boat" as never,
      resourceId: ManagedCloudSandboxResourceId.make(id),
      controllerEnvironmentId: controller,
      environmentId: EnvironmentId.make(`env-${id}`),
      label: "Sandbox",
      routePrefix: `/proxy/${id}`,
      bearerToken: `bearer-${id}`,
    },
  });

  const layer = SandboxChats.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Sqlite.layerMemory,
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-sandbox-chats-" }),
        Layer.succeed(
          SecureRandom.SecureRandom,
          SecureRandom.make((size) => crypto.getRandomValues(new Uint8Array(size))),
        ),
        Layer.mock(ServerSecretStore.ServerSecretStore)({
          get: (name) =>
            Effect.sync(() => {
              const value = secrets.get(name);
              return value === undefined ? Option.none() : Option.some(value);
            }),
          set: (name, value) => Effect.sync(() => void secrets.set(name, value)),
          remove: (name) => Effect.sync(() => void secrets.delete(name)),
        }),
        Layer.mock(Settings.ServerSettingsService)({
          getSettings: Effect.succeed({
            providerInstances: {},
            projectSettingsOverrides: {},
          } as never),
          updateSettings: () => Effect.succeed({} as never),
          updateProviderInstance: () => Effect.succeed({} as never),
        }),
        Layer.mock(DocumentText.DocumentText)({
          ensure: () => Effect.succeed<string | undefined>(undefined),
        }),
        Layer.mock(Threads.ThreadManagementService)({
          streamDomainEvents: Stream.empty,
          dispatch: (command) =>
            Effect.gen(function* () {
              commands.push(command);
              if (command.type === "thread.create")
                threadProjects.set(command.threadId, command.projectId);
              if (command.type === "prepared-run.release" || command.type === "prepared-run.fail") {
                const done = launched.get(command.threadId);
                if (done !== undefined) yield* Deferred.succeed(done, undefined);
              }
              // The consumed attachments ride back in the stored events, which keeps the
              // intake's fresh claims instead of releasing them.
              const storedEvents =
                command.type === "message.dispatch"
                  ? [
                      {
                        event: {
                          type: "message.updated",
                          payload: { attachments: command.attachments },
                        },
                      },
                    ]
                  : [];
              return { storedEvents } as never;
            }),
          getThreadRecords: (threadId) =>
            Effect.succeed({
              thread: {
                id: threadId,
                projectId: threadProjects.get(threadId) ?? projectId,
                title: "Fix the build",
              },
              runs: [{ id: RunId.make(`run-${threadId}`), status: "preparing" }],
            } as never),
        }),
        Layer.mock(Projects.ProjectService)({
          getById: () =>
            Effect.succeedSome({ id: projectId, title: "Repo", workspaceRoot: "/repo" } as never),
        }),
        Layer.mock(Profiles.CloudSandboxProfiles)({
          list: Effect.succeed([
            {
              profile: {
                profileId: SandboxProfileId.make("boat"),
                template: null,
                defaultSize: "small",
                autoStopMinutes: 30,
              },
              spend: null,
            } as never,
          ]),
        }),
        Layer.mock(Manager.CloudSandboxManager)({
          get: (id) => {
            const found = resources.get(id);
            return found === undefined
              ? Effect.fail(new CloudSandboxManagerError({ reason: "not-found" }))
              : Effect.succeed(found);
          },
          provision: (input) =>
            Effect.gen(function* () {
              if (pauseProvisionOn !== undefined) {
                yield* Deferred.succeed(pauseProvisionOn.entered, undefined);
                yield* Deferred.await(pauseProvisionOn.release);
              }
              provisions.push(input);
              resources.set(input.operationId, {
                resourceId: input.operationId,
                controllerEnvironmentId: controller,
                phase: "connected",
                owner: {
                  _tag: "thread",
                  hostEnvironmentId: controller,
                  threadId: input.ownership!.sourceThreadId,
                },
                retention: input.ownership!.retention,
              } as never);
              return route(input.operationId);
            }),
          connect: (id, options) =>
            Effect.sync(() => {
              if (options?.resume === true) resumes.push(id);
              return route(id);
            }),
          execute: (id, operation) =>
            Effect.sync(() => {
              controls.push({ id, operation });
              return {} as never;
            }),
          bindOwner: (id, _owner, retention) =>
            Effect.sync(() => {
              binds.push({ id, retention });
              return resources.get(id)!;
            }),
          release: (id) =>
            Effect.sync(() => {
              released.push(id);
              resources.set(id, { ...resources.get(id)!, phase: "deleted" });
            }),
        }),
      ),
    ),
    Layer.provide(
      Layer.mock(ProjectTransfer.CloudSandboxProjectTransfer)({
        transfer: ({ resourceId }) =>
          Effect.sync(() => {
            transfers.push(resourceId);
            return "/sandbox/repo";
          }),
      }),
    ),
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    Layer.provideMerge(WorktreeSetupTracker.layer),
  );

  const startChat = (
    threadId: ThreadId,
    target: "new" | "project",
    attachments?: CloudSandboxChatStartInput["message"]["attachments"],
  ) =>
    Effect.gen(function* () {
      const chats = yield* SandboxChats.SandboxChats;
      const done = yield* Deferred.make<void>();
      launched.set(threadId, done);
      yield* chats.startChat({
        threadId,
        projectId,
        target: { _tag: target },
        message: {
          messageId: MessageId.make(`message-${threadId}`),
          text: "Fix the build",
          title: "Fix the build",
          ...(attachments === undefined ? {} : { attachments }),
        },
        runtimeMode: "full-access",
        interactionMode: "default",
      });
      yield* Deferred.await(done);
    });

  return {
    layer,
    launched,
    startChat,
    provisions,
    resumes,
    controls,
    binds,
    released,
    transfers,
    commands,
    secrets,
  };
};

const registrar = Effect.provideService(SandboxChats.SandboxProjectRegistrar, (_link, mutation) =>
  Effect.succeed({ id: ProjectIdSchema.make(mutation.projectId) } as never),
);

it.effect(
  "starts a new-sandbox chat owned by its thread and releases the held message once linked",
  () => {
    const h = harness();
    const threadId = ThreadId.make("thread-new");
    return Effect.gen(function* () {
      yield* h.startChat(threadId, "new", [
        {
          type: "image",
          name: "screen.png",
          mimeType: "image/png",
          sizeBytes: 3,
          dataUrl: "data:image/png;base64,YWJj",
        },
      ]);
      expect(h.provisions).toHaveLength(1);
      expect(h.provisions[0]?.ownership).toEqual({
        sourceThreadId: threadId,
        retention: "delete-on-completion",
      });
      const create = h.commands.find((command) => command.type === "thread.create");
      expect(create).toMatchObject({ modelSelection: { instanceId: "sandboxes" } });
      const message = h.commands.find((command) => command.type === "message.dispatch");
      expect(message).toMatchObject({ dispatchMode: { type: "defer_start" } });
      // The upload is stored on the host like any chat image; the bridge forwards it from there.
      const [stored] = message?.type === "message.dispatch" ? message.attachments : [];
      expect(stored).toMatchObject({ type: "image", name: "screen.png", sizeBytes: 3 });
      expect(stored).not.toHaveProperty("dataUrl");
      expect(h.commands.at(-1)?.type).toBe("prepared-run.release");
      const link = JSON.parse(
        new TextDecoder().decode(h.secrets.get(bridgeLinkSecretName(threadId))),
      );
      expect(link).toMatchObject({ threadId, token: `bearer-${h.provisions[0]?.operationId}` });

      const chats = yield* SandboxChats.SandboxChats;
      yield* chats.startChat({
        threadId,
        projectId,
        target: { _tag: "new" },
        message: { messageId: MessageId.make("again"), text: "again", title: "again" },
        runtimeMode: "full-access",
        interactionMode: "default",
      });
      expect(h.provisions).toHaveLength(1);
    }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
  },
);

it.effect(
  "reports provisioning stages on the setup tracker while the first message is held",
  () => {
    // Park the launch inside provisioning so the card's mid-flight snapshot is observable.
    const gate = {
      entered: Deferred.makeUnsafe<void>(),
      release: Deferred.makeUnsafe<void>(),
    };
    const h = harness({ pauseProvisionOn: gate });
    const threadId = ThreadId.make("thread-tracker");
    return Effect.gen(function* () {
      const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
      const chats = yield* SandboxChats.SandboxChats;
      const settled = yield* Deferred.make<WorktreeSetupSnapshot>();
      // The stream slides to the newest snapshot, and the terminal one is retained, so
      // awaiting a non-running snapshot settles even if every intermediate one is dropped.
      const collector = yield* tracker.stream(threadId).pipe(
        Stream.runForEach((snapshot) =>
          snapshot !== null && snapshot.phase !== "running"
            ? Deferred.succeed(settled, snapshot)
            : Effect.void,
        ),
        Effect.forkScoped,
      );

      yield* chats.startChat({
        threadId,
        projectId,
        target: { _tag: "new" },
        message: {
          messageId: MessageId.make(`message-${threadId}`),
          text: "Fix the build",
          title: "Fix the build",
        },
        runtimeMode: "full-access",
        interactionMode: "default",
      });
      yield* Deferred.await(gate.entered);

      const midFlight = yield* tracker.get(threadId);
      expect(midFlight?.phase).toBe("running");
      expect(midFlight?.stages.map((stage) => [stage.id, stage.status])).toEqual([
        ["provision", "running"],
        ["connect", "pending"],
        ["copy", "pending"],
        ["register", "pending"],
        ["start", "pending"],
      ]);

      yield* Deferred.succeed(gate.release, undefined);
      const finalSnapshot = yield* Deferred.await(settled);
      yield* Fiber.interrupt(collector).pipe(Effect.ignore);

      expect(finalSnapshot.phase).toBe("done");
      expect(finalSnapshot.stages.map((stage) => [stage.id, stage.status])).toEqual([
        ["provision", "done"],
        ["connect", "done"],
        ["copy", "done"],
        ["register", "done"],
        ["start", "done"],
      ]);
      // The sandbox repo root shows in the card's details.
      expect(finalSnapshot.worktreePath).toBe("/sandbox/repo");
    }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
  },
);

it.effect("cancelling setup settles the card and fails the held first message retryable", () => {
  const gate = {
    entered: Deferred.makeUnsafe<void>(),
    release: Deferred.makeUnsafe<void>(),
  };
  const h = harness({ pauseProvisionOn: gate });
  const threadId = ThreadId.make("thread-tracker-cancel");
  return Effect.gen(function* () {
    const tracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
    const chats = yield* SandboxChats.SandboxChats;
    yield* chats.startChat({
      threadId,
      projectId,
      target: { _tag: "new" },
      message: {
        messageId: MessageId.make(`message-${threadId}`),
        text: "Fix the build",
        title: "Fix the build",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
    });
    yield* Deferred.await(gate.entered);

    expect(yield* tracker.cancel(threadId)).toBe(true);
    expect((yield* tracker.get(threadId))?.phase).toBe("cancelled");
    // The held first message must leave `preparing` so the composer unblocks.
    const fail = h.commands.filter((command) => command.type === "prepared-run.fail").at(-1);
    expect(fail).toMatchObject({ threadId, failure: { retryable: true } });
  }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
});

it.effect(
  "claims a web client's pending upload so the sandboxed turn reads the stored file",
  () => {
    const h = harness();
    const threadId = ThreadId.make("thread-upload");
    return Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      const pendingId = ChatAttachmentId.make("pending-3f2504e0-4f89-11d3-9a0c-0305e82c3301");
      NodeFS.mkdirSync(config.attachmentsDir, { recursive: true });
      NodeFS.writeFileSync(NodePath.join(config.attachmentsDir, `${pendingId}.png`), "abc");
      // A web composer still owns this pending upload and releases it shortly after sending;
      // the claim must have copied the bytes to the thread-scoped id before that release.
      yield* h.startChat(threadId, "new", [
        { type: "image", id: pendingId, name: "screen.png", mimeType: "image/png", sizeBytes: 3 },
      ]);
      const message = h.commands.find((command) => command.type === "message.dispatch");
      const [stored] = message?.type === "message.dispatch" ? message.attachments : [];
      expect(stored?.id).not.toBe(pendingId);
      expect(String(stored?.id)).toMatch(/^thread-upload-/);
      expect(stored).toMatchObject({ type: "image", name: "screen.png", sizeBytes: 3 });
      expect(NodeFS.existsSync(NodePath.join(config.attachmentsDir, `${stored?.id}.png`))).toBe(
        true,
      );
    }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
  },
);

it.effect("reuses the project's sandbox instead of provisioning a second one", () => {
  const h = harness();
  return Effect.gen(function* () {
    yield* h.startChat(ThreadId.make("thread-a"), "project");
    yield* h.startChat(ThreadId.make("thread-b"), "project");
    expect(h.provisions).toHaveLength(1);
    expect(h.provisions[0]?.ownership?.retention).toBe("stop-and-retain");
    expect(h.resumes).toEqual([h.provisions[0]?.operationId]);
    // The second chat finds the project already on the sandbox; copying again could clobber work.
    expect(h.transfers).toHaveLength(1);
  }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
});

it.effect("asks once on settle, keeps by stopping, resumes on open, and remembers", () => {
  const h = harness();
  const first = ThreadId.make("thread-keep");
  const second = ThreadId.make("thread-keep-later");
  return Effect.gen(function* () {
    const chats = yield* SandboxChats.SandboxChats;
    yield* h.startChat(first, "new");
    yield* chats.onSettled(first);
    yield* chats.onSettled(first);
    expect(yield* chats.state(first)).toMatchObject({ settle: "pending", projectDefault: null });
    // Without an answer the follower stops instead of billing, exactly once.
    expect(h.controls.map(({ operation }) => operation._tag)).toEqual(["stop"]);
    // Opening a chat that still owes an answer asks first; it does not wake the sandbox.
    yield* chats.open(first);
    expect(h.resumes).toEqual([]);

    const kept = yield* chats.resolveSettle({ threadId: first, choice: "keep", remember: true });
    expect(kept).toMatchObject({ settle: "kept", projectDefault: "keep" });
    expect(h.binds).toEqual([{ id: h.provisions[0]?.operationId, retention: "stop-and-retain" }]);
    expect(h.released).toEqual([]);
    yield* chats.open(first);
    expect(h.resumes).toEqual([h.provisions[0]?.operationId]);

    yield* h.startChat(second, "new");
    yield* chats.onSettled(second);
    expect(yield* chats.state(second)).toMatchObject({ settle: "kept" });
    expect(h.binds).toHaveLength(2);
  }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
});

it.effect("deletes a settled chat's sandbox and its link when the user says delete", () => {
  const h = harness();
  const threadId = ThreadId.make("thread-delete");
  return Effect.gen(function* () {
    const chats = yield* SandboxChats.SandboxChats;
    yield* h.startChat(threadId, "new");
    yield* chats.onSettled(threadId);
    const deleted = yield* chats.resolveSettle({ threadId, choice: "delete", remember: false });
    expect(deleted).toMatchObject({ settle: "deleted", projectDefault: null });
    expect(h.released).toEqual([h.provisions[0]?.operationId]);
    expect(h.secrets.has(bridgeLinkSecretName(threadId))).toBe(false);
    // Answered: a later settle of the same pairing asks nothing and deletes nothing more.
    yield* chats.onSettled(threadId);
    yield* chats.open(threadId);
    expect(h.released).toHaveLength(1);
    expect(h.resumes).toEqual([]);
  }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
});

it.effect("relaunches a chat whose launch a restart cut short, exactly once", () => {
  const h = harness();
  const threadId = ThreadId.make("thread-restart");
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // The restarted server finds the chat recorded and its first message still held.
    yield* sql`INSERT INTO cloud_sandbox_chats (thread_id, resource_id, target)
      VALUES (${threadId}, ${"chat-restarted"}, ${"new"})`;
    const done = yield* Deferred.make<void>();
    h.launched.set(threadId, done);
    const chats = yield* SandboxChats.SandboxChats;
    yield* chats.start();
    yield* Deferred.await(done);
    expect(h.provisions.map((input) => input.operationId)).toEqual(["chat-restarted"]);
    expect(h.provisions[0]?.ownership?.sourceThreadId).toBe(threadId);
    expect(h.commands.filter((command) => command.type === "prepared-run.release")).toHaveLength(1);
    expect(h.transfers).toEqual(["chat-restarted"]);

    // Another restart finds the follower and its copied project: it reconnects, nothing more.
    const again = yield* Deferred.make<void>();
    h.launched.set(threadId, again);
    yield* chats.start();
    yield* Deferred.await(again);
    expect(h.provisions).toHaveLength(1);
    expect(h.transfers).toHaveLength(1);
    expect(h.resumes).toEqual(["chat-restarted"]);
  }).pipe(Effect.provide(h.layer), registrar, Effect.scoped);
});
