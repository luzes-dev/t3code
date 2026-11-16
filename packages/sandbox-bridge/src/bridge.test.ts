import {
  type MessageId,
  type ModelSelection,
  SANDBOX_BRIDGE_BACKGROUND_WORK_METHOD,
  SANDBOX_BRIDGE_CANCEL_META,
  SANDBOX_BRIDGE_DEFAULT_MODEL_META,
  SANDBOX_BRIDGE_MESSAGE_META,
  SANDBOX_BRIDGE_STEER_CANCEL,
  SANDBOX_BRIDGE_WAKE_METHOD,
  SANDBOX_BRIDGE_WITHDRAW_METHOD,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadLaunchInput,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadStreamItem,
  ProjectId,
  type RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import type * as Cause from "effect/Cause";
import type * as Scope from "effect/Scope";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as AcpAgent from "effect-acp/agent";
import * as AcpClient from "effect-acp/client";
import type * as AcpSchema from "effect-acp/compat";

import {
  approvalOptions,
  answersOf,
  decisionOf,
  hostMcpOf,
  SANDBOX_GONE_MESSAGE,
  PROBE_CLIENT_NAME,
  promptText,
  modeSyncCommands,
  runSandboxBridge,
  sandboxConfigOptions,
  sandboxSelectionFor,
  SandboxWork,
  SandboxBridgeError,
  type SandboxBridgePorts,
  type SandboxSession,
  skippedAttachmentsMessage,
  toolTitleOf,
  TurnTracker,
} from "./bridge.ts";
import { releasableEnd } from "./images.ts";
import type { SandboxBridgeLink, SandboxBridgeResumeFailure } from "./link.ts";

const threadId = ThreadId.make("thread-sandbox-1");
const sandboxProjectId = ProjectId.make("project-on-sandbox");

const link: SandboxBridgeLink = {
  threadId,
  instanceId: "sandbox-instance",
  sandboxId: "bx_123",
  label: "api",
  httpBaseUrl: "http://sandbox.test:3773",
  sandboxProjectId,
  token: "bridge-token",
};

const hostMcpServer = {
  name: "t3-code",
  command: "/usr/bin/t3",
  args: ["mcp"],
  env: [
    { name: "T3_ACP_MCP_ENDPOINT", value: "http://127.0.0.1:3773/mcp" },
    { name: "T3_ACP_MCP_AUTHORIZATION", value: "Bearer host-mcp" },
  ],
};

const codexSelection = (model: string, options?: ModelSelection["options"]) =>
  ({ instanceId: "codex", model, ...(options ? { options } : {}) }) as ModelSelection;
const effort = {
  id: "effort",
  label: "Effort",
  type: "select",
  role: "reasoning",
  options: [
    { id: "medium", label: "Medium", isDefault: true },
    { id: "high", label: "High" },
  ],
} as const;
const sandboxProviders = [
  {
    instanceId: "codex",
    models: [
      {
        slug: "gpt-5.5",
        name: "GPT-5.5",
        isCustom: false,
        capabilities: { optionDescriptors: [effort] },
      },
      { slug: "gpt-5.5-mini", name: "GPT-5.5 Mini", isCustom: false, capabilities: null },
      { slug: "gpt-4", name: "GPT-4", isCustom: false, isLegacy: true, capabilities: null },
    ],
  },
] as unknown as SandboxSession["providers"];
const bridgeSelection = (model: string, options?: ModelSelection["options"]) =>
  ({ instanceId: "sandboxes", model, ...(options ? { options } : {}) }) as ModelSelection;

const encoder = new TextEncoder();

/** Two in-memory stdio ends wired to each other, so the real ACP client talks to the bridge. */
const crossedStdio = Effect.gen(function* () {
  const toAgent = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const toClient = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const writeTo = (queue: Queue.Queue<Uint8Array, Cause.Done<void>>) =>
    Sink.forEach((chunk: string | Uint8Array) =>
      Queue.offer(queue, typeof chunk === "string" ? encoder.encode(chunk) : chunk),
    );
  return {
    agent: Stdio.make({
      args: Effect.succeed([]),
      stdin: Stream.fromQueue(toAgent),
      stdout: () => writeTo(toClient),
      stderr: () => Sink.drain,
    }),
    client: Stdio.make({
      args: Effect.succeed([]),
      stdin: Stream.fromQueue(toClient),
      stdout: () => writeTo(toAgent),
      stderr: () => Sink.drain,
    }),
  };
});

// Entities the bridge only reads a few fields of; the rest of each schema does not matter here.
const run = (messageId: MessageId, status: string, id = "run-1", ordinal = 1) =>
  ({ id, threadId, userMessageId: messageId, status, ordinal }) as never;
// When the sandbox last touched an item, in ms; fixtures default to one shared instant.
const at = (ms = 1_000) => DateTime.makeUnsafe(ms);
const assistant = (id: string, text: string, ordinal: number, runId = "run-1", ms?: number) =>
  ({
    id,
    type: "assistant_message",
    text,
    status: "running",
    ordinal,
    runId,
    updatedAt: at(ms),
  }) as never;
const approval = (requestId: string, requestKind: string, ordinal = 5) =>
  ({
    id: `item-${requestId}`,
    type: "approval_request",
    requestId,
    requestKind,
    title: "Run a command",
    prompt: "rm -rf build",
    status: "waiting",
    ordinal,
    updatedAt: at(),
  }) as never;
const question = (requestId: string) =>
  ({
    id: `item-${requestId}`,
    type: "user_input_request",
    requestId,
    status: "waiting",
    ordinal: 6,
    updatedAt: at(),
    questions: [
      {
        id: "color",
        header: "Color",
        question: "Which color?",
        options: [
          { label: "Red", value: "red" },
          { label: "Blue", value: "blue" },
        ],
      },
    ],
  }) as never;
const event = (type: string, payload: unknown): OrchestrationV2ThreadStreamItem =>
  ({ kind: "event", sequence: 1, event: { type, payload } }) as never;
const projection = (
  turnItems: ReadonlyArray<unknown>,
  runs: ReadonlyArray<unknown> = [],
  runtimeMode: RuntimeMode = "approval-required",
  modelSelection: ModelSelection = codexSelection("gpt-5.5"),
) =>
  ({
    thread: {
      runtimeMode,
      interactionMode: "default",
      modelSelection,
      activeProviderThreadId: null,
    },
    turnItems,
    runs,
    runtimeRequests: [],
    providerThreads: [],
    messages: [],
  }) as unknown as OrchestrationV2ThreadProjection;
const userMessage = (
  messageId: string,
  ordinal: number,
  runId: string | null = null,
  ms?: number,
) =>
  ({
    id: `item-${messageId}`,
    type: "user_message",
    messageId,
    runId,
    status: "completed",
    ordinal,
    startedAt: at(ms),
    updatedAt: at(ms),
  }) as never;

interface FakeSandbox {
  readonly session: SandboxSession;
  readonly commands: Array<OrchestrationV2Command>;
  readonly launches: Array<OrchestrationV2ThreadLaunchInput>;
  readonly uploads: Array<unknown>;
  readonly secretAnswers: Array<unknown>;
  readonly imageReads: Array<string>;
  readonly push: (item: OrchestrationV2ThreadStreamItem) => Effect.Effect<void>;
  /** What the sandbox's thread holds now, for the next read or subscription. */
  readonly settle: (projection: OrchestrationV2ThreadProjection) => void;
}

/** A sandbox whose thread stream the test drives; `react` answers each command it receives. */
const makeFakeSandbox = (options: {
  readonly existing?: OrchestrationV2ThreadProjection;
  readonly providers?: SandboxSession["providers"];
  readonly threadStop?: boolean;
  /** Images on the sandbox, by the path a reply names them with. */
  readonly images?: Readonly<Record<string, string>>;
  readonly react?: (
    command: { readonly type: string; readonly messageId?: MessageId | undefined },
    fake: FakeSandbox,
  ) => Effect.Effect<void>;
}) =>
  Effect.gen(function* () {
    const stream = yield* Queue.unbounded<OrchestrationV2ThreadStreamItem>();
    const commands: Array<OrchestrationV2Command> = [];
    const launches: Array<OrchestrationV2ThreadLaunchInput> = [];
    const uploads: Array<unknown> = [];
    const secretAnswers: Array<unknown> = [];
    const imageReads: Array<string> = [];
    let existing = options.existing;
    const fake: FakeSandbox = {
      commands,
      launches,
      uploads,
      secretAnswers,
      imageReads,
      push: (item) => Queue.offer(stream, item).pipe(Effect.asVoid),
      settle: (projection) => {
        existing = projection;
      },
      session: {
        threadStop: options.threadStop ?? true,
        defaultModelSelection: { instanceId: "codex", model: "gpt-5.5" } as never,
        providers: options.providers ?? [],
        projection: () => Effect.sync(() => Option.fromNullishOr(existing)),
        launch: (input) =>
          Effect.suspend(() => {
            launches.push(input);
            return (
              options.react?.(
                { type: "launch", messageId: input.initialMessage?.messageId },
                fake,
              ) ?? Effect.void
            );
          }),
        dispatch: (command) =>
          Effect.suspend(() => {
            commands.push(command);
            return options.react?.(command as never, fake) ?? Effect.void;
          }),
        persistAttachments: (input) =>
          Effect.sync(() => {
            uploads.push(input);
            return input.attachments.map((attachment, index) => ({
              type: "image" as const,
              id: `saved-${index}`,
              name: attachment.name,
              mimeType: attachment.mimeType,
              sizeBytes: attachment.sizeBytes,
            })) as never;
          }),
        // As a sandbox does: the thread's snapshot first, and nothing for a thread it lacks.
        subscribe: () =>
          Stream.suspend(() =>
            existing === undefined && launches.length === 0
              ? Stream.empty
              : Stream.concat(
                  existing === undefined
                    ? Stream.empty
                    : Stream.make({
                        kind: "snapshot",
                        snapshotSequence: 0,
                        projection: existing,
                      } as OrchestrationV2ThreadStreamItem),
                  Stream.fromQueue(stream),
                ),
          ),
        readImage: (_threadId, path) =>
          Effect.suspend(() => {
            imageReads.push(path);
            const image = options.images?.[path];
            return image === undefined
              ? Effect.fail(new SandboxBridgeError({ message: "No such file on the sandbox." }))
              : Effect.succeed({ bytes: encoder.encode(image), extension: ".png" });
          }),
        answerSecret: (input) =>
          Effect.suspend(() => {
            secretAnswers.push(input);
            return options.react?.({ type: "secret.answer" }, fake) ?? Effect.void;
          }),
      },
    };
    return fake;
  });

const launchedMessageId = (sandbox: FakeSandbox) =>
  sandbox.launches[0]?.initialMessage?.messageId as MessageId;

/** A turn that streams `text` and completes, the way a sandbox reports one. */
const answerWith =
  (text: string) =>
  (
    command: { readonly type: string; readonly messageId?: MessageId | undefined },
    fake: FakeSandbox,
  ) =>
    command.type === "launch" || command.type === "message.dispatch"
      ? Effect.forEach(
          [
            event("run.created", run(command.messageId!, "running")),
            event("turn-item.updated", assistant("answer", text.slice(0, 5), 2)),
            event("turn-item.updated", assistant("answer", text, 2)),
            event("run.updated", run(command.messageId!, "completed")),
          ],
          fake.push,
          { discard: true },
        )
      : Effect.void;

interface Harness {
  readonly acp: AcpClient.AcpClient["Service"];
  readonly updates: Array<AcpSchema.SessionNotification>;
  readonly permissions: Array<AcpSchema.RequestPermissionRequest>;
  readonly elicitations: Array<AcpSchema.CreateElicitationRequest>;
  readonly withdrawals: Array<unknown>;
  /** The background rosters the bridge reported, in order. */
  readonly rosters: Queue.Queue<unknown>;
  /** The runs the bridge asked the host for, in order. */
  readonly wakes: Queue.Queue<unknown>;
}

const messages = (
  updates: ReadonlyArray<AcpSchema.SessionNotification>,
  kind = "agent_message_chunk",
) =>
  updates.flatMap((notification) =>
    notification.update.sessionUpdate === kind && "content" in notification.update
      ? [(notification.update.content as { readonly text: string }).text]
      : [],
  );

/** Runs the bridge against the real effect-acp client, crossed over in-memory stdio. */
const withBridge = <A, E>(
  options: {
    readonly fake?: FakeSandbox;
    readonly linked?: boolean;
    readonly runtimeMode?: RuntimeMode;
    readonly hostModel?: ModelSelection;
    readonly connect?: SandboxBridgePorts["connect"];
    readonly findResumeFailure?: SandboxBridgePorts["findResumeFailure"];
    readonly requestSecret?: SandboxBridgePorts["requestSecret"];
    readonly saveImage?: SandboxBridgePorts["saveImage"];
    /** Null waits as long as the bridge does by default, spaced as it does by default. */
    readonly reconnectWindow?: null;
    readonly permission?: (
      request: AcpSchema.RequestPermissionRequest,
    ) => AcpSchema.RequestPermissionResponse;
    readonly elicitation?: AcpSchema.CreateElicitationResponse;
    /** Holds every permission request open, as a user who has not answered yet; set on arrival. */
    readonly permissionPending?: Deferred.Deferred<void>;
    readonly clientName?: string;
  },
  body: (harness: Harness) => Effect.Effect<A, E, Scope.Scope>,
) =>
  Effect.gen(function* () {
    const pipes = yield* crossedStdio;
    const ports: SandboxBridgePorts = {
      readHostThread: () =>
        Effect.succeed({
          threadId,
          modelSelection: options.hostModel ?? bridgeSelection("sandbox"),
          runtimeMode: options.runtimeMode ?? "approval-required",
          interactionMode: "default",
        }),
      findLink: () => Effect.succeed(options.linked === false ? Option.none() : Option.some(link)),
      connect:
        options.connect ??
        (() =>
          options.fake === undefined
            ? Effect.fail(new SandboxBridgeError({ message: "no sandbox" }))
            : Effect.succeed(options.fake.session)),
      ...(options.findResumeFailure === undefined
        ? {}
        : { findResumeFailure: options.findResumeFailure }),
      ...(options.requestSecret === undefined ? {} : { requestSecret: options.requestSecret }),
      ...(options.saveImage === undefined ? {} : { saveImage: options.saveImage }),
      ...(options.reconnectWindow === null
        ? {}
        : { reconnectWindow: "40 millis", reconnectSpacing: "10 millis" }),
    };
    yield* runSandboxBridge(ports).pipe(
      Effect.provide(Layer.mergeAll(AcpAgent.layer(pipes.agent), SecureRandom.layerWebCrypto)),
      Effect.forkScoped,
    );
    yield* Effect.yieldNow;
    const acp = yield* AcpClient.make(pipes.client);
    const harness: Harness = {
      acp,
      updates: [],
      permissions: [],
      elicitations: [],
      withdrawals: [],
      rosters: yield* Queue.unbounded<unknown>(),
      wakes: yield* Queue.unbounded<unknown>(),
    };
    yield* acp.handleExtNotification(
      SANDBOX_BRIDGE_BACKGROUND_WORK_METHOD,
      Schema.Unknown,
      (payload) => Queue.offer(harness.rosters, payload).pipe(Effect.asVoid),
    );
    yield* acp.handleExtNotification(SANDBOX_BRIDGE_WAKE_METHOD, Schema.Unknown, (payload) =>
      Queue.offer(harness.wakes, payload).pipe(Effect.asVoid),
    );
    yield* acp.handleExtNotification(SANDBOX_BRIDGE_WITHDRAW_METHOD, Schema.Unknown, (payload) =>
      Effect.sync(() => void harness.withdrawals.push(payload)),
    );
    yield* acp.handleSessionUpdate((notification) =>
      Effect.sync(() => void harness.updates.push(notification)),
    );
    yield* acp.handleRequestPermission((request) =>
      Effect.suspend(() => {
        harness.permissions.push(request);
        if (options.permissionPending !== undefined) {
          return Deferred.succeed(options.permissionPending, undefined).pipe(
            Effect.andThen(Effect.never),
          );
        }
        return Effect.succeed(
          options.permission?.(request) ?? {
            outcome: { outcome: "selected", optionId: request.options[0]!.optionId },
          },
        );
      }),
    );
    yield* acp.handleElicitation((request) =>
      Effect.sync(() => {
        harness.elicitations.push(request);
        return options.elicitation ?? { action: "cancel" };
      }),
    );
    yield* acp.agent.initialize({
      protocolVersion: 1,
      clientInfo: { name: options.clientName ?? "t3-code", version: "1" },
      clientCapabilities: { elicitation: { form: {} } },
    });
    return yield* body(harness);
  }).pipe(Effect.scoped);

const openThread = (acp: AcpClient.AcpClient["Service"]) =>
  acp.agent.createSession({ cwd: "/tmp/host-project", mcpServers: [hostMcpServer] });

const say = (
  acp: AcpClient.AcpClient["Service"],
  sessionId: string,
  text: string,
  message?: {
    readonly messageId: string;
    readonly createdBy?: string;
    readonly creationSource?: string;
  },
) =>
  acp.agent.prompt({
    sessionId,
    prompt: [{ type: "text", text }],
    ...(message === undefined
      ? {}
      : {
          _meta: {
            [SANDBOX_BRIDGE_MESSAGE_META]: {
              createdBy: "user",
              creationSource: "mobile",
              ...message,
            },
          },
        }),
  });

describe("the ACP side", () => {
  it.effect("advertises session loading and images, and gives the probe a dry-run session", () =>
    Effect.gen(function* () {
      const pipes = yield* crossedStdio;
      yield* runSandboxBridge({
        readHostThread: () => Effect.die("the probe must not read a host thread"),
        findLink: () => Effect.die("the probe must not look up a link"),
        connect: () => Effect.die("the probe must not reach a sandbox"),
      }).pipe(
        Effect.provide(Layer.mergeAll(AcpAgent.layer(pipes.agent), SecureRandom.layerWebCrypto)),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      const acp = yield* AcpClient.make(pipes.client);
      const init = yield* acp.agent.initialize({
        protocolVersion: 1,
        clientInfo: { name: PROBE_CLIENT_NAME, version: "1" },
      });
      assert.strictEqual(init.agentCapabilities?.loadSession, true);
      assert.strictEqual(init.agentCapabilities?.promptCapabilities?.image, true);
      const session = yield* openThread(acp);
      assert.isTrue(session.sessionId.startsWith("sandbox-dry-run:"));
      assert.deepStrictEqual(
        session.configOptions?.map((option) => option.id),
        ["model"],
      );
    }).pipe(Effect.scoped),
  );

  it.effect("says plainly that a thread's sandbox is gone once its link is", () =>
    withBridge({ linked: false }, ({ acp, updates }) =>
      Effect.gen(function* () {
        const session = yield* openThread(acp);
        assert.isTrue(session.sessionId.startsWith("sandbox-dry-run:"));
        const result = yield* say(acp, session.sessionId, "hello");
        assert.strictEqual(result.stopReason, "end_turn");
        // A plain reply, not an error, and no thinking left running.
        assert.deepStrictEqual(messages(updates), [
          "This chat's sandbox was deleted, so this message wasn't sent. The conversation stays here to read; to keep working, start a new chat from T3 Code on your computer.",
        ]);
        assert.strictEqual(SANDBOX_GONE_MESSAGE, messages(updates)[0]);
        assert.deepStrictEqual(messages(updates, "agent_thought_chunk"), []);
        assert.deepStrictEqual(updates.at(-1)?.update, {
          sessionUpdate: "state_update",
          state: "idle",
          stopReason: "end_turn",
        });
      }),
    ),
  );

  it.effect("resumes a linked session without replaying it", () =>
    withBridge({}, ({ acp, updates }) =>
      Effect.gen(function* () {
        yield* acp.agent.loadSession({
          sessionId: `sandbox:${threadId}`,
          cwd: "/tmp/host-project",
          mcpServers: [hostMcpServer],
        });
        assert.deepStrictEqual(updates, []);
      }),
    ),
  );
});

describe("the sandbox's models", () => {
  const sentModel = (fake: FakeSandbox) =>
    fake.commands.flatMap((command) =>
      command.type === "message.dispatch" ? [command.modelSelection ?? null] : [],
    );

  it.effect("offers the sandbox thread's models and effort, or none when it does not answer", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({
        providers: sandboxProviders,
        existing: projection(
          [],
          [],
          "approval-required",
          codexSelection("gpt-5.5", [{ id: "effort", value: "high" }]),
        ),
      });
      yield* withBridge({ fake }, ({ acp }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const [model, level] = session.configOptions ?? [];
          assert.deepInclude(model, { id: "model", currentValue: "gpt-5.5" });
          assert.deepStrictEqual(
            model?.type === "select" ? model.options.map((option) => option.name) : [],
            ["GPT-5.5", "GPT-5.5 Mini", "Sandbox default"],
          );
          assert.deepStrictEqual(model?._meta, { [SANDBOX_BRIDGE_DEFAULT_MODEL_META]: "gpt-5.5" });
          assert.deepInclude(level, {
            id: "effort",
            category: "thought_level",
            currentValue: "high",
          });
        }),
      );
      // An empty list keeps the models a sandbox listed before, instead of a placeholder.
      yield* withBridge({}, ({ acp }) =>
        Effect.gen(function* () {
          assert.deepStrictEqual((yield* openThread(acp)).configOptions, []);
        }),
      );
    }),
  );

  // The host marks the model a chat gets with nothing picked, not the one this chat runs.
  it("names the sandbox's default model apart from the thread's pick", () => {
    const [mini] =
      sandboxConfigOptions(
        sandboxProviders,
        codexSelection("gpt-5.5-mini"),
        codexSelection("gpt-5.5"),
      ) ?? [];
    assert.deepInclude(mini, {
      currentValue: "gpt-5.5-mini",
      _meta: { [SANDBOX_BRIDGE_DEFAULT_MODEL_META]: "gpt-5.5" },
    });
    const [elsewhere] =
      sandboxConfigOptions(sandboxProviders, codexSelection("gpt-5.5-mini"), {
        instanceId: "claudeAgent",
        model: "gpt-5.5",
      } as ModelSelection) ?? [];
    assert.notProperty(elsewhere, "_meta");
  });

  it.effect("sends the model picked on the host with the next message, and only then", () =>
    Effect.gen(function* () {
      const picked = bridgeSelection("gpt-5.5-mini", [{ id: "effort", value: "high" }]);
      const fake = yield* makeFakeSandbox({
        providers: sandboxProviders,
        existing: projection(
          [userMessage("from-computer", 1, "run-0")],
          [run("from-computer" as MessageId, "completed", "run-0")],
        ),
        react: answerWith("Done"),
      });
      yield* withBridge({ fake, hostModel: picked }, ({ acp }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          // A message the computer already sent keeps the model it was sent with.
          yield* say(acp, session.sessionId, "From the computer", { messageId: "from-computer" });
          assert.deepStrictEqual(sentModel(fake), []);
          yield* say(acp, session.sessionId, "From the phone", { messageId: "from-phone" });
          assert.deepStrictEqual(sentModel(fake), [
            codexSelection("gpt-5.5-mini", [{ id: "effort", value: "high" }]),
          ]);
        }),
      );
    }),
  );

  it("keeps the sandbox's model for the bridge's own value, a model it lacks, or the same one", () => {
    const current = codexSelection("gpt-5.5");
    for (const host of [
      bridgeSelection("sandbox"),
      bridgeSelection("claude-opus-5-5"),
      bridgeSelection("gpt-5.5"),
    ]) {
      assert.isNull(sandboxSelectionFor(host, current, sandboxProviders));
    }
    assert.deepStrictEqual(
      sandboxSelectionFor(bridgeSelection("gpt-5.5-mini"), current, sandboxProviders),
      codexSelection("gpt-5.5-mini"),
    );
  });

  // A host pick that only spelled options differently, or left them out, reset the sandbox's effort.
  it("compares options by meaning and keeps the sandbox's for any the host leaves out", () => {
    const fastMode = { id: "fastMode", label: "Fast", type: "boolean" } as const;
    const capabilities = { optionDescriptors: [effort, fastMode] };
    const providers = [
      {
        instanceId: "codex",
        models: [
          { slug: "gpt-5.5", name: "GPT-5.5", isCustom: false, capabilities },
          { slug: "gpt-5.5-pro", name: "GPT-5.5 Pro", isCustom: false, capabilities },
        ],
      },
    ] as unknown as SandboxSession["providers"];
    const high = { id: "effort", value: "high" };
    const current = codexSelection("gpt-5.5", [high]);
    for (const host of [
      bridgeSelection("gpt-5.5", [high, { id: "fastMode", value: false }]),
      bridgeSelection("gpt-5.5"),
      bridgeSelection("gpt-5.5", [{ id: "fastMode", value: false }]),
    ]) {
      assert.isNull(sandboxSelectionFor(host, current, providers));
    }
    assert.deepStrictEqual(
      sandboxSelectionFor(bridgeSelection("gpt-5.5-pro"), current, providers),
      codexSelection("gpt-5.5-pro", [high]),
    );
    assert.deepStrictEqual(
      sandboxSelectionFor(
        bridgeSelection("gpt-5.5", [{ id: "fastMode", value: true }]),
        current,
        providers,
      ),
      codexSelection("gpt-5.5", [{ id: "fastMode", value: true }, high]),
    );
    assert.deepStrictEqual(
      sandboxSelectionFor(
        bridgeSelection("gpt-5.5", [{ id: "effort", value: "medium" }]),
        current,
        providers,
      ),
      codexSelection("gpt-5.5", [{ id: "effort", value: "medium" }]),
    );
  });
});

describe("forwarding a turn", () => {
  it.effect("creates the sandbox thread with the host thread's id and streams the answer", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({ react: answerWith("Hello from the sandbox") });
      yield* withBridge({ fake, runtimeMode: "approval-required" }, ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          assert.strictEqual(session.sessionId, `sandbox:${threadId}`);
          const result = yield* say(acp, session.sessionId, "Fix the bug");
          assert.strictEqual(result.stopReason, "end_turn");
          assert.strictEqual(messages(updates).join(""), "Hello from the sandbox");
          const launch = fake.launches[0]!;
          assert.strictEqual(launch.threadId, threadId);
          assert.strictEqual(launch.projectId, sandboxProjectId);
          assert.strictEqual(launch.runtimeMode, "approval-required");
          assert.strictEqual(launch.initialMessage?.text, "Fix the bug");
          assert.deepStrictEqual(launch.modelSelection, {
            instanceId: "codex",
            model: "gpt-5.5",
          } as never);
          assert.deepStrictEqual(fake.commands, []);
        }),
      );
    }),
  );

  it.effect("sends to an existing sandbox thread without replaying its history", () =>
    Effect.gen(function* () {
      const earlierRun = run("msg-0" as MessageId, "completed", "run-1", 1);
      const fake = yield* makeFakeSandbox({
        existing: projection([assistant("old", "Earlier answer", 101)], [earlierRun]),
        react: (command, sandbox) =>
          command.type === "message.dispatch"
            ? Effect.forEach(
                [
                  {
                    kind: "snapshot",
                    snapshotSequence: 1,
                    projection: projection([assistant("old", "Earlier answer", 101)], [earlierRun]),
                  } as never,
                  event("run.created", run(command.messageId!, "running", "run-2", 2)),
                  event("turn-item.updated", assistant("new", "Next answer", 201, "run-2")),
                  event("run.updated", run(command.messageId!, "completed", "run-2", 2)),
                ],
                sandbox.push,
                { discard: true },
              )
            : Effect.void,
      });
      yield* withBridge({ fake }, ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const result = yield* say(acp, session.sessionId, "And now?");
          assert.strictEqual(result.stopReason, "end_turn");
          assert.deepStrictEqual(messages(updates), ["Next answer"]);
          const dispatched = fake.commands[0]!;
          assert.strictEqual(dispatched.type, "message.dispatch");
          assert.strictEqual(dispatched.type === "message.dispatch" && dispatched.text, "And now?");
          assert.deepStrictEqual(fake.launches, []);
        }),
      );
    }),
  );

  it.effect(
    "streams only what follows the message, though the sandbox's first read is a window",
    () =>
      Effect.gen(function* () {
        // Three runs on the sandbox; the third is still working when the message steers into it.
        const history = [
          userMessage("msg-1", 100, "run-1", 1_000),
          assistant("a1", "First answer", 101, "run-1", 1_001),
          userMessage("msg-2", 102, "run-2", 1_002),
          assistant("a2", "Second answer", 201, "run-2", 1_003),
          userMessage("msg-3", 202, "run-3", 1_004),
          assistant("a3", "Third answer", 301, "run-3", 1_005),
          assistant("a3b", "still going", 302, "run-3", 1_006),
        ];
        const runs = [
          run("msg-1" as MessageId, "completed", "run-1", 1),
          run("msg-2" as MessageId, "completed", "run-2", 2),
          run("msg-3" as MessageId, "running", "run-3", 3),
        ];
        const fake = yield* makeFakeSandbox({
          // `getThreadProjection` bounds its rows; the stream's snapshot is the whole thread.
          existing: projection(history.slice(-1), runs),
          react: (command, sandbox) =>
            command.type === "message.dispatch"
              ? Effect.forEach(
                  [
                    {
                      kind: "snapshot",
                      snapshotSequence: 1,
                      projection: projection(
                        [...history, userMessage(command.messageId!, 303, "run-3", 1_007)],
                        runs,
                      ),
                    } as never,
                    event(
                      "turn-item.updated",
                      assistant("a4", "Steered answer", 304, "run-3", 1_008),
                    ),
                    event("run.updated", run("msg-3" as MessageId, "completed", "run-3", 3)),
                  ],
                  sandbox.push,
                  { discard: true },
                )
              : Effect.void,
        });
        yield* withBridge({ fake }, ({ acp, updates }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            const result = yield* say(acp, session.sessionId, "Also check the tests", {
              messageId: "msg-steer",
            });
            assert.strictEqual(result.stopReason, "end_turn");
            assert.deepStrictEqual(messages(updates), ["Steered answer"]);
          }),
        );
      }),
  );

  it.effect("asks for a sandbox approval with a non-read kind and answers with the choice", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({
        react: (command, sandbox) => {
          if (command.type === "launch") {
            return Effect.forEach(
              [
                event("run.created", run(command.messageId!, "waiting")),
                event("turn-item.updated", approval("req-1", "command")),
              ],
              sandbox.push,
              { discard: true },
            );
          }
          if (command.type === "runtime-request.respond") {
            return sandbox.push(event("run.updated", run(launchedMessageId(sandbox), "completed")));
          }
          return Effect.void;
        },
      });
      yield* withBridge(
        {
          fake,
          permission: (request) => ({
            outcome: {
              outcome: "selected",
              optionId: request.options.find((option) => option.kind === "allow_always")!.optionId,
            },
          }),
        },
        ({ acp, permissions }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            const result = yield* say(acp, session.sessionId, "Clean up");
            assert.strictEqual(result.stopReason, "end_turn");
            const asked = permissions[0]!;
            assert.strictEqual(asked.toolCall.kind, "execute");
            assert.include(asked.toolCall.title ?? "", "Sandbox bx_123");
            // The host shows the request by its command, as the computer does.
            assert.deepStrictEqual(asked.toolCall.rawInput, { command: "rm -rf build" });
            assert.deepStrictEqual(
              fake.commands.map((command) => command.type),
              ["runtime-request.respond"],
            );
            const respond = fake.commands[0]!;
            assert.strictEqual(
              respond.type === "runtime-request.respond" && respond.decision,
              "acceptForSession",
            );
          }),
      );
    }),
  );

  it.effect("asks with a form when the thread is not Supervised, so its policy cannot answer", () =>
    Effect.gen(function* () {
      const approvalTurn = makeFakeSandbox({
        react: (command, sandbox) => {
          if (command.type === "launch") {
            return Effect.forEach(
              [
                event("run.created", run(command.messageId!, "waiting")),
                event("turn-item.updated", approval("req-1", "command", 5)),
              ],
              sandbox.push,
              { discard: true },
            );
          }
          return command.type === "runtime-request.respond"
            ? sandbox.push(event("run.updated", run(launchedMessageId(sandbox), "completed")))
            : Effect.void;
        },
      });
      // Full access grants it without asking anyone.
      for (const [runtimeMode, asked] of [
        ["auto", 1],
        ["full-access", 0],
      ] as const) {
        const fake = yield* approvalTurn;
        yield* withBridge(
          {
            fake,
            runtimeMode,
            elicitation: { action: "accept", content: { decision: "Allow" } },
          },
          ({ acp, permissions, elicitations }) =>
            Effect.gen(function* () {
              const session = yield* openThread(acp);
              const result = yield* say(acp, session.sessionId, "Clean up");
              assert.strictEqual(result.stopReason, "end_turn");
              assert.deepStrictEqual(permissions, []);
              assert.strictEqual(elicitations.length, asked);
              const respond = fake.commands.find(
                (command) => command.type === "runtime-request.respond",
              );
              assert.strictEqual(
                respond?.type === "runtime-request.respond" && respond.decision,
                "accept",
              );
            }),
        );
      }
    }),
  );

  it("moves the sandbox to new modes at once, granting its waiting approvals under full access", () => {
    const waiting = {
      ...projection([], [], "approval-required"),
      thread: { id: threadId, runtimeMode: "approval-required", interactionMode: "plan" },
      runtimeRequests: [
        { id: "req-cmd", kind: "command", status: "pending" },
        { id: "req-ask", kind: "user_input", status: "pending" },
        { id: "req-old", kind: "file-change", status: "resolved" },
      ],
    } as unknown as OrchestrationV2ThreadProjection;
    let count = 0;
    const commandId = () => `cmd-${count++}` as never;
    assert.deepStrictEqual(
      modeSyncCommands(
        waiting,
        { runtimeMode: "full-access", interactionMode: "default" },
        commandId,
      ),
      [
        {
          type: "thread.runtime-mode.set",
          commandId: "cmd-0",
          threadId,
          runtimeMode: "full-access",
        },
        {
          type: "thread.interaction-mode.set",
          commandId: "cmd-1",
          threadId,
          interactionMode: "default",
        },
        {
          type: "runtime-request.respond",
          commandId: "cmd-2",
          threadId,
          requestId: "req-cmd",
          decision: "accept",
        },
      ] as never,
    );
    assert.deepStrictEqual(
      modeSyncCommands(
        waiting,
        { runtimeMode: "approval-required", interactionMode: "plan" },
        commandId,
      ),
      [],
    );
  });

  it("grants a running sandbox's approvals under full access but moves its mode after the run", () => {
    // Changing a sandbox thread's runtime mode cuts its agent's run short.
    const running = {
      ...projection([], [run("msg-1" as MessageId, "running", "run-1")], "approval-required"),
      thread: { id: threadId, runtimeMode: "approval-required", interactionMode: "default" },
      runtimeRequests: [{ id: "req-cmd", kind: "command", status: "pending" }],
    } as unknown as OrchestrationV2ThreadProjection;
    let count = 0;
    const commandId = () => `cmd-${count++}` as never;
    assert.deepStrictEqual(
      modeSyncCommands(
        running,
        { runtimeMode: "full-access", interactionMode: "default" },
        commandId,
      ),
      [
        {
          type: "runtime-request.respond",
          commandId: "cmd-0",
          threadId,
          requestId: "req-cmd",
          decision: "accept",
        },
      ] as never,
    );
    const ended = {
      ...running,
      runs: [run("msg-1" as MessageId, "completed", "run-1")],
      runtimeRequests: [],
    } as unknown as OrchestrationV2ThreadProjection;
    assert.deepStrictEqual(
      modeSyncCommands(
        ended,
        { runtimeMode: "full-access", interactionMode: "default" },
        commandId,
      ),
      [
        {
          type: "thread.runtime-mode.set",
          commandId: "cmd-1",
          threadId,
          runtimeMode: "full-access",
        },
      ] as never,
    );
  });

  it.effect(
    "asks the host for a secret the sandbox needs and hands the answer to the sandbox",
    () =>
      Effect.gen(function* () {
        const secretRequest = {
          id: "item-secret-1",
          type: "secret_request",
          label: "GitHub token",
          reason: "To push the branch.",
          url: "https://github.com/settings/tokens",
          secretStatus: "pending",
          status: "waiting",
          runId: "run-1",
          ordinal: 5,
          updatedAt: at(),
        } as never;
        const fake = yield* makeFakeSandbox({
          react: (command, sandbox) =>
            command.type === "launch"
              ? Effect.forEach(
                  [
                    event("run.created", run(command.messageId!, "running")),
                    event("turn-item.updated", secretRequest),
                  ],
                  sandbox.push,
                  { discard: true },
                )
              : command.type === "secret.answer"
                ? sandbox.push(event("run.updated", run(launchedMessageId(sandbox), "completed")))
                : Effect.void,
        });
        const asked: Array<unknown> = [];
        yield* withBridge(
          {
            fake,
            requestSecret: (_mcp, request) =>
              Effect.sync(() => {
                asked.push(request);
                return { status: "saved", value: Redacted.make("ghp_secret-value") } as const;
              }),
          },
          ({ acp, updates }) =>
            Effect.gen(function* () {
              const session = yield* openThread(acp);
              const result = yield* say(acp, session.sessionId, "Push it");
              assert.strictEqual(result.stopReason, "end_turn");
              assert.deepStrictEqual(asked, [
                {
                  clientRequestId: "item-secret-1",
                  label: "GitHub token",
                  reason: "To push the branch.",
                  url: "https://github.com/settings/tokens",
                },
              ]);
              assert.deepStrictEqual(fake.secretAnswers, [
                {
                  threadId,
                  turnItemId: "item-secret-1",
                  answer: { type: "save", secret: "ghp_secret-value" },
                },
              ]);
              // The value goes to the sandbox alone, never into what the host shows or keeps.
              assert.notInclude(JSON.stringify(updates), "ghp_secret-value");
              assert.deepStrictEqual(fake.commands, []);
            }),
        );
      }),
  );

  it.effect("closes the host's request when the approval is answered on the sandbox", () =>
    Effect.gen(function* () {
      const asked = yield* Deferred.make<void>();
      const fake = yield* makeFakeSandbox({
        react: (command, sandbox) =>
          command.type === "launch"
            ? Effect.forEach(
                [
                  event("run.created", run(command.messageId!, "waiting")),
                  event("turn-item.updated", approval("req-1", "command", 5)),
                ],
                sandbox.push,
                { discard: true },
              )
            : Effect.void,
      });
      yield* withBridge({ fake, permissionPending: asked }, ({ acp, withdrawals }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const prompt = yield* say(acp, session.sessionId, "Clean up").pipe(Effect.forkScoped);
          // The computer answers: the sandbox resolves the request and the run finishes.
          yield* Deferred.await(asked);
          yield* fake.push(
            event("runtime-request.updated", { id: "req-1", status: "resolved" } as never),
          );
          yield* fake.push(event("run.updated", run(launchedMessageId(fake), "completed")));
          const result = yield* Fiber.join(prompt);
          assert.strictEqual(result.stopReason, "end_turn");
          assert.deepStrictEqual(withdrawals, [{ sessionId: session.sessionId }]);
          assert.deepStrictEqual(fake.commands, []);
        }),
      );
    }),
  );

  it.effect("follows a message the sandbox already has instead of sending it again", () =>
    Effect.gen(function* () {
      // Sent on the computer: the sandbox has the message and part of its answer already.
      const existing = projection(
        [
          userMessage("msg-0", 100, "run-0", 1_000),
          assistant("old", "Earlier answer", 101, "run-0", 1_001),
          userMessage("msg-desk", 102, "run-desk", 1_002),
          assistant("partial", "Half", 201, "run-desk", 1_003),
        ],
        [
          run("msg-0" as MessageId, "completed", "run-0", 1),
          run("msg-desk" as MessageId, "running", "run-desk", 2),
        ],
      );
      const fake = yield* makeFakeSandbox({ existing });
      yield* withBridge({ fake }, ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const prompt = yield* say(acp, session.sessionId, "From the computer", {
            messageId: "msg-desk",
            creationSource: "web",
          }).pipe(Effect.forkScoped);
          yield* fake.push(
            event("turn-item.updated", assistant("partial", "Half done", 201, "run-desk", 1_004)),
          );
          yield* fake.push(
            event("run.updated", run("msg-desk" as MessageId, "completed", "run-desk", 2)),
          );
          const result = yield* Fiber.join(prompt);
          assert.strictEqual(result.stopReason, "end_turn");
          assert.deepStrictEqual(messages(updates), ["Half", " done"]);
          assert.deepStrictEqual(fake.commands, []);
          assert.deepStrictEqual(fake.launches, []);
        }),
      );
    }),
  );

  it.effect("follows a message queued on the sandbox instead of sending it again", () =>
    Effect.gen(function* () {
      // Sent on the computer while a run worked: queued, with no user message item yet.
      const fake = yield* makeFakeSandbox({
        existing: projection(
          [
            userMessage("msg-0", 100, "run-0", 1_000),
            assistant("old", "Earlier answer", 101, "run-0", 1_001),
          ],
          [
            run("msg-0" as MessageId, "running", "run-0", 1),
            run("msg-queued" as MessageId, "queued", "run-queued", 2),
          ],
        ),
      });
      yield* withBridge({ fake }, ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const prompt = yield* say(acp, session.sessionId, "Then do this", {
            messageId: "msg-queued",
            creationSource: "web",
          }).pipe(Effect.forkScoped);
          yield* Effect.forEach(
            [
              event("run.updated", run("msg-0" as MessageId, "completed", "run-0", 1)),
              event("run.updated", run("msg-queued" as MessageId, "running", "run-queued", 2)),
              event("turn-item.updated", userMessage("msg-queued", 200, "run-queued", 1_002)),
              event(
                "turn-item.updated",
                assistant("next", "Queued answer", 201, "run-queued", 1_003),
              ),
              event("run.updated", run("msg-queued" as MessageId, "completed", "run-queued", 2)),
            ],
            fake.push,
            { discard: true },
          );
          const result = yield* Fiber.join(prompt);
          assert.strictEqual(result.stopReason, "end_turn");
          assert.deepStrictEqual(messages(updates), ["Queued answer"]);
          assert.deepStrictEqual(fake.commands, []);
          assert.deepStrictEqual(fake.launches, []);
        }),
      );
    }),
  );

  it.effect("keeps one sandbox connection for the session's models and all its turns", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({
        existing: projection([], []),
        react: answerWith("Done"),
      });
      let connects = 0;
      yield* withBridge(
        { fake, connect: () => Effect.sync(() => (connects++, fake.session)) },
        ({ acp }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            yield* say(acp, session.sessionId, "One", { messageId: "msg-1" });
            yield* say(acp, session.sessionId, "Two", { messageId: "msg-2" });
            assert.strictEqual(connects, 1);
          }),
      );
    }),
  );

  it.effect("connects again when the session's connection has dropped", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({
        existing: projection([], []),
        react: answerWith("Done"),
      });
      let connects = 0;
      const dropped: SandboxSession = {
        ...fake.session,
        subscribe: () => Stream.fail(new SandboxBridgeError({ message: "socket closed" })),
      };
      yield* withBridge(
        {
          fake,
          // The session's first connection drops before the turn; the next one answers.
          connect: () => Effect.sync(() => (connects++ === 0 ? dropped : fake.session)),
        },
        ({ acp, updates }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            const result = yield* say(acp, session.sessionId, "Still there?");
            assert.strictEqual(result.stopReason, "end_turn");
            assert.strictEqual(connects, 2);
            assert.strictEqual(messages(updates).join(""), "Done");
            assert.deepStrictEqual(messages(updates, "agent_thought_chunk"), []);
          }),
      );
    }),
  );

  it.effect(
    "sends a new message with the host's id, after moving the sandbox to the thread's modes",
    () =>
      Effect.gen(function* () {
        const fake = yield* makeFakeSandbox({
          existing: projection([], [], "approval-required"),
          react: answerWith("Done"),
        });
        yield* withBridge({ fake, runtimeMode: "auto" }, ({ acp }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            yield* say(acp, session.sessionId, "Go", { messageId: "msg-phone" });
            assert.deepStrictEqual(
              fake.commands.map((command) => command.type),
              ["thread.runtime-mode.set", "message.dispatch"],
            );
            const [mode, sent] = fake.commands;
            assert.strictEqual(
              mode?.type === "thread.runtime-mode.set" && mode.runtimeMode,
              "auto",
            );
            assert.strictEqual(
              sent?.type === "message.dispatch" && String(sent.messageId),
              "msg-phone",
            );
          }),
        );
      }),
  );

  it.effect("follows the sandbox's turn when the host continues after a restart", () =>
    Effect.gen(function* () {
      const existing = projection(
        [userMessage("msg-1", 1, "run-1"), assistant("a", "Working", 2)],
        [run("msg-1" as MessageId, "running", "run-1")],
      );
      const fake = yield* makeFakeSandbox({ existing });
      yield* withBridge({ fake }, ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const prompt = yield* say(
            acp,
            session.sessionId,
            "Continue where you left off. Your last turn was cut short.",
            { messageId: "msg-continue", createdBy: "agent", creationSource: "server" },
          ).pipe(Effect.forkScoped);
          yield* fake.push(event("run.updated", run("msg-1" as MessageId, "completed", "run-1")));
          const result = yield* Fiber.join(prompt);
          assert.strictEqual(result.stopReason, "end_turn");
          assert.deepStrictEqual(fake.commands, []);
          assert.deepStrictEqual(messages(updates), ["Working"]);
        }),
      );
    }),
  );

  it.effect("sends images as sandbox attachments and says which files stayed behind", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({ react: answerWith("Nice picture") });
      yield* withBridge({ fake }, ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          yield* acp.agent.prompt({
            sessionId: session.sessionId,
            prompt: [
              {
                type: "text",
                text:
                  "<t3_code_instructions>\nx\n</t3_code_instructions>\n\n<user_request>\nLook at this\n\n" +
                  '[Attached image "shot.png" is saved at: /Users/me/.t3/attachments/a.png]\n\n' +
                  '[Attached file "notes.pdf" is saved at: /Users/me/.t3/attachments/b.pdf]\n</user_request>',
              },
              { type: "image", data: "aGVsbG8=", mimeType: "image/png" },
            ],
          });
          const launch = fake.launches[0]!;
          assert.strictEqual(launch.initialMessage?.text, "Look at this");
          assert.deepStrictEqual(
            launch.initialMessage?.attachments.map((attachment) => attachment.name),
            ["shot.png"],
          );
          const upload = fake.uploads[0] as {
            attachments: ReadonlyArray<{ dataUrl: string; sizeBytes: number }>;
          };
          assert.strictEqual(upload.attachments[0]?.dataUrl, "data:image/png;base64,aGVsbG8=");
          assert.strictEqual(upload.attachments[0]?.sizeBytes, 5);
          assert.include(messages(updates), skippedAttachmentsMessage(["notes.pdf"]));
        }),
      );
    }),
  );

  it.effect(
    "copies the images a reply embeds to the host, once each, and points the reply at them",
    () =>
      Effect.gen(function* () {
        const reply = [
          "Listo:",
          "",
          "![Mockup de Inicio y Correo](/home/user/shots/mesa-v4.png)",
          "",
          'Otra vez <img src="/home/user/shots/mesa-v4.png" alt="mesa"> y ![relativa](shots/logo.png "Logo")',
          "",
          "![perdida](/home/user/shots/gone.png) ![texto](/home/user/notes.txt)",
          "",
          "```md",
          "![en código](/home/user/shots/mesa-v4.png)",
          "```",
          "Fin",
        ].join("\n");
        const split = reply.indexOf("shots/mesa-v4.png)") + 3;
        const fake = yield* makeFakeSandbox({
          images: { "/home/user/shots/mesa-v4.png": "mesa", "shots/logo.png": "logo" },
          react: (command, fake) =>
            command.type === "launch"
              ? Effect.forEach(
                  [
                    event("run.created", run(command.messageId!, "running")),
                    event("turn-item.updated", assistant("answer", reply.slice(0, split), 2)),
                    event("turn-item.updated", assistant("answer", reply, 2)),
                    event("run.updated", run(command.messageId!, "completed")),
                  ],
                  fake.push,
                  { discard: true },
                )
              : Effect.void,
        });
        const saved: Array<string> = [];
        yield* withBridge(
          {
            fake,
            saveImage: (_threadId, image) =>
              Effect.sync(() => {
                const content = new TextDecoder().decode(image.bytes);
                saved.push(content);
                return `/host state/attachments/${content}${image.extension}`;
              }),
          },
          ({ acp, updates }) =>
            Effect.gen(function* () {
              const session = yield* openThread(acp);
              yield* say(acp, session.sessionId, "Haz el mockup");
              const shown = messages(updates).join("");
              assert.strictEqual(
                shown,
                [
                  "Listo:",
                  "",
                  "![Mockup de Inicio y Correo](</host state/attachments/mesa.png>)",
                  "",
                  'Otra vez <img src="/host state/attachments/mesa.png" alt="mesa"> y ![relativa](</host state/attachments/logo.png> "Logo")',
                  "",
                  "![perdida](/home/user/shots/gone.png) ![texto](/home/user/notes.txt)",
                  "",
                  "```md",
                  "![en código](/home/user/shots/mesa-v4.png)",
                  "```",
                  "Fin",
                ].join("\n"),
              );
              assert.deepStrictEqual(saved, ["mesa", "logo"]);
              assert.deepStrictEqual(fake.imageReads, [
                "/home/user/shots/mesa-v4.png",
                "shots/logo.png",
                "/home/user/shots/gone.png",
              ]);
            }),
        );
      }),
  );

  it.effect("turns a sandbox question into a form and sends back the answers", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({
        react: (command, sandbox) => {
          if (command.type === "launch") {
            return Effect.forEach(
              [
                event("run.created", run(command.messageId!, "waiting")),
                event("turn-item.updated", question("req-q")),
              ],
              sandbox.push,
              { discard: true },
            );
          }
          return command.type === "runtime-request.respond"
            ? sandbox.push(event("run.updated", run(launchedMessageId(sandbox), "completed")))
            : Effect.void;
        },
      });
      yield* withBridge(
        { fake, elicitation: { action: "accept", content: { color: "blue" } } },
        ({ acp, elicitations }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            yield* say(acp, session.sessionId, "Paint it");
            const form = elicitations[0]!;
            assert.strictEqual(form.mode, "form");
            const respond = fake.commands[0]!;
            assert.deepStrictEqual(
              respond.type === "runtime-request.respond" ? respond.answers : null,
              { color: "blue" },
            );
          }),
      );
    }),
  );

  it.effect("surfaces a request already pending on the sandbox when a turn starts", () =>
    Effect.gen(function* () {
      const pending = projection([approval("req-old", "command", 1)]);
      const fake = yield* makeFakeSandbox({
        existing: pending,
        react: (command, sandbox) => {
          if (command.type === "message.dispatch") {
            return Effect.forEach(
              [
                { kind: "snapshot", snapshotSequence: 1, projection: pending } as never,
                event("run.created", run(command.messageId!, "queued")),
              ],
              sandbox.push,
              { discard: true },
            );
          }
          if (command.type === "runtime-request.respond") {
            const sent = sandbox.commands[0]!;
            const messageId = sent.type === "message.dispatch" ? sent.messageId : undefined;
            return sandbox.push(event("run.updated", run(messageId!, "completed")));
          }
          return Effect.void;
        },
      });
      yield* withBridge({ fake }, ({ acp, permissions }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          yield* say(acp, session.sessionId, "Go on");
          assert.strictEqual(permissions.length, 1);
          const respond = fake.commands[1]!;
          assert.strictEqual(
            respond.type === "runtime-request.respond" ? String(respond.requestId) : null,
            "req-old",
          );
        }),
      );
    }),
  );

  it.effect.each([true, false])("stops the sandbox turn on cancel (threadStop %s)", (threadStop) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const fake = yield* makeFakeSandbox({
        threadStop,
        react: (command, sandbox) =>
          command.type === "launch"
            ? sandbox
                .push(event("run.created", run(command.messageId!, "running")))
                .pipe(Effect.andThen(Deferred.succeed(started, undefined)))
            : Effect.void,
      });
      yield* withBridge({ fake }, ({ acp }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const prompt = yield* say(acp, session.sessionId, "Long job").pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          yield* acp.agent.cancel({ sessionId: session.sessionId });
          const result = yield* Fiber.join(prompt);
          assert.strictEqual(result.stopReason, "cancelled");
          assert.deepStrictEqual(
            fake.commands.map((command) => command.type),
            [threadStop ? "thread.stop" : "run.interrupt"],
          );
        }),
      );
    }),
  );

  it.effect("leaves the sandbox working when a steer cancels the turn, and steers it in", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const fake = yield* makeFakeSandbox({
        existing: projection(
          [userMessage("msg-0", 100, "run-0")],
          [run("msg-0" as MessageId, "completed", "run-0", 1)],
        ),
        react: (command, sandbox) => {
          if (command.type !== "message.dispatch") return Effect.void;
          if (command.messageId === "msg-1") {
            return sandbox
              .push(event("run.created", run("msg-1" as MessageId, "running", "run-1", 2)))
              .pipe(Effect.andThen(Deferred.succeed(started, undefined)));
          }
          // The sandbox takes the steer into the run that is still working.
          return Effect.forEach(
            [
              event("turn-item.updated", userMessage("msg-steer", 201, "run-1")),
              event("turn-item.updated", assistant("steered", "On it", 202, "run-1")),
              event("run.updated", run("msg-1" as MessageId, "completed", "run-1", 2)),
            ],
            sandbox.push,
            { discard: true },
          );
        },
      });
      yield* withBridge({ fake }, ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const prompt = yield* say(acp, session.sessionId, "Long job", {
            messageId: "msg-1",
          }).pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          yield* acp.agent.cancel({
            sessionId: session.sessionId,
            _meta: { [SANDBOX_BRIDGE_CANCEL_META]: SANDBOX_BRIDGE_STEER_CANCEL },
          });
          assert.strictEqual((yield* Fiber.join(prompt)).stopReason, "cancelled");
          const steer = yield* say(acp, session.sessionId, "Also check the tests", {
            messageId: "msg-steer",
          });
          assert.strictEqual(steer.stopReason, "end_turn");
          assert.deepStrictEqual(messages(updates), ["On it"]);
          // Nothing stopped the run or its tasks; the steer went the way a chat's send goes.
          assert.deepStrictEqual(
            fake.commands.map((command) =>
              command.type === "message.dispatch" ? command.deliveryIntent : command.type,
            ),
            ["auto", "auto"],
          );
        }),
      );
    }),
  );

  it.effect("holds the host thread until the sandbox's thread settles or is stopped", () =>
    Effect.gen(function* () {
      const stopped = yield* Deferred.make<void>();
      const reviewer = (status: string) =>
        ({
          id: "item-reviewer",
          type: "subagent",
          title: "Review the diff",
          status,
          ordinal: 3,
          runId: "run-1",
          childThreadId: "sandbox-child",
          updatedAt: at(),
        }) as never;
      const fake = yield* makeFakeSandbox({
        existing: projection([]),
        react: (command, sandbox) => {
          if (command.type === "thread.stop") {
            return Effect.forEach(
              [
                event("turn-item.updated", reviewer("interrupted")),
                event("run.updated", run(null as never, "interrupted", "run-2", 2)),
              ],
              sandbox.push,
              { discard: true },
            ).pipe(Effect.andThen(Deferred.succeed(stopped, undefined)));
          }
          if (command.type !== "message.dispatch") return Effect.void;
          const ran = run(command.messageId!, "completed");
          sandbox.settle(projection([reviewer("running")], [ran]));
          return Effect.forEach(
            [
              event("run.created", run(command.messageId!, "running")),
              event("turn-item.updated", reviewer("running")),
              event("turn-item.updated", assistant("answer", "Started a reviewer", 2)),
              event("run.updated", ran),
            ],
            sandbox.push,
            { discard: true },
          );
        },
      });
      yield* withBridge({ fake }, ({ acp, rosters }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const result = yield* say(acp, session.sessionId, "Review it in the background");
          // The forwarded run ended, so the host's turn did; the reviewer still runs.
          assert.strictEqual(result.stopReason, "end_turn");
          assert.deepStrictEqual(yield* Queue.take(rosters), {
            sessionId: session.sessionId,
            tasks: [{ taskId: "item-reviewer", kind: "subagent", description: "Review the diff" }],
          });
          // A run the sandbox wakes for on its own holds it too, and only once that ends is it done.
          const wake = run(null as never, "running", "run-2", 2);
          yield* fake.push(event("run.created", wake));
          assert.deepStrictEqual(yield* Queue.take(rosters), {
            sessionId: session.sessionId,
            tasks: [
              {
                taskId: "sandbox-run:run-2",
                kind: "background_task",
                description: "Working on the sandbox",
              },
            ],
          });
          // The host's Stop between turns stops that work on the sandbox, which then settles.
          yield* acp.agent.cancel({ sessionId: session.sessionId });
          yield* Deferred.await(stopped);
          assert.strictEqual(fake.commands.at(-1)?.type, "thread.stop");
          assert.deepStrictEqual(yield* Queue.take(rosters), {
            sessionId: session.sessionId,
            tasks: [],
          });
          assert.strictEqual(yield* Queue.size(rosters), 0);
        }),
      );
    }),
  );

  it.effect("shows a run the sandbox starts itself as a run of the host's, once", () =>
    Effect.gen(function* () {
      const reviewer = (status: string) =>
        ({
          id: "item-reviewer",
          type: "subagent",
          title: "Review the diff",
          status,
          ordinal: 3,
          runId: "run-1",
          childThreadId: "sandbox-child",
          updatedAt: at(),
        }) as never;
      // The reviewer's result, which the sandbox's thread gives itself once the reviewer ends.
      const wakeMessage = {
        id: "wake-1",
        role: "user",
        createdBy: "agent",
        creationSource: "provider",
        text: "Background task completed.",
        notification: {
          source: { kind: "subagent", childThreadId: "sandbox-child" },
          outcome: "completed",
          summary: "Review the diff finished",
        },
      } as never;
      const wakeRun = (status: string) => run("wake-1" as MessageId, status, "run-2", 2);
      const wakeItems = (reply: string) => [
        reviewer("completed"),
        userMessage("wake-1", 300, "run-2", 2_000),
        assistant("wake-answer", reply, 301, "run-2", 2_001),
      ];
      const fake = yield* makeFakeSandbox({
        existing: projection([]),
        react: (command, sandbox) => {
          if (command.type !== "message.dispatch") return Effect.void;
          const ran = run(command.messageId!, "completed");
          sandbox.settle(projection([reviewer("running")], [ran]));
          return Effect.forEach(
            [
              event("run.created", run(command.messageId!, "running")),
              event("turn-item.updated", reviewer("running")),
              event("turn-item.updated", assistant("answer", "Started a reviewer", 2)),
              event("run.updated", ran),
            ],
            sandbox.push,
            { discard: true },
          );
        },
      });
      const subscribed = yield* Queue.unbounded<void>();
      const counted: SandboxSession = {
        ...fake.session,
        subscribe: (id) =>
          Stream.unwrap(
            Queue.offer(subscribed, undefined).pipe(Effect.as(fake.session.subscribe(id))),
          ),
      };
      yield* withBridge(
        { fake, connect: () => Effect.succeed(counted) },
        ({ acp, updates, rosters, wakes }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            const roster = (tasks: ReadonlyArray<unknown>) => ({
              sessionId: session.sessionId,
              tasks,
            });
            yield* say(acp, session.sessionId, "Review it in the background", {
              messageId: "msg-1",
            });
            assert.deepStrictEqual(
              yield* Queue.take(rosters),
              roster([
                { taskId: "item-reviewer", kind: "subagent", description: "Review the diff" },
              ]),
            );
            // The reviewer ends before the run it wakes the sandbox's thread for begins.
            yield* fake.push(event("turn-item.updated", reviewer("completed")));
            assert.deepStrictEqual(yield* Queue.take(rosters), roster([]));
            yield* fake.push(event("message.updated", wakeMessage));
            yield* fake.push(event("run.created", wakeRun("running")));
            // The host is asked for a run with the sandbox's message, its notice in host terms.
            assert.deepStrictEqual(yield* Queue.take(wakes), {
              sessionId: session.sessionId,
              messageId: "wake-1",
              text: "Background task completed.",
              notification: {
                source: { kind: "subagent" },
                outcome: "completed",
                summary: "Review the diff finished",
              },
            });
            assert.deepStrictEqual(
              yield* Queue.take(rosters),
              roster([
                {
                  taskId: "sandbox-run:run-2",
                  kind: "background_task",
                  description: "Working on the sandbox",
                },
              ]),
            );

            // That run's prompt follows the sandbox's run: what it said so far, then the rest.
            const before = updates.length;
            fake.settle(
              projection(wakeItems("Looks"), [
                run("msg-1" as MessageId, "completed"),
                wakeRun("running"),
              ]),
            );
            const wake = yield* say(acp, session.sessionId, "Background task completed.", {
              messageId: "wake-1",
              createdBy: "agent",
              creationSource: "provider",
            }).pipe(Effect.forkScoped);
            // The first turn, the follow after it, and this one.
            yield* Effect.forEach([1, 2, 3], () => Queue.take(subscribed), { discard: true });
            fake.settle(
              projection(wakeItems("Looks good"), [
                run("msg-1" as MessageId, "completed"),
                wakeRun("completed"),
              ]),
            );
            yield* fake.push(
              event(
                "turn-item.updated",
                assistant("wake-answer", "Looks good", 301, "run-2", 2_002),
              ),
            );
            yield* fake.push(event("run.updated", wakeRun("completed")));
            assert.strictEqual((yield* Fiber.join(wake)).stopReason, "end_turn");
            assert.deepStrictEqual(messages(updates.slice(before)), ["Looks", " good"]);
            assert.deepStrictEqual(
              fake.commands.flatMap((command) =>
                command.type === "message.dispatch" ? [String(command.messageId)] : [],
              ),
              ["msg-1"],
            );
            // Following again after it, the bridge does not ask for that run a second time.
            assert.deepStrictEqual(yield* Queue.take(rosters), roster([]));
            assert.strictEqual(yield* Queue.size(wakes), 0);
          }),
      );
    }),
  );

  it.effect("waits out a slow resume and then sends the message it was given", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeSandbox({ react: answerWith("Back up") });
      // A machine start and T3's start at their limits: past the four minutes the bridge waited.
      const upAt = 390_000;
      const startedAt = yield* Clock.currentTimeMillis;
      // Set once the session is open, so only the turn's own attempts count.
      let turnRefused: Deferred.Deferred<void> | undefined;
      yield* withBridge(
        {
          fake,
          reconnectWindow: null,
          connect: () =>
            Clock.currentTimeMillis.pipe(
              Effect.flatMap((now) =>
                now - startedAt < upAt
                  ? Effect.suspend(() =>
                      turnRefused === undefined
                        ? Effect.void
                        : Deferred.succeed(turnRefused, undefined),
                    ).pipe(
                      Effect.andThen(
                        Effect.fail(new SandboxBridgeError({ message: "connection refused" })),
                      ),
                    )
                  : Effect.succeed(fake.session),
              ),
            ),
        },
        ({ acp, updates }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            const refused = yield* Deferred.make<void>();
            turnRefused = refused;
            const prompt = yield* say(acp, session.sessionId, "Pick this up").pipe(
              Effect.forkScoped,
            );
            yield* Deferred.await(refused);
            yield* TestClock.adjust(upAt + 10_000);
            const result = yield* Fiber.join(prompt);
            assert.strictEqual(result.stopReason, "end_turn");
            assert.deepStrictEqual(messages(updates, "agent_thought_chunk"), [
              "Starting sandbox bx_123…",
            ]);
            assert.strictEqual(messages(updates).join(""), "Back up");
            assert.strictEqual(fake.launches[0]?.initialMessage?.text, "Pick this up");
          }),
      );
    }),
  );

  it.effect("ends the turn at once when the host's resume of its sandbox failed", () =>
    Effect.gen(function* () {
      let failure = Option.none<SandboxBridgeResumeFailure>();
      let attempts = 0;
      // Set once the session is open, so only the turn's own attempts count.
      let turnRefused: Deferred.Deferred<void> | undefined;
      yield* withBridge(
        {
          // As long as the bridge waits by default: only the host's word can end the turn early.
          reconnectWindow: null,
          connect: () =>
            Effect.suspend(() => {
              attempts++;
              return turnRefused === undefined
                ? Effect.void
                : Deferred.succeed(turnRefused, undefined);
            }).pipe(
              Effect.andThen(
                Effect.fail(new SandboxBridgeError({ message: "connection refused" })),
              ),
            ),
          findResumeFailure: () => Effect.sync(() => failure),
        },
        ({ acp, updates }) =>
          Effect.gen(function* () {
            const session = yield* openThread(acp);
            // A failure from an earlier message does not end this one.
            failure = Option.some({
              reason: "An earlier attempt failed",
              messageIds: ["message-earlier"],
              failedAt: 0,
            });
            const refused = yield* Deferred.make<void>();
            turnRefused = refused;
            const prompt = yield* say(acp, session.sessionId, "Pick this up", {
              messageId: "message-1",
            }).pipe(Effect.forkScoped);
            yield* Deferred.await(refused);
            const before = attempts;
            yield* TestClock.adjust("30 seconds");
            // Tried every two seconds, so a turn goes out soon after its sandbox answers.
            assert.strictEqual(attempts - before, 15);
            assert.isUndefined(prompt.pollUnsafe());

            failure = Option.some({
              reason: "Boat refused to start bx_123.",
              messageIds: ["message-1"],
              failedAt: 1,
            });
            yield* TestClock.adjust("5 seconds");
            const result = yield* Fiber.join(prompt);
            assert.strictEqual(result.stopReason, "end_turn");
            assert.deepStrictEqual(messages(updates, "agent_thought_chunk"), [
              "Starting sandbox bx_123…",
            ]);
            assert.deepStrictEqual(messages(updates), [
              "Couldn't start sandbox bx_123: Boat refused to start bx_123. Send again to retry.",
            ]);
          }),
      );
    }),
  );

  it.live("waits for a sandbox that is resuming, then reports it unreachable", () =>
    withBridge(
      { connect: () => Effect.fail(new SandboxBridgeError({ message: "connection refused" })) },
      ({ acp, updates }) =>
        Effect.gen(function* () {
          const session = yield* openThread(acp);
          const result = yield* say(acp, session.sessionId, "Hello?");
          assert.strictEqual(result.stopReason, "end_turn");
          assert.deepStrictEqual(messages(updates, "agent_thought_chunk"), [
            "Starting sandbox bx_123…",
          ]);
          assert.match(messages(updates).join(""), /bx_123 is stopped or not answering/u);
          assert.notMatch(messages(updates).join(""), /\bMac\b/u);
        }),
    ),
  );
});

describe("mapping", () => {
  it("holds back only an image reference still arriving on the last line", () => {
    const rows: ReadonlyArray<readonly [string, number]> = [
      ["Plain text, sent at once.", 25],
      ["Wow!", 3],
      ["if (!ready) {", 13],
      ["See ![mockup](/home/user/sh", 4],
      ["See ![mockup](/home/user/shots/a.png) and more", 46],
      ["See ![mockup] and more", 22],
      ["x < y", 5],
      ['Then <img src="/home', 5],
      ['Then <img src="/a.png"> too', 27],
      ["![unfinished\nnext line", 22],
    ];
    for (const [text, end] of rows) assert.strictEqual(releasableEnd(text, 0), end, text);
  });

  it("holds for a monitor between the progress wakes it gives the sandbox, as the sandbox does", () => {
    const monitor = {
      id: "item-monitor",
      type: "command_execution",
      monitor: true,
      input: "tail -f build.log",
      title: null,
      status: "running",
      ordinal: 2,
      runId: "run-1",
      updatedAt: at(),
    };
    const rosterAfter = (wake: boolean) => {
      const work = new SandboxWork();
      work.apply({
        kind: "snapshot",
        snapshotSequence: 0,
        projection: {
          ...projection([monitor], [run("msg-1" as MessageId, "completed", "run-1", 1)]),
          messages: [
            {
              id: "msg-1",
              ...(wake
                ? {
                    notification: {
                      source: { kind: "monitor" },
                      outcome: "updated",
                      summary: "Build log grew",
                    },
                  }
                : {}),
            },
          ],
        },
      } as never);
      return work.tasks;
    };
    assert.deepStrictEqual(rosterAfter(false), [
      { taskId: "item-monitor", kind: "monitor", description: "tail -f build.log" },
    ]);
    assert.deepStrictEqual(rosterAfter(true), [
      { taskId: "item-monitor", kind: "monitor", description: "tail -f build.log", awaited: true },
    ]);
  });

  it("offers one option per kind and always a way to decline", () => {
    const options = approvalOptions([
      { decision: "accept", label: "Yes" },
      { decision: "acceptAlways", label: "Always" },
      { decision: "acceptForSession", label: "Session" },
    ] as never);
    assert.deepStrictEqual(
      options.map((option) => [option.optionId, option.kind]),
      [
        ["accept", "allow_once"],
        ["acceptAlways", "allow_always"],
        ["decline", "reject_once"],
      ],
    );
  });

  it("titles a tool by what it does, not the provider's generic label", () => {
    assert.strictEqual(
      toolTitleOf({
        type: "command_execution",
        title: "Ran command",
        input: "cargo build\n--release",
      } as never),
      "cargo build",
    );
    assert.strictEqual(
      toolTitleOf({ type: "subagent", title: null, prompt: "Review the diff" } as never),
      "Subagent: Review the diff",
    );
    assert.strictEqual(
      toolTitleOf({ type: "dynamic_tool", title: null, toolName: "t3_thread_read" } as never),
      "t3_thread_read",
    );
  });

  it("following a steer, sends only what an answer begun before it adds after the read", () => {
    const text = (id: string, value: string, startedMs: number, updatedMs: number) =>
      ({
        ...(assistant(id, value, startedMs, "run-1", updatedMs) as object),
        startedAt: at(startedMs),
      }) as never;
    // The host showed "Hello wor" before its turn was cancelled for the steer.
    const tracker = new TurnTracker(
      "message-2" as MessageId,
      Option.some(
        projection(
          [
            text("item-before", "Hello wor", 1_000, 3_000),
            userMessage("message-2", 2_000, "run-1", 2_000),
            text("item-after", "Sure", 2_500, 2_500),
          ],
          [run("message-1" as MessageId, "running")],
        ),
      ),
      true,
    );
    assert.deepStrictEqual(tracker.replay(), [
      { _tag: "text", kind: "message", id: "item-after", text: "Sure" },
    ]);
    assert.deepStrictEqual(
      tracker.apply(event("turn-item.updated", text("item-before", "Hello world", 1_000, 4_000))),
      [{ _tag: "text", kind: "message", id: "item-before", text: "ld" }],
    );
  });

  it("updates a command once its input arrives, and names the command for the host", () => {
    const tracker = new TurnTracker("message-1" as MessageId, Option.none());
    const command = (input: string | undefined) =>
      event("turn-item.updated", {
        id: "item-cmd",
        type: "command_execution",
        title: "Ran command",
        ...(input === undefined ? {} : { input }),
        status: "running",
        ordinal: 3,
      });
    assert.deepStrictEqual(tracker.apply(command(undefined)), [
      {
        _tag: "tool",
        id: "item-cmd",
        title: "Ran command",
        kind: "execute",
        status: "in_progress",
      },
    ]);
    assert.deepStrictEqual(tracker.apply(command("touch /tmp/x && rm /tmp/x")), [
      {
        _tag: "tool",
        id: "item-cmd",
        title: "touch /tmp/x && rm /tmp/x",
        kind: "execute",
        status: "in_progress",
        command: "touch /tmp/x && rm /tmp/x",
      },
    ]);
    assert.deepStrictEqual(tracker.apply(command("touch /tmp/x && rm /tmp/x")), []);
  });

  it("reads the decision back, and nothing when the host cancelled", () => {
    assert.strictEqual(
      decisionOf({ outcome: { outcome: "selected", optionId: "decline" } }),
      "decline",
    );
    assert.strictEqual(decisionOf({ outcome: { outcome: "selected", optionId: "bogus" } }), null);
    assert.strictEqual(decisionOf({ outcome: { outcome: "cancelled" } }), null);
  });

  it("keeps only answers to the questions asked", () => {
    assert.deepStrictEqual(
      answersOf([{ id: "a" }, { id: "b" }, { id: "c" }] as never, {
        a: "x",
        b: true,
        c: ["1", 2],
        d: "ignored",
      }),
      { a: "x", b: "true", c: ["1"] },
    );
  });

  it("finds the host's MCP credential over stdio or HTTP", () => {
    assert.deepStrictEqual(hostMcpOf([hostMcpServer] as never), {
      endpoint: "http://127.0.0.1:3773/mcp",
      authorization: "Bearer host-mcp",
    });
    assert.deepStrictEqual(
      hostMcpOf([
        {
          type: "http",
          name: "t3-code",
          url: "http://127.0.0.1:3773/mcp",
          headers: [{ name: "Authorization", value: "Bearer http" }],
        },
      ] as never),
      { endpoint: "http://127.0.0.1:3773/mcp", authorization: "Bearer http" },
    );
    assert.strictEqual(hostMcpOf([]), null);
  });

  it("forwards a prompt's text and embedded text resources", () => {
    assert.strictEqual(
      promptText([
        { type: "text", text: "one" },
        { type: "resource", resource: { uri: "file:///a", text: "two" } },
        { type: "image", data: "", mimeType: "image/png" },
      ] as never),
      "one\n\ntwo",
    );
  });

  it("forwards only the user's request, since the sandbox adds its own instructions", () => {
    const wrapped =
      "<t3_code_instructions>\n## T3 Code interaction mode: Default\n</t3_code_instructions>\n\n" +
      "<user_request>\nList the files\nin this repo\n</user_request>";
    assert.strictEqual(
      promptText([{ type: "text", text: wrapped }] as never),
      "List the files\nin this repo",
    );
    // The first prompt also carries the host's runtime notes in a block of their own.
    assert.strictEqual(
      promptText([
        { type: "text", text: wrapped },
        {
          type: "text",
          text: "<runtime_info>acpRegistry</runtime_info>\n\n<pull_request_linking>\nx\n</pull_request_linking>",
        },
        { type: "text", text: "see the diff" },
      ] as never),
      "List the files\nin this repo\n\nsee the diff",
    );
    assert.strictEqual(
      promptText([{ type: "text", text: "Explain <user_request> tags" }] as never),
      "Explain <user_request> tags",
    );
  });
});
