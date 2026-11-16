/**
 * The sandbox bridge: an ACP agent the host runs for a thread whose work happens on a sandbox's
 * own T3 server. It forwards each prompt to the thread with the same id on the sandbox, streams
 * the sandbox's work back, and turns the sandbox's approvals and questions into ACP requests, so
 * any client of the host (the phone included) can drive the thread.
 */
import {
  type ChatAttachment,
  CommandId,
  CONTINUE_AFTER_INTERRUPTION_PROMPT,
  MessageId,
  type ModelSelection,
  type OrchestrationV2Command,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2Notification,
  type OrchestrationV2PendingBackgroundTask,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ThreadLaunchInput,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadStreamItem,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderApprovalOption,
  type ProviderInteractionMode,
  ProviderRequestKind,
  type RunId,
  type RuntimeMode,
  type RuntimeRequestId,
  SANDBOX_BRIDGE_CANCEL_META,
  SANDBOX_BRIDGE_DEFAULT_MODEL_META,
  SANDBOX_BRIDGE_INSTANCE_ID,
  SANDBOX_BRIDGE_MESSAGE_META,
  SANDBOX_BRIDGE_MODEL,
  SANDBOX_BRIDGE_STEER_CANCEL,
  SANDBOX_BRIDGE_WAKE_METHOD,
  SANDBOX_BRIDGE_WITHDRAW_METHOD,
  SANDBOX_RESUME_WAIT_SECONDS,
  type SecretRequestAnswerInput,
  secretRequestPageUrl,
  type ServerProvider,
  ThreadId,
  type UploadChatAttachment,
  isOrchestrationV2WorkActive,
  SANDBOX_BRIDGE_BACKGROUND_WORK_METHOD,
} from "@t3tools/contracts";
import {
  derivePendingBackgroundWork,
  monitorHoldsWakeCompletion,
} from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as RcMap from "effect/RcMap";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as AcpAgent from "effect-acp/agent";
import * as AcpError from "effect-acp/errors";
import type * as AcpSchema from "effect-acp/schema";

import {
  localImageReferences,
  pointAtHostCopies,
  type RelayedImage,
  releasableEnd,
} from "./images.ts";
import type { SandboxBridgeLink, SandboxBridgeResumeFailure } from "./link.ts";

export class SandboxBridgeError extends Schema.TaggedError<SandboxBridgeError>()(
  "SandboxBridgeError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/** The host thread this bridge process serves, read through the host's MCP endpoint. */
export interface HostThread {
  readonly threadId: ThreadId;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
}

/** The thread-scoped MCP credential the host hands every ACP session. */
export interface HostMcp {
  readonly endpoint: string;
  readonly authorization: string;
}

/** One open connection to a sandbox's T3 server, which a session's turns share. */
export interface SandboxSession {
  readonly threadStop: boolean;
  readonly defaultModelSelection: ModelSelection | null;
  /** The sandbox's providers, whose models the host's picker offers for the sandbox's thread. */
  readonly providers: ReadonlyArray<ServerProvider>;
  /** None when the sandbox has no such thread yet. */
  readonly projection: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<OrchestrationV2ThreadProjection>, SandboxBridgeError>;
  readonly launch: (
    input: OrchestrationV2ThreadLaunchInput,
  ) => Effect.Effect<void, SandboxBridgeError>;
  readonly dispatch: (command: OrchestrationV2Command) => Effect.Effect<void, SandboxBridgeError>;
  /** Saves images on the sandbox for the message that will carry them. */
  readonly persistAttachments: (input: {
    readonly threadId: ThreadId;
    readonly messageId: MessageId;
    readonly attachments: ReadonlyArray<UploadChatAttachment>;
  }) => Effect.Effect<ReadonlyArray<ChatAttachment>, SandboxBridgeError>;
  /** Opens with a snapshot, or ends at once with nothing when the sandbox has no such thread. */
  readonly subscribe: (
    threadId: ThreadId,
  ) => Stream.Stream<OrchestrationV2ThreadStreamItem, SandboxBridgeError>;
  readonly answerSecret: (
    input: SecretRequestAnswerInput,
  ) => Effect.Effect<void, SandboxBridgeError>;
  /** An image file on the sandbox, at most `RELAYED_IMAGE_MAX_BYTES` and of a type T3 shows. */
  readonly readImage: (
    threadId: ThreadId,
    path: string,
  ) => Effect.Effect<RelayedImage, SandboxBridgeError>;
}

/** A secret the sandbox's agent asked for, put to the user on the host thread. */
export interface HostSecretRequest {
  readonly clientRequestId: string;
  readonly label: string;
  readonly reason: string;
  readonly url?: string;
  readonly placeholder?: string;
}

export type HostSecretAnswer =
  | { readonly status: "saved"; readonly value: Redacted.Redacted<string> }
  | { readonly status: "declined" | "cancelled" | "timed_out" };

export interface SandboxBridgePorts {
  readonly readHostThread: (mcp: HostMcp) => Effect.Effect<HostThread, SandboxBridgeError>;
  readonly findLink: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<SandboxBridgeLink>, SandboxBridgeError>;
  readonly connect: (
    link: SandboxBridgeLink,
  ) => Effect.Effect<SandboxSession, SandboxBridgeError, Scope.Scope>;
  /**
   * How long a turn waits for an unreachable sandbox, such as one the host is resuming. By default
   * as long as the host's slowest resume, so a message sent to a stopped sandbox goes out once it is up.
   */
  readonly reconnectWindow?: Duration.Input;
  readonly reconnectSpacing?: Duration.Input;
  /** The host's word that its resume of this thread's sandbox failed; none while it may still come up. */
  readonly findResumeFailure?: (
    threadId: ThreadId,
  ) => Effect.Effect<Option.Option<SandboxBridgeResumeFailure>, SandboxBridgeError>;
  /**
   * Asks with the host's own secret card on the host thread, which keeps only the request's status;
   * interrupting the ask closes the card.
   */
  readonly requestSecret?: (
    mcp: HostMcp,
    request: HostSecretRequest,
  ) => Effect.Effect<HostSecretAnswer, SandboxBridgeError>;
  /**
   * Keeps an image from the sandbox on the host for the thread, the same bytes at the same path,
   * and answers that path. Without it, replies keep their sandbox paths.
   */
  readonly saveImage?: (
    threadId: ThreadId,
    image: RelayedImage,
  ) => Effect.Effect<string, SandboxBridgeError>;
}

export const PROBE_CLIENT_NAME = "t3-code-provider-test";
export const SANDBOX_SESSION_PREFIX = "sandbox:";
const DRY_RUN_SESSION_PREFIX = "sandbox-dry-run:";
const MODEL_OPTION_VALUE = SANDBOX_BRIDGE_MODEL;
// One short read of the sandbox's models when a session opens; a stopped sandbox must not stall it.
const CATALOG_READ_TIMEOUT = "5 seconds";
// The reply waits for an image's copy; past this it goes out with the sandbox's path.
const IMAGE_COPY_TIMEOUT = "10 seconds";
// A session's connection outlives its turn this long, so a follow-up skips the ticket and handshake.
const CONNECTION_IDLE_TTL = "5 minutes";

/** Sandbox chats are made only where the sandbox configs live; other clients continue them. */
export const NOT_LINKED_MESSAGE =
  "Sandbox chats start from T3 Code on your computer (Run on → Sandbox). After that you can keep the conversation going here.";
/** A thread the bridge served whose sandbox is gone: deleted, so its link went with it. */
export const SANDBOX_GONE_MESSAGE =
  "This chat's sandbox was deleted, so this message wasn't sent. The conversation stays here to read; to keep working, start a new chat from T3 Code on your computer.";
export const unreachableMessage = (link: SandboxBridgeLink) =>
  `Sandbox ${link.sandboxId || link.label} is stopped or not answering. Resume it from T3 Code on your computer, then send again.`;
export const waitingThought = (link: SandboxBridgeLink) =>
  `Starting sandbox ${link.sandboxId || link.label}…`;
export const resumeFailedMessage = (link: SandboxBridgeLink, reason: string) =>
  `Couldn't start sandbox ${link.sandboxId || link.label}: ${reason.trim().replace(/[.\s]+$/u, "") || "the resume failed"}. Send again to retry.`;
export const skippedAttachmentsMessage = (names: ReadonlyArray<string>) =>
  `${names.map((name) => `"${name}"`).join(", ")} ${names.length === 1 ? "wasn't" : "weren't"} sent to the sandbox. Only images can be sent from here; attach other files from T3 Code on your computer.`;

const modelConfigOptions: ReadonlyArray<AcpSchema.SessionConfigOption> = [
  {
    configId: "model",
    name: "Model",
    category: "model",
    type: "select",
    currentValue: MODEL_OPTION_VALUE,
    options: [{ value: MODEL_OPTION_VALUE, name: "Sandbox agent" }],
  },
];

const choiceOf = (
  descriptor: Extract<
    NonNullable<
      NonNullable<ServerProvider["models"][number]["capabilities"]>["optionDescriptors"]
    >[number],
    { readonly type: "select" }
  >,
  picked: unknown,
) =>
  (typeof picked === "string" && descriptor.options.some((choice) => choice.id === picked)
    ? picked
    : undefined) ??
  descriptor.currentValue ??
  descriptor.options.find((choice) => choice.isDefault === true)?.id ??
  descriptor.options[0]?.id;

/**
 * The models of the provider a sandbox's thread runs on, with that model's options, for the host's
 * picker. The bridge's `sandbox` value stays listed ("whatever the sandbox has"); the model that is
 * goes in the option's `_meta` for the host to mark. Null when the sandbox lacks the provider.
 */
export const sandboxConfigOptions = (
  providers: ReadonlyArray<ServerProvider>,
  selection: ModelSelection | null,
  sandboxDefault: ModelSelection | null,
): ReadonlyArray<AcpSchema.SessionConfigOption> | null => {
  if (selection === null) return null;
  const provider = providers.find((candidate) => candidate.instanceId === selection.instanceId);
  const models = (provider?.models ?? []).filter((model) => model.isLegacy !== true);
  if (models.length === 0) return null;
  const current = models.find((model) => model.slug === selection.model);
  const picked = new Map((selection.options ?? []).map((option) => [option.id, option.value]));
  const defaultModel =
    models.find(
      (model) =>
        model.slug === sandboxDefault?.model && sandboxDefault.instanceId === selection.instanceId,
    ) ?? models.find((model) => model.isDefault === true);
  const options: Array<AcpSchema.SessionConfigOption> = [
    {
      configId: "model",
      name: "Model",
      category: "model",
      type: "select",
      currentValue: current?.slug ?? MODEL_OPTION_VALUE,
      options: [
        ...models.map((model) => ({ value: model.slug, name: model.name })),
        { value: MODEL_OPTION_VALUE, name: "Sandbox default" },
      ],
      ...(defaultModel === undefined
        ? {}
        : { _meta: { [SANDBOX_BRIDGE_DEFAULT_MODEL_META]: defaultModel.slug } }),
    },
  ];
  for (const descriptor of (current ?? models[0])?.capabilities?.optionDescriptors ?? []) {
    if (descriptor.type === "boolean") {
      const value = picked.get(descriptor.id);
      options.push({
        configId: descriptor.id,
        name: descriptor.label,
        type: "boolean",
        currentValue: typeof value === "boolean" ? value : (descriptor.currentValue ?? false),
      });
      continue;
    }
    const currentValue = choiceOf(descriptor, picked.get(descriptor.id));
    if (currentValue === undefined) continue;
    options.push({
      configId: descriptor.id,
      name: descriptor.label,
      ...(descriptor.role === "reasoning" ? { category: "thought_level" } : {}),
      type: "select",
      currentValue,
      options: descriptor.options.map((choice) => ({ value: choice.id, name: choice.label })),
    });
  }
  return options;
};

export const sameConfigOptions = (
  left: ReadonlyArray<AcpSchema.SessionConfigOption>,
  right: ReadonlyArray<AcpSchema.SessionConfigOption>,
) => JSON.stringify(left) === JSON.stringify(right);

/** The options as a client's `session/set_config_option` leaves them. */
export const withConfigValue = (
  options: ReadonlyArray<AcpSchema.SessionConfigOption>,
  configId: string,
  value: unknown,
): ReadonlyArray<AcpSchema.SessionConfigOption> =>
  options.map((option) =>
    option.configId !== configId
      ? option
      : option.type === "boolean" && typeof value === "boolean"
        ? { ...option, currentValue: value }
        : option.type === "select" && typeof value === "string"
          ? { ...option, currentValue: value }
          : option,
  );

type OptionDescriptors = NonNullable<
  NonNullable<ServerProvider["models"][number]["capabilities"]>["optionDescriptors"]
>;

const defaultOptionValue = (descriptor: OptionDescriptors[number]) =>
  descriptor.type === "boolean"
    ? (descriptor.currentValue ?? false)
    : choiceOf(descriptor, undefined);

// Options as the model runs them: one stated at its default reads as one left out.
const optionKey = (options: ModelSelection["options"], descriptors: OptionDescriptors) =>
  JSON.stringify(
    [...(options ?? [])]
      .filter((option) => {
        const descriptor = descriptors.find((candidate) => candidate.id === option.id);
        return descriptor === undefined || defaultOptionValue(descriptor) !== option.value;
      })
      .map((option) => [option.id, option.value] as const)
      .toSorted(([left], [right]) => left.localeCompare(right)),
  );

/**
 * The model to give the sandbox's thread for a message sent from the host: the one picked there,
 * on the provider the sandbox's thread runs on, with the sandbox's own value for any option the
 * host leaves out. Null to keep the sandbox's own, as for the bridge's `sandbox` value, a model the
 * sandbox does not list, or one it already runs the same way.
 */
export const sandboxSelectionFor = (
  host: ModelSelection | null,
  sandbox: ModelSelection | null,
  providers: ReadonlyArray<ServerProvider>,
): ModelSelection | null => {
  if (
    host === null ||
    sandbox === null ||
    host.instanceId !== SANDBOX_BRIDGE_INSTANCE_ID ||
    host.model === MODEL_OPTION_VALUE
  ) {
    return null;
  }
  const provider = providers.find((candidate) => candidate.instanceId === sandbox.instanceId);
  const model = provider?.models.find((candidate) => candidate.slug === host.model);
  if (model === undefined) return null;
  const descriptors = model.capabilities?.optionDescriptors ?? [];
  const hostOptions = host.options ?? [];
  const options = [
    ...hostOptions,
    ...(sandbox.options ?? []).filter(
      (option) =>
        !hostOptions.some((stated) => stated.id === option.id) &&
        descriptors.some((descriptor) => descriptor.id === option.id),
    ),
  ];
  if (
    host.model === sandbox.model &&
    optionKey(options, descriptors) === optionKey(sandbox.options, descriptors)
  ) {
    return null;
  }
  return {
    instanceId: sandbox.instanceId,
    model: host.model,
    ...(options.length === 0 ? {} : { options }),
  } as ModelSelection;
};

/** What a sandbox thread runs on when nothing names its model. */
export const CLAUDE_FALLBACK_MODEL: ModelSelection = {
  instanceId: "claudeAgent",
  model: "claude-opus-5-5",
} as ModelSelection;

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

// The host's ACP adapter wraps a prompt in T3 Code's instructions and adds its runtime notes; the
// sandbox's own server adds both again for its agent.
const HOST_INSTRUCTIONS_WRAPPER =
  /^\s*<t3_code_instructions>[\s\S]*?<\/t3_code_instructions>\s*<user_request>\n?([\s\S]*?)\n?<\/user_request>\s*$/u;
const HOST_RUNTIME_NOTES = /^\s*<runtime_info>/u;
// The host lists each attachment by its path on the host, which means nothing on the sandbox.
const HOST_ATTACHMENT_LINE = /^\[Attached (\S+) "(.+)" is (?:saved at: .*|unavailable: .*)\]$/u;

/** The text a prompt carries, without the host's notes or its attachment paths. */
export const promptText = (blocks: ReadonlyArray<AcpSchema.ContentBlock>): string =>
  promptContent(blocks).text;

export interface PromptContent {
  readonly text: string;
  readonly images: ReadonlyArray<UploadChatAttachment>;
  /** Attachments the bridge cannot carry, by name. */
  readonly skipped: ReadonlyArray<string>;
}

const base64Bytes = (data: string) => {
  const padding = data.endsWith("==") ? 2 : data.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((data.length * 3) / 4) - padding);
};

const IMAGE_EXTENSIONS: Readonly<Record<string, string>> = {
  "image/png": "png",
  "image/jpeg": "jpg",
  "image/gif": "gif",
  "image/webp": "webp",
};

export const promptContent = (blocks: ReadonlyArray<AcpSchema.ContentBlock>): PromptContent => {
  const texts: Array<string> = [];
  const attached: Array<{ readonly type: string; readonly name: string }> = [];
  const imageBlocks: Array<{ readonly data: string; readonly mimeType: string }> = [];
  for (const block of blocks) {
    switch (block.type) {
      case "text": {
        const raw = String(block.text);
        if (HOST_RUNTIME_NOTES.test(raw)) break;
        const lines = (HOST_INSTRUCTIONS_WRAPPER.exec(raw)?.[1] ?? raw).split("\n");
        const kept = lines.filter((line) => {
          const match = HOST_ATTACHMENT_LINE.exec(line.trim());
          if (match === null) return true;
          attached.push({ type: match[1]!, name: match[2]! });
          return false;
        });
        texts.push(kept.join("\n").trim());
        break;
      }
      case "resource_link":
        texts.push(String(block.uri));
        break;
      case "resource": {
        const resource: unknown = block.resource;
        if (isRecord(resource) && typeof resource.text === "string") texts.push(resource.text);
        break;
      }
      case "image":
        imageBlocks.push({ data: String(block.data), mimeType: String(block.mimeType) });
        break;
      default:
        break;
    }
  }
  const imageNames = attached.filter((entry) => entry.type === "image").map((entry) => entry.name);
  const images = imageBlocks.map((image, index): UploadChatAttachment => ({
    type: "image",
    name: imageNames[index] ?? `image-${index + 1}.${IMAGE_EXTENSIONS[image.mimeType] ?? "png"}`,
    mimeType: image.mimeType,
    sizeBytes: base64Bytes(image.data),
    dataUrl: `data:${image.mimeType};base64,${image.data}`,
  }));
  // The host sends an image block only for types agents read; any other attachment stays behind.
  const skipped = [
    ...imageNames.slice(imageBlocks.length),
    ...attached.filter((entry) => entry.type !== "image").map((entry) => entry.name),
  ];
  return {
    text: texts.filter((text) => text.length > 0).join("\n\n"),
    images,
    skipped,
  };
};

const isApprovalKind = Schema.is(ProviderRequestKind);

const TERMINAL_RUN_STATUSES: ReadonlySet<OrchestrationV2Run["status"]> = new Set([
  "completed",
  "interrupted",
  "failed",
  "cancelled",
  "rolled_back",
]);

const isTerminalRun = (run: OrchestrationV2Run) => TERMINAL_RUN_STATUSES.has(run.status);

/** Whether `modeSyncCommands` leaves a move to full access for after the sandbox's run ends. */
export const holdsRuntimeMode = (
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs">,
  modes: { readonly runtimeMode: RuntimeMode },
) =>
  modes.runtimeMode === "full-access" &&
  projection.thread.runtimeMode !== modes.runtimeMode &&
  projection.runs.some((run) => !isTerminalRun(run));

/**
 * Puts a sandbox's thread in the host thread's modes. Full access grants its waiting approvals
 * (not questions) now, but its runtime mode waits for the run to end: a sandbox cuts its agent's
 * run short on that change.
 */
export const modeSyncCommands = (
  projection: OrchestrationV2ThreadProjection,
  modes: { readonly runtimeMode: RuntimeMode; readonly interactionMode: ProviderInteractionMode },
  commandId: () => CommandId,
): ReadonlyArray<OrchestrationV2Command> => {
  const threadId = projection.thread.id;
  return [
    ...(projection.thread.runtimeMode === modes.runtimeMode || holdsRuntimeMode(projection, modes)
      ? []
      : [
          {
            type: "thread.runtime-mode.set" as const,
            commandId: commandId(),
            threadId,
            runtimeMode: modes.runtimeMode,
          },
        ]),
    ...(projection.thread.interactionMode === modes.interactionMode
      ? []
      : [
          {
            type: "thread.interaction-mode.set" as const,
            commandId: commandId(),
            threadId,
            interactionMode: modes.interactionMode,
          },
        ]),
    ...(modes.runtimeMode === "full-access"
      ? projection.runtimeRequests
          .filter((request) => request.status === "pending" && isApprovalKind(request.kind))
          .map((request) => ({
            type: "runtime-request.respond" as const,
            commandId: commandId(),
            threadId,
            requestId: request.id,
            decision: "accept" as const,
          }))
      : []),
  ];
};

/** The T3 message a host prompt carries, from the `_meta` the host's ACP adapter sets. */
export interface PromptMessage {
  readonly messageId: MessageId;
  readonly createdBy: string;
  readonly creationSource: string;
}

export const promptMessageOf = (meta: unknown): PromptMessage | null => {
  const message = isRecord(meta) ? meta[SANDBOX_BRIDGE_MESSAGE_META] : undefined;
  if (!isRecord(message) || typeof message.messageId !== "string" || message.messageId === "") {
    return null;
  }
  return {
    messageId: MessageId.make(message.messageId),
    createdBy: typeof message.createdBy === "string" ? message.createdBy : "",
    creationSource: typeof message.creationSource === "string" ? message.creationSource : "",
  };
};

/**
 * The host's own "continue" after it restarted or a usage limit lifted. The sandbox never stopped,
 * so forwarding it would make its agent repeat work; the bridge follows the sandbox's turn instead.
 */
export const isHostContinuation = (text: string, message: PromptMessage | null) =>
  text.startsWith(CONTINUE_AFTER_INTERRUPTION_PROMPT) &&
  (message === null || (message.createdBy === "agent" && message.creationSource === "server"));

/**
 * Never a read kind: the host's ACP policy allows reads without asking, and every request the
 * sandbox raises needs the user's answer.
 */
export const approvalToolKind = (requestKind: string): AcpSchema.ToolKind =>
  requestKind === "file-change" ? "edit" : "execute";

const DEFAULT_APPROVAL_OPTIONS: ReadonlyArray<ProviderApprovalOption> = [
  { decision: "accept", label: "Allow" },
  { decision: "acceptForSession", label: "Allow for this session" },
  { decision: "decline", label: "Decline" },
] as ReadonlyArray<ProviderApprovalOption>;

const ACP_OPTION_KIND: Partial<Record<ProviderApprovalDecision, AcpSchema.PermissionOptionKind>> = {
  accept: "allow_once",
  acceptForSession: "allow_always",
  acceptAlways: "allow_always",
  decline: "reject_once",
};

/** One ACP option per kind, since the host answers by kind; the option id is the T3 decision. */
export const approvalOptions = (
  offered: ReadonlyArray<ProviderApprovalOption> | undefined,
): ReadonlyArray<AcpSchema.PermissionOption> => {
  const options: Array<AcpSchema.PermissionOption> = [];
  for (const option of offered !== undefined && offered.length > 0
    ? offered
    : DEFAULT_APPROVAL_OPTIONS) {
    const kind = ACP_OPTION_KIND[option.decision];
    if (kind === undefined || options.some((existing) => existing.kind === kind)) continue;
    options.push({ optionId: option.decision, name: option.label, kind });
  }
  if (!options.some((option) => option.kind === "reject_once")) {
    options.push({ optionId: "decline", name: "Decline", kind: "reject_once" });
  }
  return options;
};

const DECISIONS: ReadonlySet<string> = new Set([
  "accept",
  "acceptForSession",
  "acceptAlways",
  "decline",
]);

/** The T3 decision an ACP permission answer picked, or null when the host cancelled it. */
export const decisionOf = (
  response: AcpSchema.RequestPermissionResponse,
): ProviderApprovalDecision | null =>
  response.outcome.outcome === "selected" &&
  typeof response.outcome.optionId === "string" &&
  DECISIONS.has(response.outcome.optionId)
    ? (response.outcome.optionId as ProviderApprovalDecision)
    : null;

const APPROVAL_FIELD = "decision";

/**
 * An approval asked as a one-question form, for a host thread that is not Supervised: its own
 * policy would answer a permission request without the user, and the sandbox needs them.
 */
export const approvalForm = (
  options: ReadonlyArray<AcpSchema.PermissionOption>,
): AcpSchema.ElicitationSchema => ({
  type: "object",
  properties: {
    [APPROVAL_FIELD]: {
      type: "string" as const,
      title: "Allow this?",
      enum: options.map((option) => option.name),
    },
  },
  required: [APPROVAL_FIELD],
});

export const formDecisionOf = (
  options: ReadonlyArray<AcpSchema.PermissionOption>,
  content: unknown,
): ProviderApprovalDecision | null => {
  const picked = isRecord(content) ? content[APPROVAL_FIELD] : undefined;
  const option = options.find((candidate) => candidate.name === picked);
  return option !== undefined && DECISIONS.has(option.optionId)
    ? (option.optionId as ProviderApprovalDecision)
    : null;
};

const optionValue = (option: OrchestrationV2UserInputQuestion["options"][number]) =>
  option.value ?? option.label;

/** A form whose fields are the sandbox's questions; option values come back as the answers. */
export const questionForm = (
  questions: ReadonlyArray<OrchestrationV2UserInputQuestion>,
): AcpSchema.ElicitationSchema => ({
  type: "object",
  properties: Object.fromEntries(
    questions.map((question) => [
      question.id,
      {
        type: "string" as const,
        title: question.header,
        description: question.question,
        ...(question.options.length > 0 && question.multiSelect !== true
          ? { enum: question.options.map(optionValue) }
          : {}),
      },
    ]),
  ),
  required: questions.filter((question) => question.required !== false).map((q) => q.id),
});

export const answersOf = (
  questions: ReadonlyArray<OrchestrationV2UserInputQuestion>,
  content: unknown,
): Record<string, string | ReadonlyArray<string>> => {
  const answers: Record<string, string | ReadonlyArray<string>> = {};
  for (const question of questions) {
    const value = isRecord(content) ? content[question.id] : undefined;
    if (typeof value === "string") answers[question.id] = value;
    else if (typeof value === "number" || typeof value === "boolean") {
      answers[question.id] = String(value);
    } else if (Array.isArray(value)) {
      answers[question.id] = value.filter((entry): entry is string => typeof entry === "string");
    }
  }
  return answers;
};

type ToolItem = Extract<
  OrchestrationV2TurnItem,
  {
    readonly type:
      | "command_execution"
      | "file_change"
      | "file_search"
      | "web_search"
      | "dynamic_tool"
      | "subagent";
  }
>;
type ApprovalItem = Extract<OrchestrationV2TurnItem, { readonly type: "approval_request" }>;
type QuestionItem = Extract<OrchestrationV2TurnItem, { readonly type: "user_input_request" }>;
type SecretItem = Extract<OrchestrationV2TurnItem, { readonly type: "secret_request" }>;
type UserMessageItem = Extract<OrchestrationV2TurnItem, { readonly type: "user_message" }>;

const isToolItem = (item: OrchestrationV2TurnItem): item is ToolItem =>
  item.type === "command_execution" ||
  item.type === "file_change" ||
  item.type === "file_search" ||
  item.type === "web_search" ||
  item.type === "dynamic_tool" ||
  item.type === "subagent";

const toolKindOf = (item: ToolItem): AcpSchema.ToolKind => {
  switch (item.type) {
    case "command_execution":
      return "execute";
    case "file_change":
      return "edit";
    case "file_search":
      return "search";
    case "web_search":
      return "fetch";
    case "dynamic_tool":
    case "subagent":
      return "other";
  }
};

const TITLE_LIMIT = 160;
// A summary line for the phone: the command or target itself, not the provider's generic label.
const summary = (text: string | null | undefined) => {
  const line = (text ?? "").trim().split("\n")[0]?.trim() ?? "";
  return line.length > TITLE_LIMIT ? `${line.slice(0, TITLE_LIMIT - 1)}…` : line;
};

export const toolTitleOf = (item: ToolItem): string => {
  switch (item.type) {
    case "command_execution":
      return summary(item.input) || summary(item.title) || "Run a command";
    case "file_change":
      return `Edit ${item.fileName}`;
    case "file_search":
      return summary(item.pattern) || summary(item.title) || "Search files";
    case "web_search":
      return summary(item.patterns?.join(", ")) || summary(item.title) || "Search the web";
    case "dynamic_tool":
      return summary(item.title) || summary(item.toolName) || "Use a tool";
    case "subagent":
      return `Subagent: ${summary(item.title) || summary(item.prompt) || "working"}`;
  }
};

const toolStatusOf = (status: OrchestrationV2TurnItem["status"]): AcpSchema.ToolCallStatus => {
  switch (status) {
    case "completed":
      return "completed";
    case "failed":
    case "cancelled":
    case "interrupted":
      return "failed";
    case "running":
    case "waiting":
      return "in_progress";
    default:
      return "pending";
  }
};

const toolSentKey = (item: ToolItem) => `${toolStatusOf(item.status)}\u0000${toolTitleOf(item)}`;

export type TurnAction =
  | {
      readonly _tag: "text";
      readonly kind: "message" | "thought";
      readonly id: string;
      readonly text: string;
    }
  | {
      readonly _tag: "tool";
      readonly id: string;
      readonly title: string;
      readonly kind: AcpSchema.ToolKind;
      readonly status: AcpSchema.ToolCallStatus;
      /** The command a command tool runs, where the host reads it to show what ran. */
      readonly command?: string;
    }
  | { readonly _tag: "approval"; readonly item: ApprovalItem }
  | { readonly _tag: "question"; readonly item: QuestionItem }
  | { readonly _tag: "secret"; readonly item: SecretItem };

/**
 * Follows a sandbox thread for one host turn, until the run carrying the message ends. Text and
 * tools last touched before the turn began (the newest `baseline` item, or the message when
 * `replay`) count as sent: the first read is a window, and a later snapshot may hold what it left out.
 * When `replay`, items begun before the message count as sent up to what the baseline holds.
 */
export class TurnTracker {
  private readonly runs = new Map<string, OrchestrationV2Run>();
  private readonly items = new Map<string, OrchestrationV2TurnItem>();
  private readonly requests = new Map<string, OrchestrationV2RuntimeRequest>();
  private readonly sent = new Map<string, number>();
  // The status and title last sent for each tool: a command's input often arrives after it starts.
  private readonly toolSent = new Map<string, string>();
  private readonly handled = new Set<string>();
  private readonly settledRequests = new Map<string, Deferred.Deferred<void>>();
  private readonly messageId: MessageId;
  private readonly since: number | undefined;

  constructor(
    messageId: MessageId,
    baseline: Option.Option<OrchestrationV2ThreadProjection>,
    replay = false,
  ) {
    this.messageId = messageId;
    let since: number | undefined;
    if (Option.isSome(baseline)) {
      for (const run of baseline.value.runs) this.runs.set(run.id, run);
      for (const request of baseline.value.runtimeRequests) this.requests.set(request.id, request);
      for (const item of baseline.value.turnItems) {
        this.items.set(item.id, item);
        if (replay) continue;
        this.markSent(item);
        since = Math.max(since ?? 0, DateTime.toEpochMillis(item.updatedAt));
      }
    }
    const message = replay ? this.userMessage : undefined;
    this.since =
      message === undefined
        ? since
        : DateTime.toEpochMillis(message.startedAt ?? message.updatedAt);
    // Following a steer, an answer begun before the message was streamed by the host's earlier
    // turn: only what it adds after this read is new.
    if (message !== undefined && this.since !== undefined) {
      for (const item of this.items.values()) {
        if (item.startedAt != null && DateTime.toEpochMillis(item.startedAt) < this.since) {
          this.markSent(item);
        }
      }
    }
  }

  /** The actions the baseline owes the host: whatever it holds from the message on. */
  replay(): ReadonlyArray<TurnAction> {
    return this.actionsFor([...this.items.values()]);
  }

  apply(item: OrchestrationV2ThreadStreamItem): ReadonlyArray<TurnAction> {
    if (item.kind === "snapshot") {
      for (const run of item.projection.runs) this.runs.set(run.id, run);
      for (const request of item.projection.runtimeRequests) this.requests.set(request.id, request);
      for (const turnItem of item.projection.turnItems) this.items.set(turnItem.id, turnItem);
      this.settleAnswered();
      return this.actionsFor(item.projection.turnItems);
    }
    if (item.kind !== "event") return [];
    const event = item.event;
    switch (event.type) {
      case "run.created":
      case "run.updated":
        this.runs.set(event.payload.id, event.payload);
        this.settleAnswered();
        return [];
      case "runtime-request.updated":
        this.requests.set(event.payload.id, event.payload);
        this.settleAnswered();
        return [];
      case "turn-item.updated":
        this.items.set(event.payload.id, event.payload);
        this.settleAnswered();
        return this.actionsFor([event.payload]);
      default:
        return [];
    }
  }

  private get userMessage(): UserMessageItem | undefined {
    for (const item of this.items.values()) {
      if (item.type === "user_message" && item.messageId === this.messageId) return item;
    }
    return undefined;
  }

  /** The run carrying the message: the one it started, or the one it was steered into. */
  get run(): OrchestrationV2Run | undefined {
    for (const run of this.runs.values()) {
      if (run.userMessageId === this.messageId) return run;
    }
    const runId = this.userMessage?.runId;
    return runId == null ? undefined : this.runs.get(runId);
  }

  get done(): boolean {
    const run = this.run;
    return run !== undefined && isTerminalRun(run);
  }

  /** The newest run still working, for an interrupt when the forwarded one is not known yet. */
  get activeRunId(): RunId | undefined {
    return this.run?.id ?? [...this.runs.values()].findLast((run) => !isTerminalRun(run))?.id;
  }

  markHandled(requestId: RuntimeRequestId): void {
    this.handled.add(requestId);
  }

  /** Completes once the request (or secret card) no longer waits on the sandbox, whoever answered it. */
  answered(requestId: string): Deferred.Deferred<void> {
    let settled = this.settledRequests.get(requestId);
    if (settled === undefined) {
      settled = Deferred.makeUnsafe<void>();
      this.settledRequests.set(requestId, settled);
      this.settleAnswered();
    }
    return settled;
  }

  private stillWaiting(requestId: string): boolean {
    if (this.done) return false;
    const card = this.items.get(requestId);
    if (card?.type === "secret_request") return card.secretStatus === "pending";
    const request = this.requests.get(requestId);
    if (request !== undefined) return request.status === "pending";
    for (const item of this.items.values()) {
      if (
        (item.type === "approval_request" || item.type === "user_input_request") &&
        item.requestId === requestId
      ) {
        return item.status === "pending" || item.status === "waiting" || item.status === "running";
      }
    }
    return true;
  }

  private settleAnswered(): void {
    for (const [requestId, settled] of this.settledRequests) {
      if (!this.stillWaiting(requestId)) {
        Deferred.doneUnsafe(settled, Effect.void);
        this.settledRequests.delete(requestId);
      }
    }
  }

  private markSent(item: OrchestrationV2TurnItem): void {
    if (item.type === "assistant_message" || item.type === "reasoning") {
      this.sent.set(item.id, item.text.length);
    } else if (item.type === "proposed_plan") {
      this.sent.set(item.id, item.markdown.length);
    } else if (isToolItem(item)) {
      this.toolSent.set(item.id, toolSentKey(item));
    }
  }

  /** Text or a tool this turn has not seen whose last change predates it. */
  private isHistory(item: OrchestrationV2TurnItem): boolean {
    return (
      this.since !== undefined &&
      (item.type === "assistant_message" ||
        item.type === "reasoning" ||
        item.type === "proposed_plan" ||
        isToolItem(item)) &&
      !this.sent.has(item.id) &&
      !this.toolSent.has(item.id) &&
      DateTime.toEpochMillis(item.updatedAt) < this.since
    );
  }

  private isPending(requestId: RuntimeRequestId, status: OrchestrationV2TurnItem["status"]) {
    if (this.handled.has(requestId)) return false;
    const request = this.requests.get(requestId);
    if (request !== undefined) return request.status === "pending";
    return status === "pending" || status === "waiting" || status === "running";
  }

  private actionsFor(items: ReadonlyArray<OrchestrationV2TurnItem>): ReadonlyArray<TurnAction> {
    const actions: Array<TurnAction> = [];
    for (const item of [...items].sort((a, b) => a.ordinal - b.ordinal)) {
      if (this.isHistory(item)) {
        this.markSent(item);
        continue;
      }
      if (
        item.type === "assistant_message" ||
        item.type === "reasoning" ||
        item.type === "proposed_plan"
      ) {
        const text = item.type === "proposed_plan" ? item.markdown : item.text;
        const sent = this.sent.get(item.id) ?? 0;
        if (text.length <= sent) continue;
        this.sent.set(item.id, text.length);
        actions.push({
          _tag: "text",
          kind: item.type === "reasoning" ? "thought" : "message",
          id: item.id,
          text: text.slice(sent),
        });
      } else if (isToolItem(item)) {
        const key = toolSentKey(item);
        if (this.toolSent.get(item.id) === key) continue;
        this.toolSent.set(item.id, key);
        const command = item.type === "command_execution" ? summary(item.input) : "";
        actions.push({
          _tag: "tool",
          id: item.id,
          title: toolTitleOf(item),
          kind: toolKindOf(item),
          status: toolStatusOf(item.status),
          ...(command === "" ? {} : { command }),
        });
      } else if (item.type === "approval_request" && this.isPending(item.requestId, item.status)) {
        this.handled.add(item.requestId);
        actions.push({ _tag: "approval", item });
      } else if (
        item.type === "user_input_request" &&
        this.isPending(item.requestId, item.status)
      ) {
        this.handled.add(item.requestId);
        actions.push({ _tag: "question", item });
      } else if (
        item.type === "secret_request" &&
        item.secretStatus === "pending" &&
        !this.handled.has(item.id)
      ) {
        this.handled.add(item.id);
        actions.push({ _tag: "secret", item });
      }
    }
    return actions;
  }
}

const BACKGROUND_ITEM_TYPES: ReadonlySet<OrchestrationV2TurnItem["type"]> = new Set([
  "command_execution",
  "dynamic_tool",
  "subagent",
]);
const WAKE_SOURCE_KINDS: ReadonlySet<string> = new Set(["background_task", "command", "monitor"]);
export const SANDBOX_RUN_TASK_PREFIX = "sandbox-run:";

const rosterKey = (tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>) =>
  JSON.stringify(tasks);

// The ids of the sandbox's own records (child threads, items, instances) mean nothing on the host.
const hostTask = (
  task: OrchestrationV2PendingBackgroundTask,
  awaited: boolean,
): OrchestrationV2PendingBackgroundTask =>
  ({
    taskId: task.taskId,
    kind: task.kind,
    ...(task.description === undefined ? {} : { description: task.description }),
    ...(task.command === undefined ? {} : { command: task.command }),
    ...(task.parentTaskId === undefined ? {} : { parentTaskId: task.parentTaskId }),
    ...(awaited || task.awaited === true ? { awaited: true } : {}),
  }) as OrchestrationV2PendingBackgroundTask;

// A notice's ids (tasks, child threads, pull requests) are the sandbox's; the host keeps its words.
const hostNotification = (notification: OrchestrationV2Notification): OrchestrationV2Notification =>
  ({
    ...notification,
    source: {
      kind:
        notification.source.kind === "delegated_task"
          ? "background_task"
          : notification.source.kind,
    },
  }) as OrchestrationV2Notification;

/** A run the sandbox's thread started itself, which the host shows as a run of its own. */
export interface SandboxWake {
  readonly messageId: MessageId;
  readonly text: string;
  readonly notification?: OrchestrationV2Notification;
}

/**
 * What a sandbox thread still works on once the host's turn ended, as the host thread's roster:
 * runs the sandbox started itself (a wake, a queued message) and the background work its thread
 * holds. Holding tasks keep the host's chat Working exactly while the sandbox's own thread is.
 */
export class SandboxWork {
  private readonly runs = new Map<string, OrchestrationV2Run>();
  // Only background work still going; nothing else decides the roster.
  private readonly items = new Map<string, OrchestrationV2TurnItem>();
  private readonly providerThreads = new Map<string, OrchestrationV2ProviderThread>();
  private readonly wakeMessages = new Set<string>();
  // Messages nobody typed: wakes, delegated results, scheduled tasks, the sandbox's own continue.
  private readonly ownPrompts = new Map<string, OrchestrationV2ConversationMessage>();
  private activeProviderThreadId: string | null = null;

  /** Whether the item can change the roster. */
  apply(item: OrchestrationV2ThreadStreamItem): boolean {
    if (item.kind === "snapshot") {
      const { thread, runs, turnItems, providerThreads, messages } = item.projection;
      this.activeProviderThreadId = thread.activeProviderThreadId;
      for (const run of runs) this.runs.set(run.id, run);
      for (const turnItem of turnItems) this.keepItem(turnItem);
      for (const providerThread of providerThreads) {
        this.providerThreads.set(providerThread.id, providerThread);
      }
      for (const message of messages) this.noteMessage(message);
      return true;
    }
    if (item.kind !== "event") return false;
    const event = item.event;
    switch (event.type) {
      case "run.created":
      case "run.updated":
        this.runs.set(event.payload.id, event.payload);
        return true;
      case "turn-item.updated":
        return this.keepItem(event.payload);
      case "provider-thread.updated":
        this.providerThreads.set(event.payload.id, event.payload);
        return true;
      case "thread.provider-switched":
        this.activeProviderThreadId = event.payload.activeProviderThreadId;
        return true;
      case "message.updated":
        return this.noteMessage(event.payload);
      default:
        return false;
    }
  }

  get tasks(): ReadonlyArray<OrchestrationV2PendingBackgroundTask> {
    const runs = [...this.runs.values()];
    const live = runs.filter((run) => !isTerminalRun(run));
    const latestRun = runs
      .filter((run) => run.status !== "queued")
      .reduce<OrchestrationV2Run | null>(
        (latest, run) => (latest === null || run.ordinal > latest.ordinal ? run : latest),
        null,
      );
    const held = derivePendingBackgroundWork({
      latestRun,
      providerThreads: [...this.providerThreads.values()],
      turnItems: [...this.items.values()],
      activeProviderThreadId: this.activeProviderThreadId,
      hasActiveRun: false,
      runs,
    });
    // As the sandbox's own shell: a monitor's progress wake keeps its thread working between lines.
    const wakeHeld =
      latestRun !== null &&
      latestRun.userMessageId !== null &&
      this.wakeMessages.has(latestRun.userMessageId) &&
      monitorHoldsWakeCompletion({
        latestRunBackgroundWake: true,
        pendingBackgroundTasks: held,
      });
    return [
      ...live.map((run): OrchestrationV2PendingBackgroundTask => ({
        taskId: `${SANDBOX_RUN_TASK_PREFIX}${run.id}`,
        kind: "background_task",
        description: "Working on the sandbox",
      })),
      ...held.map((task) => hostTask(task, wakeHeld && task.kind === "monitor")),
    ];
  }

  /** The newest run over when following began; it and every run before it are history. */
  get settledOrdinal(): number {
    let newest = 0;
    for (const run of this.runs.values()) {
      if (isTerminalRun(run)) newest = Math.max(newest, run.ordinal);
    }
    return newest;
  }

  /** Runs after `ordinal` the sandbox's thread started itself, oldest first, once each began. */
  wakesAfter(ordinal: number): ReadonlyArray<SandboxWake> {
    return [...this.runs.values()]
      .filter(
        (run) =>
          run.ordinal > ordinal &&
          run.status !== "queued" &&
          // Cancelled while it waited in the queue: it never ran, so there is nothing to show.
          !(isTerminalRun(run) && run.startedAt === null),
      )
      .toSorted((left, right) => left.ordinal - right.ordinal)
      .flatMap((run) => {
        const message = this.ownPrompts.get(run.userMessageId);
        if (message === undefined) return [];
        return [
          {
            messageId: message.id,
            text: message.text,
            ...(message.notification === undefined
              ? {}
              : { notification: hostNotification(message.notification) }),
          },
        ];
      });
  }

  private keepItem(item: OrchestrationV2TurnItem): boolean {
    if (!BACKGROUND_ITEM_TYPES.has(item.type)) return false;
    if (isOrchestrationV2WorkActive(item.status)) this.items.set(item.id, item);
    else if (!this.items.delete(item.id)) return false;
    return true;
  }

  private noteMessage(message: OrchestrationV2ConversationMessage): boolean {
    const own =
      message.role === "user" &&
      (message.createdBy !== "user" || message.scheduledTaskId !== undefined) &&
      !this.ownPrompts.has(message.id);
    if (own) this.ownPrompts.set(message.id, message);
    return this.noteWakeMessage(message) || own;
  }

  private noteWakeMessage(message: OrchestrationV2ConversationMessage): boolean {
    const notification = message.notification;
    if (
      message.delegatedCompletion !== undefined ||
      notification?.outcome !== "updated" ||
      !WAKE_SOURCE_KINDS.has(notification.source.kind)
    ) {
      return false;
    }
    this.wakeMessages.add(message.id);
    return true;
  }
}

/**
 * Where a host turn picks up on the sandbox: a message already there, or a new one to send. An
 * attach replays what the sandbox already did for the message, unless it is still only queued.
 */
export type TurnTarget =
  | { readonly _tag: "attach"; readonly messageId: MessageId; readonly replay: boolean }
  | { readonly _tag: "send"; readonly messageId: MessageId | null }
  | { readonly _tag: "nothing" };

const userMessages = (projection: Option.Option<OrchestrationV2ThreadProjection>) =>
  Option.isNone(projection)
    ? []
    : projection.value.turnItems.filter(
        (item): item is UserMessageItem => item.type === "user_message",
      );

/**
 * A message the sandbox already has (sent on the computer, or forwarded before a restart) is
 * followed rather than sent again; the host's own "continue" follows the sandbox's latest message.
 */
export const turnTargetOf = (
  before: Option.Option<OrchestrationV2ThreadProjection>,
  message: PromptMessage | null,
  text: string,
): TurnTarget => {
  const messages = userMessages(before);
  if (isHostContinuation(text, message)) {
    const latest = messages.reduce<UserMessageItem | undefined>(
      (newest, item) => (newest === undefined || item.ordinal > newest.ordinal ? item : newest),
      undefined,
    );
    return latest === undefined
      ? { _tag: "nothing" }
      : { _tag: "attach", messageId: latest.messageId, replay: true };
  }
  if (message === null) return { _tag: "send", messageId: null };
  if (messages.some((item) => item.messageId === message.messageId)) {
    return { _tag: "attach", messageId: message.messageId, replay: true };
  }
  // A message queued behind a working run has its run before its item.
  const queued = Option.isSome(before)
    ? before.value.runs.some((run) => run.userMessageId === message.messageId)
    : false;
  if (queued) return { _tag: "attach", messageId: message.messageId, replay: false };
  // The host's run for one the sandbox began itself (`SANDBOX_BRIDGE_WAKE_METHOD`): its notice
  // only names that run, so it is never sent back.
  return message.createdBy === "agent" && message.creationSource === "provider"
    ? { _tag: "nothing" }
    : { _tag: "send", messageId: message.messageId };
};
/** The MCP credential the host put in a session's `mcpServers`, over stdio or HTTP. */
export const hostMcpOf = (
  servers: ReadonlyArray<AcpSchema.McpServer> | undefined,
): HostMcp | null => {
  // `_meta`'s index signature widens every field, so read the shapes structurally.
  const named = (entries: unknown, matches: (name: string) => boolean): string | undefined => {
    if (!Array.isArray(entries)) return undefined;
    for (const entry of entries) {
      if (isRecord(entry) && typeof entry.name === "string" && matches(entry.name)) {
        return typeof entry.value === "string" ? entry.value : undefined;
      }
    }
    return undefined;
  };
  for (const server of servers ?? []) {
    const fields: Readonly<Record<string, unknown>> = server;
    const endpoint = named(fields.env, (name) => name === "T3_ACP_MCP_ENDPOINT");
    const envAuthorization = named(fields.env, (name) => name === "T3_ACP_MCP_AUTHORIZATION");
    if (endpoint && envAuthorization) return { endpoint, authorization: envAuthorization };
    const headerAuthorization = named(
      fields.headers,
      (name) => name.toLowerCase() === "authorization",
    );
    if (typeof fields.url === "string" && headerAuthorization) {
      return { endpoint: fields.url, authorization: headerAuthorization };
    }
  }
  return null;
};

export const sessionIdFor = (threadId: ThreadId) => `${SANDBOX_SESSION_PREFIX}${threadId}`;

export const threadIdOfSession = (sessionId: string): ThreadId | null =>
  sessionId.startsWith(SANDBOX_SESSION_PREFIX) && sessionId.length > SANDBOX_SESSION_PREFIX.length
    ? ThreadId.make(sessionId.slice(SANDBOX_SESSION_PREFIX.length))
    : null;

/** Why the host ended a turn: the user's Stop, or a newer message taking the turn over. */
type CancelReason = "stop" | "steer";

const cancelReasonOf = (meta: unknown): CancelReason =>
  isRecord(meta) && meta[SANDBOX_BRIDGE_CANCEL_META] === SANDBOX_BRIDGE_STEER_CANCEL
    ? "steer"
    : "stop";

interface BridgeSession {
  readonly threadId: ThreadId | null;
  mcp: HostMcp | null;
  cancel: Deferred.Deferred<CancelReason> | null;
  configOptions: ReadonlyArray<AcpSchema.SessionConfigOption>;
  /** A host thread with no sandbox link: its sandbox was deleted. */
  readonly sandboxGone?: boolean;
  /** The sandbox the last turn reached, whose connection a failed turn drops. */
  link?: SandboxBridgeLink | undefined;
  /** Follows the sandbox thread's own work between turns; see `followSandboxWork`. */
  watch?: Fiber.Fiber<void>;
  /** The newest sandbox run a host turn followed: a run it starts itself after that is a wake. */
  followedOrdinal?: number;
}

type ThreadSnapshot = Extract<OrchestrationV2ThreadStreamItem, { readonly kind: "snapshot" }>;

/** A turn's subscription to the sandbox thread: its snapshot, then whatever follows it. */
interface FollowedThread {
  readonly snapshot: Option.Option<ThreadSnapshot>;
  readonly rest: Stream.Stream<OrchestrationV2ThreadStreamItem, SandboxBridgeError>;
}

const linkKey = (link: SandboxBridgeLink) => `${link.httpBaseUrl}\u0000${link.token}`;

const END_OF_TURN = { _tag: "end" } as const;

/**
 * Serves ACP on the agent's stdio until the host closes it. The host starts one bridge process
 * per thread; a session that maps to no sandbox (the provider probe's, for one) is a dry run.
 */
export const runSandboxBridge = Effect.fn("SandboxBridge.run")(function* (
  ports: SandboxBridgePorts,
) {
  const agent = yield* AcpAgent.AcpAgent;
  const random = yield* SecureRandom.SecureRandom;
  const bridgeScope = yield* Effect.scope;
  const sessions = new Map<string, BridgeSession>();
  let clientName: string | undefined;
  const newId = random.uuidV4().pipe(Effect.orDie);

  // One connection per sandbox address, shared by the session's model read and its turns. A
  // resumed sandbox has a new address, so its link names a new connection.
  const linksByKey = new Map<string, SandboxBridgeLink>();
  const connections = yield* RcMap.make({
    lookup: (key: string) => {
      const link = linksByKey.get(key);
      return link === undefined ? Effect.die(`No sandbox link for ${key}`) : ports.connect(link);
    },
    idleTimeToLive: CONNECTION_IDLE_TTL,
  });
  // A failed connect is never kept, so the next attempt connects again.
  const reachSandbox = (link: SandboxBridgeLink) => {
    const key = linkKey(link);
    linksByKey.set(key, link);
    return RcMap.get(connections, key).pipe(
      Effect.tapError(() => RcMap.invalidate(connections, key)),
    );
  };
  const dropSandbox = (link: SandboxBridgeLink) => RcMap.invalidate(connections, linkKey(link));

  const update = (sessionId: string, value: AcpSchema.SessionUpdate) =>
    agent.client.sessionUpdate({ sessionId, update: value });
  const say = (sessionId: string, kind: "message" | "thought", id: string, text: string) =>
    update(sessionId, {
      sessionUpdate: kind === "message" ? "agent_message_chunk" : "agent_thought_chunk",
      messageId: id,
      content: { type: "text", text },
    });
  const sayOnce = (sessionId: string, kind: "message" | "thought", text: string) =>
    newId.pipe(Effect.flatMap((id) => say(sessionId, kind, `bridge-${id}`, text)));

  const sessionOf = (sessionId: string) => {
    const session = sessions.get(sessionId);
    return session === undefined
      ? Effect.fail(AcpError.AcpRequestError.resourceNotFound(`Unknown session ${sessionId}`))
      : Effect.succeed(session);
  };

  yield* agent.handleInitialize((request) =>
    Effect.sync(() => {
      clientName = request.info?.name;
      return {
        protocolVersion: 2,
        info: { name: "t3-sandbox-bridge", version: "1" },
        // Images go to the sandbox as its own attachments; other files get a notice instead.
        capabilities: { session: { prompt: { image: {} } } },
      } satisfies AcpSchema.InitializeResponse;
    }),
  );

  // The sandbox's models for its thread, or none when it does not answer in time.
  const catalogFor = Effect.fn("SandboxBridge.catalogFor")(
    function* (threadId: ThreadId) {
      const link = yield* ports.findLink(threadId);
      if (Option.isNone(link)) return [];
      const sandbox = yield* reachSandbox(link.value);
      const thread = yield* sandbox.projection(threadId);
      const selection = Option.match(thread, {
        onNone: () => sandbox.defaultModelSelection,
        onSome: (projection) => projection.thread.modelSelection,
      });
      return (
        sandboxConfigOptions(sandbox.providers, selection, sandbox.defaultModelSelection) ?? []
      );
    },
    Effect.scoped,
    Effect.timeoutOption(CATALOG_READ_TIMEOUT),
    Effect.map(Option.getOrElse((): ReadonlyArray<AcpSchema.SessionConfigOption> => [])),
    Effect.orElseSucceed((): ReadonlyArray<AcpSchema.SessionConfigOption> => []),
  );

  // The probe and threads no sandbox maps to get a session that only explains itself.
  const openSession = Effect.fn("SandboxBridge.openSession")(function* (
    mcp: HostMcp | null,
  ): Effect.fn.Return<string> {
    if (clientName === PROBE_CLIENT_NAME || mcp === null) {
      const sessionId = `${DRY_RUN_SESSION_PREFIX}${yield* newId}`;
      sessions.set(sessionId, {
        threadId: null,
        mcp: null,
        cancel: null,
        configOptions: modelConfigOptions,
      });
      return sessionId;
    }
    const host = yield* ports.readHostThread(mcp).pipe(Effect.option);
    const link = Option.isNone(host)
      ? Option.none()
      : yield* ports.findLink(host.value.threadId).pipe(Effect.orElseSucceed(Option.none));
    if (Option.isNone(host) || Option.isNone(link)) {
      const sessionId = `${DRY_RUN_SESSION_PREFIX}${yield* newId}`;
      // No models of its own: an empty list leaves the picker with the ones a sandbox listed.
      sessions.set(sessionId, {
        threadId: null,
        mcp,
        cancel: null,
        configOptions: [],
        sandboxGone: Option.isSome(host),
      });
      return sessionId;
    }
    const sessionId = sessionIdFor(host.value.threadId);
    sessions.set(sessionId, {
      threadId: host.value.threadId,
      mcp,
      cancel: null,
      configOptions: yield* catalogFor(host.value.threadId),
    });
    return sessionId;
  });

  yield* agent.handleCreateSession((request) =>
    openSession(hostMcpOf(request.mcpServers)).pipe(
      Effect.map((sessionId) => ({
        sessionId,
        configOptions: sessions.get(sessionId)?.configOptions ?? [],
      })),
    ),
  );

  // The host keeps the conversation, so a session is resumed without replaying any of it.
  const resume = Effect.fn("SandboxBridge.resume")(function* (
    request: AcpSchema.ResumeSessionRequest,
  ) {
    const threadId = threadIdOfSession(request.sessionId);
    if (threadId === null) {
      return yield* AcpError.AcpRequestError.resourceNotFound(
        `Unknown session ${request.sessionId}`,
      );
    }
    const existing = sessions.get(request.sessionId);
    const mcp = hostMcpOf(request.mcpServers) ?? existing?.mcp ?? null;
    const configOptions =
      existing !== undefined && existing.configOptions.length > 0
        ? existing.configOptions
        : yield* catalogFor(threadId);
    sessions.set(request.sessionId, {
      threadId,
      mcp,
      cancel: existing?.cancel ?? null,
      configOptions,
      ...(existing?.followedOrdinal === undefined
        ? {}
        : { followedOrdinal: existing.followedOrdinal }),
    });
    return { configOptions } satisfies AcpSchema.ResumeSessionResponse;
  });
  yield* agent.handleResumeSession(resume);
  yield* agent.handleLoadSession(resume);

  // The pick reaches the sandbox with the next message, read from the host thread then.
  yield* agent.handleSetSessionConfigOption((request) =>
    Effect.gen(function* () {
      const session = yield* sessionOf(request.sessionId);
      session.configOptions = withConfigValue(
        session.configOptions,
        request.configId,
        request.value,
      );
      return { configOptions: session.configOptions };
    }),
  );

  yield* agent.handleCloseSession((request) =>
    Effect.gen(function* () {
      const session = sessions.get(request.sessionId);
      if (session?.cancel) Deferred.doneUnsafe(session.cancel, Effect.succeed("stop"));
      sessions.delete(request.sessionId);
      if (session?.watch !== undefined) yield* Fiber.interrupt(session.watch);
      return {};
    }),
  );

  yield* agent.handleCancel((notification) =>
    Effect.suspend(() => {
      const session = sessions.get(notification.sessionId);
      if (session?.cancel != null) {
        return Deferred.succeed(session.cancel, cancelReasonOf(notification._meta)).pipe(
          Effect.asVoid,
        );
      }
      // Between turns, the host's Stop is for the work the sandbox's thread still does.
      const link = session?.link;
      const threadId = session?.threadId ?? null;
      if (
        session?.watch === undefined ||
        link === undefined ||
        threadId === null ||
        cancelReasonOf(notification._meta) === "steer"
      ) {
        return Effect.void;
      }
      return Effect.gen(function* () {
        const sandbox = yield* reachSandbox(link);
        if (!sandbox.threadStop) return;
        yield* sandbox.dispatch({
          type: "thread.stop",
          commandId: CommandId.make(yield* newId),
          threadId,
        });
      }).pipe(Effect.scoped, Effect.ignore);
    }),
  );

  /**
   * Between turns, keeps the host thread's roster to the sandbox thread's work, and asks the host
   * for a run of its own for each run that thread starts itself, which the next prompt follows.
   * It follows past a settle, since a task's end and the wake it causes arrive apart.
   */
  const followSandboxWork = Effect.fn("SandboxBridge.followSandboxWork")(function* (
    sessionId: string,
    session: BridgeSession,
    threadId: ThreadId,
    link: SandboxBridgeLink,
  ) {
    const report = (tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>) =>
      agent.client
        .extNotification(SANDBOX_BRIDGE_BACKGROUND_WORK_METHOD, { sessionId, tasks })
        .pipe(Effect.ignore);
    const work = new SandboxWork();
    // The host's adapter drops a roster it already has, so the first one always goes.
    let sent: string | null = null;
    // Each wake is asked for once here; the host keeps one pending, and after its run this
    // follow starts again and asks for the next.
    const asked = new Set<string>();
    yield* Effect.gen(function* () {
      const sandbox = yield* reachSandbox(link);
      yield* sandbox.subscribe(threadId).pipe(
        Stream.runForEach((item) =>
          Effect.gen(function* () {
            if (!work.apply(item)) return;
            session.followedOrdinal ??= work.settledOrdinal;
            for (const wake of work.wakesAfter(session.followedOrdinal)) {
              if (asked.has(wake.messageId)) continue;
              asked.add(wake.messageId);
              yield* agent.client
                .extNotification(SANDBOX_BRIDGE_WAKE_METHOD, { sessionId, ...wake })
                .pipe(Effect.ignore);
            }
            const tasks = work.tasks;
            const key = rosterKey(tasks);
            if (key !== sent) {
              sent = key;
              yield* report(tasks);
            }
          }),
        ),
      );
    }).pipe(Effect.scoped, Effect.ignore);
    // Settled, or no longer readable: either way the host stops showing work it cannot follow.
    if (sent !== "[]") yield* report([]);
  });

  yield* agent.handlePrompt((request) =>
    Effect.gen(function* () {
      const session = yield* sessionOf(request.sessionId);
      if (session.watch !== undefined) yield* Fiber.interrupt(session.watch);
      session.link = undefined;
      const cancel = yield* Deferred.make<CancelReason>();
      session.cancel = cancel;
      const stopReason = yield* runTurn(request, session, cancel).pipe(
        Effect.scoped,
        // The next turn connects again rather than trust a connection that just failed.
        Effect.tapError((error) =>
          error._tag === "SandboxBridgeError" && session.link !== undefined
            ? dropSandbox(session.link)
            : Effect.void,
        ),
        Effect.catchTags({
          SandboxBridgeError: (error) =>
            sayOnce(
              request.sessionId,
              "message",
              `The sandbox bridge failed: ${error.message}`,
            ).pipe(
              Effect.orElseSucceed(() => undefined),
              Effect.as<AcpSchema.StopReason>("end_turn"),
            ),
        }),
        Effect.ensuring(Effect.sync(() => (session.cancel = null))),
      );
      // ACP v2 ends a turn with an idle state; the prompt response only acknowledges it.
      yield* update(request.sessionId, {
        sessionUpdate: "state_update",
        state: "idle",
        stopReason,
      });
      if (session.threadId !== null && session.link !== undefined) {
        session.watch = yield* followSandboxWork(
          request.sessionId,
          session,
          session.threadId,
          session.link,
        ).pipe(Effect.forkIn(bridgeScope));
      }
      return {} satisfies AcpSchema.PromptResponse;
    }),
  );

  /**
   * The host's failed resume, when it covers this turn: the message was waiting when it failed, or,
   * for a prompt with no message id, the failure came after the turn began.
   */
  const resumeFailureFor = (
    threadId: ThreadId,
    message: PromptMessage | null,
    turnStartedAt: number,
  ) =>
    (ports.findResumeFailure?.(threadId) ?? Effect.succeedNone).pipe(
      Effect.map(
        Option.filter((failure) =>
          message === null
            ? failure.failedAt >= turnStartedAt
            : failure.messageIds.includes(message.messageId),
        ),
      ),
      // An unreadable marker never ends a turn; the window still bounds the wait.
      Effect.orElseSucceed(() => Option.none<SandboxBridgeResumeFailure>()),
    );

  // Subscribed before the turn sends anything, so its snapshot is the turn's baseline and no
  // event falls between the two.
  const followThread = Effect.fn("SandboxBridge.followThread")(function* (
    sandbox: SandboxSession,
    threadId: ThreadId,
  ) {
    const [first, rest] = yield* Stream.peel(sandbox.subscribe(threadId), Sink.head());
    return {
      snapshot: Option.filter(first, (item): item is ThreadSnapshot => item.kind === "snapshot"),
      rest,
    } satisfies FollowedThread;
  });

  const connectWithRetry = Effect.fn("SandboxBridge.connect")(function* (
    sessionId: string,
    threadId: ThreadId,
    message: PromptMessage | null,
  ): Effect.fn.Return<
    Option.Option<{
      readonly link: SandboxBridgeLink;
      readonly sandbox: SandboxSession;
      readonly thread: FollowedThread;
    }>,
    SandboxBridgeError | AcpError.AcpError,
    Scope.Scope
  > {
    const window = Duration.toMillis(
      Duration.fromInputUnsafe(
        ports.reconnectWindow ?? Duration.seconds(SANDBOX_RESUME_WAIT_SECONDS),
      ),
    );
    // Short, so a turn goes out soon after its sandbox answers; the window is in clock time, so a
    // slow failing attempt does not stretch it.
    const spacing = ports.reconnectSpacing ?? "2 seconds";
    const turnStartedAt = yield* Clock.currentTimeMillis;
    let announced = false;
    let lastLink: SandboxBridgeLink | null = null;
    while (true) {
      const link = yield* ports.findLink(threadId);
      if (Option.isNone(link)) return Option.none();
      lastLink = link.value;
      // A connection kept from earlier may have dropped since; that one is replaced at once.
      const reused = yield* RcMap.has(connections, linkKey(link.value));
      const connected = yield* reachSandbox(link.value).pipe(Effect.option);
      if (Option.isSome(connected)) {
        const read = followThread(connected.value, threadId).pipe(
          Effect.tapError(() => dropSandbox(link.value)),
        );
        const thread = reused ? yield* Effect.option(read) : Option.some(yield* read);
        if (Option.isNone(thread)) continue;
        yield* Effect.annotateCurrentSpan({ "sandbox.connection_reused": reused });
        return Option.some({ link: link.value, sandbox: connected.value, thread: thread.value });
      }
      if (reused) continue;
      const failure = yield* resumeFailureFor(threadId, message, turnStartedAt);
      if (Option.isSome(failure)) {
        yield* sayOnce(sessionId, "message", resumeFailedMessage(link.value, failure.value.reason));
        return Option.none();
      }
      if ((yield* Clock.currentTimeMillis) - turnStartedAt >= window) break;
      if (!announced) {
        announced = true;
        yield* sayOnce(sessionId, "thought", waitingThought(link.value));
      }
      yield* Effect.sleep(spacing);
    }
    yield* sayOnce(sessionId, "message", unreachableMessage(lastLink));
    return Option.none();
  });

  const runTurn = Effect.fn("SandboxBridge.turn")(function* (
    request: AcpSchema.PromptRequest,
    session: BridgeSession,
    cancel: Deferred.Deferred<CancelReason>,
  ): Effect.fn.Return<AcpSchema.StopReason, SandboxBridgeError | AcpError.AcpError, Scope.Scope> {
    const sessionId = request.sessionId;
    const threadId = session.threadId;
    if (threadId === null) {
      yield* sayOnce(
        sessionId,
        "message",
        session.sandboxGone === true ? SANDBOX_GONE_MESSAGE : NOT_LINKED_MESSAGE,
      );
      return "end_turn";
    }
    const readHost = session.mcp
      ? ports.readHostThread(session.mcp).pipe(Effect.option)
      : Effect.succeedNone;
    // The host thread's modes and pick, read while the sandbox connects.
    const hostAtStart = yield* Effect.forkScoped(readHost);
    const connected = yield* connectWithRetry(
      sessionId,
      threadId,
      promptMessageOf(request._meta),
    ).pipe(Effect.raceFirst(Deferred.await(cancel).pipe(Effect.as(null))));
    if (connected === null) return "cancelled";
    if (Option.isNone(connected)) {
      if (!(yield* ports.findLink(threadId)).pipe(Option.isSome)) {
        yield* sayOnce(sessionId, "message", SANDBOX_GONE_MESSAGE);
      }
      return "end_turn";
    }
    const { link, sandbox, thread } = connected.value;
    session.link = link;
    const sandboxName = link.sandboxId || link.label;
    const before = Option.map(thread.snapshot, (snapshot) => snapshot.projection);
    const content = promptContent(request.prompt);
    const target = turnTargetOf(before, promptMessageOf(request._meta), content.text);
    if (target._tag === "nothing") return "end_turn";

    const respondQuietly = (command: OrchestrationV2Command) =>
      sandbox.dispatch(command).pipe(Effect.catchTags({ SandboxBridgeError: () => Effect.void }));
    let tracker: TurnTracker;
    let sandboxSelection = Option.match(before, {
      onNone: () => sandbox.defaultModelSelection,
      onSome: (projection) => projection.thread.modelSelection,
    });
    if (target._tag === "attach") {
      tracker = new TurnTracker(target.messageId, before, target.replay);
    } else {
      if (content.skipped.length > 0) {
        yield* sayOnce(sessionId, "message", skippedAttachmentsMessage(content.skipped));
      }
      if (content.text === "" && content.images.length === 0) return "end_turn";
      const messageId = target.messageId ?? MessageId.make(yield* newId);
      const [host, attachments] = yield* Effect.all(
        [
          Fiber.join(hostAtStart),
          content.images.length === 0
            ? Effect.succeed([])
            : sandbox.persistAttachments({ threadId, messageId, attachments: content.images }),
        ],
        { concurrency: "unbounded" },
      );
      const runtimeMode = Option.match(host, {
        onNone: (): RuntimeMode => "approval-required",
        onSome: (thread) => thread.runtimeMode,
      });
      const interactionMode = Option.match(host, {
        onNone: (): ProviderInteractionMode => "default",
        onSome: (thread) => thread.interactionMode,
      });
      const commandId = CommandId.make(yield* newId);
      // A model picked on the host (the phone's picker) goes with the message; none keeps the
      // sandbox's, such as one picked on the computer and already sent from there.
      const picked = sandboxSelectionFor(
        Option.match(host, { onNone: () => null, onSome: (thread) => thread.modelSelection }),
        sandboxSelection,
        sandbox.providers,
      );
      if (picked !== null) sandboxSelection = picked;
      const dispatchMessage = sandbox.dispatch({
        type: "message.dispatch",
        commandId,
        createdBy: "user",
        creationSource: "web",
        threadId,
        messageId,
        text: content.text,
        attachments: [...attachments],
        ...(picked === null ? {} : { modelSelection: picked }),
        // As a chat's send: a busy thread takes it as a steer, or queues it when it cannot.
        deliveryIntent: "auto",
        dispatchMode: { type: "start_immediately" },
      });
      if (Option.isNone(before)) {
        // The first message makes the thread on the sandbox, with the id the host thread has.
        yield* sandbox
          .launch({
            commandId,
            creationSource: "web",
            threadId,
            projectId: link.sandboxProjectId,
            title: content.text.slice(0, 80).trim() || "Sandbox thread",
            generateTitle: true,
            modelSelection: sandboxSelection ?? CLAUDE_FALLBACK_MODEL,
            runtimeMode,
            interactionMode,
            workspaceStrategy: { type: "root" },
            initialMessage: { messageId, text: content.text, attachments: [...attachments] },
          })
          .pipe(Effect.catch(() => dispatchMessage));
      } else {
        // The phone's permission and plan modes are the thread's; the sandbox runs with them.
        if (Option.isSome(host)) {
          const id = yield* newId;
          let count = 0;
          const commands = modeSyncCommands(before.value, { runtimeMode, interactionMode }, () =>
            CommandId.make(`${id}:${count++}`),
          );
          yield* Effect.forEach(
            commands,
            (command) =>
              command.type === "runtime-request.respond"
                ? respondQuietly(command)
                : sandbox.dispatch(command),
            { concurrency: "unbounded", discard: true },
          );
        }
        yield* dispatchMessage;
      }
      tracker = new TurnTracker(messageId, before);
    }
    // A session opened while the sandbox was stopped gets its models now.
    const catalog = sandboxConfigOptions(
      sandbox.providers,
      sandboxSelection,
      sandbox.defaultModelSelection,
    );
    if (catalog !== null && !sameConfigOptions(catalog, session.configOptions)) {
      session.configOptions = catalog;
      yield* update(sessionId, { sessionUpdate: "config_option_update", configOptions: catalog });
    }

    const withdraw = agent.client
      .extNotification(SANDBOX_BRIDGE_WITHDRAW_METHOD, { sessionId })
      .pipe(Effect.ignore);
    // A request answered on the computer (or by the sandbox itself) closes the host's copy.
    const unlessAnswered = <A, E, R>(requestId: RuntimeRequestId, ask: Effect.Effect<A, E, R>) =>
      ask.pipe(
        Effect.asSome,
        Effect.raceFirst(
          Deferred.await(tracker.answered(requestId)).pipe(
            Effect.andThen(withdraw),
            Effect.as(Option.none<A>()),
          ),
        ),
      );
    // The sandbox refuses an answer to a request someone already answered; that one is settled.
    const respond = (command: OrchestrationV2Command) =>
      sandbox.dispatch(command).pipe(Effect.catchTags({ SandboxBridgeError: () => Effect.void }));

    const forwardApproval = Effect.fn("SandboxBridge.forwardApproval")(function* (
      item: ApprovalItem,
    ) {
      const mode = Option.map(yield* readHost, (host) => host.runtimeMode);
      const toolCallId = `sandbox-request:${item.requestId}`;
      const title = `Sandbox ${sandboxName}: ${item.prompt ?? item.title ?? "Approve this action"}`;
      const kind = approvalToolKind(item.requestKind);
      const options = approvalOptions(item.options);
      // The host names a command request by its command, as the desktop does.
      const command = kind === "execute" ? summary(item.prompt) : "";
      const rawInput = command === "" ? {} : { rawInput: { command } };
      // Full access grants what the sandbox still asks for, such as a run started before it.
      if (Option.getOrUndefined(mode) === "full-access") {
        yield* respond({
          type: "runtime-request.respond",
          commandId: CommandId.make(yield* newId),
          threadId,
          requestId: item.requestId,
          decision: "accept",
        });
        yield* update(sessionId, {
          sessionUpdate: "tool_call_update",
          toolCallId,
          title,
          kind,
          status: "completed",
          ...rawInput,
        });
        return;
      }
      yield* update(sessionId, {
        sessionUpdate: "tool_call_update",
        toolCallId,
        title,
        kind,
        status: "pending",
        ...rawInput,
      });
      // Only a Supervised thread shows a permission request to the user; any other mode's
      // policy would answer it alone, so the request comes as a question instead.
      const ask =
        Option.getOrUndefined(mode) === "approval-required"
          ? agent.client
              .requestPermission({
                sessionId,
                title,
                ...(item.reason === undefined ? {} : { description: item.reason }),
                subject: {
                  type: "tool_call",
                  toolCall: { toolCallId, title, kind, status: "pending", ...rawInput },
                },
                options,
              })
              .pipe(Effect.map(decisionOf))
          : agent.client
              .elicit({
                sessionId,
                message: item.reason === undefined ? title : `${title}\n\n${item.reason}`,
                mode: "form",
                requestedSchema: approvalForm(options),
              })
              .pipe(
                Effect.map((response): ProviderApprovalDecision | null =>
                  response.action === "accept"
                    ? formDecisionOf(options, "content" in response ? response.content : undefined)
                    : response.action === "decline"
                      ? "decline"
                      : null,
                ),
              );
      const answer = yield* unlessAnswered(item.requestId, ask);
      if (Option.isNone(answer)) {
        yield* update(sessionId, {
          sessionUpdate: "tool_call_update",
          toolCallId,
          status: "completed",
        });
        return;
      }
      const decision = answer.value;
      if (decision === null) return;
      yield* respond({
        type: "runtime-request.respond",
        commandId: CommandId.make(yield* newId),
        threadId,
        requestId: item.requestId,
        decision,
      });
      yield* update(sessionId, {
        sessionUpdate: "tool_call_update",
        toolCallId,
        status: decision === "decline" ? "failed" : "completed",
      });
    });

    const forwardQuestion = Effect.fn("SandboxBridge.forwardQuestion")(function* (
      item: QuestionItem,
    ) {
      const answer = yield* unlessAnswered(
        item.requestId,
        agent.client.elicit({
          sessionId,
          message: `Sandbox ${sandboxName} asks`,
          mode: "form",
          requestedSchema: questionForm(item.questions),
        }),
      );
      if (Option.isNone(answer)) return;
      const response = answer.value;
      if (response.action === "accept") {
        yield* respond({
          type: "runtime-request.respond",
          commandId: CommandId.make(yield* newId),
          threadId,
          requestId: item.requestId,
          answers: answersOf(item.questions, "content" in response ? response.content : undefined),
        });
      } else if (response.action === "decline") {
        yield* respond({
          type: "thread.user-input.dismiss",
          commandId: CommandId.make(yield* newId),
          threadId,
          requestId: item.requestId,
        });
      }
    });

    // The host's own card asks, so the value reaches only the sandbox: never this thread's history.
    const forwardSecret = Effect.fn("SandboxBridge.forwardSecret")(function* (item: SecretItem) {
      const ask = ports.requestSecret;
      if (session.mcp === null || ask === undefined) return;
      const url = item.url === undefined ? null : secretRequestPageUrl(item.url);
      const answer = yield* ask(session.mcp, {
        clientRequestId: item.id,
        label: item.label,
        reason: item.reason.trim() || `Sandbox ${sandboxName}'s agent needs it.`,
        ...(url === null ? {} : { url: url.href }),
        ...(item.placeholder === undefined ? {} : { placeholder: item.placeholder }),
      }).pipe(
        Effect.asSome,
        Effect.raceFirst(
          Deferred.await(tracker.answered(item.id)).pipe(
            Effect.as(Option.none<HostSecretAnswer>()),
          ),
        ),
        Effect.orElseSucceed(() => Option.none<HostSecretAnswer>()),
      );
      if (Option.isNone(answer)) return;
      const reply = answer.value;
      if (reply.status !== "saved" && reply.status !== "declined") return;
      // Ignored, not reported: a failure here must never carry the value into a message or log.
      yield* sandbox
        .answerSecret({
          threadId,
          turnItemId: item.id,
          answer:
            reply.status === "saved"
              ? { type: "save", secret: Redacted.value(reply.value) }
              : { type: "decline" },
        })
        .pipe(Effect.ignore);
    });

    // A reply's images live on the sandbox, which the host's clients cannot read and which goes
    // away with the chat: each is copied to the host once a turn, and the reply names the copy.
    const saveImage = ports.saveImage;
    const copied = new Map<string, Option.Option<string>>();
    const relayed = new Map<string, { kind: "message" | "thought"; text: string; sent: number }>();
    const hostCopies = Effect.fn("SandboxBridge.hostCopies")(function* (
      paths: ReadonlyArray<string>,
      copyNew: boolean,
    ) {
      if (saveImage === undefined) return new Map<string, string>();
      const missing = copyNew ? [...new Set(paths)].filter((path) => !copied.has(path)) : [];
      yield* Effect.forEach(
        missing,
        (path) =>
          sandbox.readImage(threadId, path).pipe(
            Effect.flatMap((image) => saveImage(threadId, image)),
            Effect.timeout(IMAGE_COPY_TIMEOUT),
            Effect.option,
            Effect.map((copy) => void copied.set(path, copy)),
          ),
        { concurrency: 4, discard: true },
      );
      return new Map(
        paths.flatMap((path) => {
          const copy = copied.get(path);
          return copy !== undefined && Option.isSome(copy) ? [[path, copy.value] as const] : [];
        }),
      );
    });
    // Sends what of an item's text can go now: all of it when `whole`, else all but an image
    // reference still arriving.
    const relayText = Effect.fn("SandboxBridge.relayText")(function* (
      kind: "message" | "thought",
      id: string,
      text: string,
      whole: boolean,
      copyNew = true,
    ) {
      const entry = relayed.get(id) ?? { kind, text: "", sent: 0 };
      relayed.set(id, entry);
      entry.text += text;
      const start = entry.sent;
      const end = whole ? entry.text.length : releasableEnd(entry.text, start);
      if (end <= start) return;
      const references = localImageReferences(entry.text, start, end);
      const copies = yield* hostCopies(
        references.map((reference) => reference.path),
        copyNew,
      );
      const segment = entry.text.slice(start, end);
      entry.sent = end;
      yield* say(sessionId, kind, id, pointAtHostCopies(segment, start, references, copies));
    });
    // Held text goes out before anything else, so the host keeps the sandbox's order.
    const flushText = (exceptId?: string, copyNew = true) =>
      Effect.forEach(
        [...relayed].filter(([id, entry]) => id !== exceptId && entry.sent < entry.text.length),
        ([id, entry]) => relayText(entry.kind, id, "", true, copyNew),
        { discard: true },
      );

    const performAction = (action: Exclude<TurnAction, { readonly _tag: "text" }>) => {
      switch (action._tag) {
        case "tool":
          return update(sessionId, {
            sessionUpdate: "tool_call_update",
            toolCallId: `sandbox-tool:${action.id}`,
            title: action.title,
            kind: action.kind,
            status: action.status,
            ...(action.command === undefined ? {} : { rawInput: { command: action.command } }),
          });
        case "approval":
          return forwardApproval(action.item);
        case "question":
          return forwardQuestion(action.item);
        case "secret":
          return forwardSecret(action.item);
      }
    };
    const perform = (action: TurnAction) =>
      action._tag !== "text"
        ? flushText().pipe(Effect.andThen(performAction(action)))
        : saveImage === undefined
          ? say(sessionId, action.kind, action.id, action.text)
          : flushText(action.id).pipe(
              Effect.andThen(relayText(action.kind, action.id, action.text, false)),
            );

    // The stream keeps reading while an approval waits on the host, so an answer given on the
    // computer reaches it; actions run one at a time, in order.
    const actions = yield* Queue.unbounded<TurnAction | typeof END_OF_TURN>();
    // The snapshot owes the host what it holds from the message on, or, for a message just sent,
    // only the requests already waiting.
    yield* Queue.offerAll(
      actions,
      target._tag === "attach" && target.replay
        ? tracker.replay()
        : Option.match(thread.snapshot, {
            onNone: () => [],
            onSome: (item) => tracker.apply(item),
          }),
    );
    // A thread the turn made has no subscription yet.
    const live = Option.isSome(thread.snapshot) ? thread.rest : sandbox.subscribe(threadId);
    const reader = yield* (
      tracker.done
        ? Effect.void
        : live.pipe(
            Stream.runForEachWhile((item) =>
              Queue.offerAll(actions, tracker.apply(item)).pipe(Effect.map(() => !tracker.done)),
            ),
          )
    ).pipe(Effect.ensuring(Queue.offer(actions, END_OF_TURN)), Effect.forkScoped);
    const performAll = Effect.gen(function* () {
      while (true) {
        const action = yield* Queue.take(actions);
        if (action._tag === "end") return yield* flushText();
        yield* perform(action);
      }
    });
    const outcome = yield* performAll.pipe(
      Effect.andThen(Fiber.join(reader)),
      Effect.as("followed" as const),
      Effect.raceFirst(Deferred.await(cancel)),
    );
    const followed = tracker.run;
    if (followed !== undefined) {
      session.followedOrdinal = Math.max(session.followedOrdinal ?? 0, followed.ordinal);
    }
    // What a cancelled turn held back still shows, with the copies it already made.
    if (outcome !== "followed") yield* flushText(undefined, false);
    // A steer's cancel leaves the run and its tasks working; the steer follows as the next prompt.
    if (outcome === "steer") return "cancelled";
    if (outcome === "stop") {
      const runId = tracker.activeRunId;
      yield* (
        sandbox.threadStop
          ? sandbox.dispatch({
              type: "thread.stop",
              commandId: CommandId.make(yield* newId),
              threadId,
            })
          : runId === undefined
            ? Effect.void
            : sandbox.dispatch({
                type: "run.interrupt",
                commandId: CommandId.make(yield* newId),
                threadId,
                runId,
              })
      ).pipe(Effect.ignore);
      return "cancelled";
    }
    const run = tracker.run;
    if (run?.status === "failed") {
      yield* sayOnce(
        sessionId,
        "message",
        "The sandbox turn failed. Open the thread on the sandbox for details.",
      );
    }
    return run?.status === "interrupted" || run?.status === "cancelled" ? "cancelled" : "end_turn";
  });

  return yield* Effect.never;
}, Effect.scoped);
