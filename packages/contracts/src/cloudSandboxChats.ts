import * as Schema from "effect/Schema";
import { MessageId, ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { SandboxProfileId } from "./environment.ts";
import { ManagedCloudSandboxResourceId } from "./managedCloudSandbox.ts";
import { ProviderInteractionMode, RuntimeMode } from "./providerPolicy.ts";

/**
 * Where a new chat runs when the composer picks a sandbox: a disposable follower of its own, the
 * project's kept follower, or a sandbox the client already has open.
 */
export const CloudSandboxChatTarget = Schema.Union([
  Schema.TaggedStruct("new", {}),
  Schema.TaggedStruct("project", {}),
  Schema.TaggedStruct("current", { resourceId: ManagedCloudSandboxResourceId }),
]);
export type CloudSandboxChatTarget = typeof CloudSandboxChatTarget.Type;

export const CloudSandboxChatStartInput = Schema.Struct({
  threadId: ThreadId,
  projectId: ProjectId,
  target: CloudSandboxChatTarget,
  profile: Schema.optionalKey(SandboxProfileId),
  message: Schema.Struct({
    messageId: MessageId,
    text: TrimmedNonEmptyString,
    title: TrimmedNonEmptyString,
  }),
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
});
export type CloudSandboxChatStartInput = typeof CloudSandboxChatStartInput.Type;

/** What a settled chat does with its disposable sandbox: stop and keep its disk, or delete it. */
export const CloudSandboxSettleChoice = Schema.Literals(["keep", "delete"]);
export type CloudSandboxSettleChoice = typeof CloudSandboxSettleChoice.Type;

export const CloudSandboxChatState = Schema.Struct({
  threadId: ThreadId,
  resourceId: ManagedCloudSandboxResourceId,
  target: Schema.Literals(["new", "project", "current"]),
  /** `pending` while the settled chat waits for the user's keep-or-delete answer. */
  settle: Schema.Literals(["none", "pending", "kept", "deleted"]),
  /** The project's remembered answer, which pre-answers future prompts. */
  projectDefault: Schema.NullOr(CloudSandboxSettleChoice),
});
export type CloudSandboxChatState = typeof CloudSandboxChatState.Type;

export const CloudSandboxChatResolveInput = Schema.Struct({
  threadId: ThreadId,
  choice: CloudSandboxSettleChoice,
  remember: Schema.Boolean,
});
export type CloudSandboxChatResolveInput = typeof CloudSandboxChatResolveInput.Type;

export class CloudSandboxChatError extends Schema.TaggedError<CloudSandboxChatError>()(
  "CloudSandboxChatError",
  { message: Schema.String },
) {}
