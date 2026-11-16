import { ProjectId, ThreadId, TrimmedNonEmptyString, TrimmedString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

/**
 * What the bridge needs to reach one thread's sandbox. The host writes it as a server secret, since
 * it carries the bearer token minted on the sandbox; the bridge reads it again before every turn,
 * so a resumed sandbox's new address is picked up.
 */
export const SandboxBridgeLink = Schema.Struct({
  threadId: ThreadId,
  instanceId: TrimmedNonEmptyString,
  sandboxId: TrimmedString,
  label: TrimmedString,
  httpBaseUrl: TrimmedNonEmptyString,
  sandboxProjectId: ProjectId,
  token: TrimmedNonEmptyString,
});
export type SandboxBridgeLink = typeof SandboxBridgeLink.Type;

export const SandboxBridgeLinkJson = Schema.fromJsonString(SandboxBridgeLink);

const hex = (threadId: string) =>
  Array.from(new TextEncoder().encode(threadId), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );

/** Thread ids are free text, so the secret's file name carries them hex-encoded. */
export const bridgeLinkSecretName = (threadId: string): string => `sandbox-bridge-${hex(threadId)}`;

/**
 * Written by the host when its resume of a thread's stopped sandbox fails, so a turn waiting for
 * that sandbox ends with the reason instead of waiting out the whole window. Names the messages
 * whose turns were waiting then; a later message starts a new resume, which clears it.
 */
export const SandboxBridgeResumeFailure = Schema.Struct({
  reason: Schema.String,
  messageIds: Schema.Array(Schema.String),
  failedAt: Schema.Number,
});
export type SandboxBridgeResumeFailure = typeof SandboxBridgeResumeFailure.Type;

export const SandboxBridgeResumeFailureJson = Schema.fromJsonString(SandboxBridgeResumeFailure);

/** The host's record of a thread's sandbox launch, kept until its first message goes out. */
export const bridgeLaunchSecretName = (threadId: string): string =>
  `sandbox-bridge-launch-${hex(threadId)}`;

export const bridgeResumeFailureSecretName = (threadId: string): string =>
  `sandbox-bridge-resume-failed-${hex(threadId)}`;

/** Names the host's secrets directory for the bridge process; set on the provider instance. */
export const SANDBOX_BRIDGE_SECRETS_DIR_ENV = "T3_SANDBOX_BRIDGE_SECRETS_DIR";
/** Where the bridge keeps the images a sandbox reply embeds; the host's attachments directory. */
export const SANDBOX_BRIDGE_ATTACHMENTS_DIR_ENV = "T3_SANDBOX_BRIDGE_ATTACHMENTS_DIR";
/** Where the bridge appends its spans, beside the host's own trace; no tracing without it. */
export const SANDBOX_BRIDGE_TRACE_FILE_ENV = "T3_SANDBOX_BRIDGE_TRACE_FILE";
