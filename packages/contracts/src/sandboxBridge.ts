/**
 * The sandbox bridge: the provider instance a cloud sandbox chat's host thread runs on. It
 * forwards each turn to the thread with the same id on the follower sandbox's own T3 server and
 * streams that thread's work back, so every client of the host, the phone included, drives it.
 * Adapted from OmarAlex24's `feat/sandbox-environments` (pingdotgg/t3code#27949).
 */

/** The provider instance that drives sandbox threads from the host, and its one model. */
export const SANDBOX_BRIDGE_INSTANCE_ID = "sandboxes";
export const SANDBOX_BRIDGE_MODEL = "sandbox";
/** The host's prompt `_meta` key naming the T3 message, so the bridge reuses its id on the sandbox. */
export const SANDBOX_BRIDGE_MESSAGE_META = "t3code/message";
/**
 * The host's `session/cancel` `_meta` key when it cancels a sandbox prompt only to send a newer
 * message (a steer), not for the user's Stop; the bridge then leaves the sandbox's run working.
 */
export const SANDBOX_BRIDGE_CANCEL_META = "t3code/cancel";
export const SANDBOX_BRIDGE_STEER_CANCEL = "steer";
/** The bridge's model option `_meta` key naming the model a sandbox chat gets when none is picked. */
export const SANDBOX_BRIDGE_DEFAULT_MODEL_META = "t3code/defaultModel";
/** The bridge's ACP notification that its open approval or question was answered on the sandbox. */
export const SANDBOX_BRIDGE_WITHDRAW_METHOD = "_t3code/sandbox_requests_withdrawn";
/**
 * The bridge's ACP notification of what the sandbox's thread still works on after the host's turn
 * ended, as the host thread's background roster; an empty list once it settles.
 */
export const SANDBOX_BRIDGE_BACKGROUND_WORK_METHOD = "_t3code/sandbox_background_work";
/**
 * The bridge's ACP notification that the sandbox's thread started a run on its own between host
 * turns: the host starts a provider continuation with the sandbox's message id, which the bridge
 * then follows like any message the sandbox already has.
 */
export const SANDBOX_BRIDGE_WAKE_METHOD = "_t3code/sandbox_wake";

/**
 * How long a host turn waits for its stopped follower to resume and answer: the provider start,
 * SSH setup, `t3 serve` start, and pairing, each at its longest.
 */
export const SANDBOX_RESUME_WAIT_SECONDS = 600;

/** Why a sandbox chat refuses another provider: it would run on this computer, with no sandbox. */
export const SANDBOX_BRIDGE_SWITCH_REFUSAL =
  "This chat runs on a sandbox, so it keeps the sandbox's agent. Pick a model from the sandbox's list, or start a new chat to work on this computer.";

/** Whether a selection would move a thread that runs through the sandbox bridge off it. */
export const leavesSandboxBridge = (
  current: { readonly instanceId: string },
  next: { readonly instanceId: string } | undefined,
): boolean =>
  current.instanceId === SANDBOX_BRIDGE_INSTANCE_ID &&
  next !== undefined &&
  next.instanceId !== SANDBOX_BRIDGE_INSTANCE_ID;

/** The prompt a sandbox continuation sends when it resumes a run the sandbox interrupted. */
export const CONTINUE_AFTER_INTERRUPTION_PROMPT = "Continue where you left off.";
