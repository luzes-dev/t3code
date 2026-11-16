import * as Effect from "effect/Effect";
import { Atom } from "effect/reactivity";
import { WS_METHODS, type CloudSandboxChatTarget } from "@t3tools/contracts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";

import { connectionAtomRuntime } from "../connection/runtime";

const label = "web:cloud-sandbox-chats";

export const sandboxChatStart = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: `${label}:start`,
  tag: WS_METHODS.cloudSandboxChatStart,
});

export const sandboxChatState = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: `${label}:state`,
  tag: WS_METHODS.cloudSandboxChatState,
  staleTimeMs: 0,
});

export const sandboxChatResolveSettle = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: `${label}:resolve-settle`,
  tag: WS_METHODS.cloudSandboxChatResolveSettle,
  onSuccess: ({ environmentId, input }, registry) =>
    Effect.sync(() =>
      registry.refresh(sandboxChatState({ environmentId, input: { threadId: input.threadId } })),
    ),
});

/** The sandbox a new chat's draft will run on; null runs it on this checkout or a worktree. */
export const draftSandboxTargetAtom = Atom.family((_draftId: string) =>
  Atom.make<CloudSandboxChatTarget | null>(null).pipe(Atom.keepAlive),
);
