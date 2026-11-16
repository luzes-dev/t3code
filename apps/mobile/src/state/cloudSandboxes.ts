import {
  createManagedSandboxAtoms,
  type SandboxPickerItem,
} from "@t3tools/client-runtime/state/cloudSandbox";
import { Atom } from "effect/reactivity";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import { WS_METHODS } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { connectionAtomRuntime } from "../connection/runtime";

const label = "mobile:cloud-sandbox-profiles";

export const sandboxProfilesList = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: `${label}:list`,
  tag: WS_METHODS.cloudSandboxProfilesList,
  // Spend and budgets move as workspaces allocate; keep answers fresh briefly.
  staleTimeMs: 30_000,
});

export const sandboxProfilesSave = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: `${label}:save`,
  tag: WS_METHODS.cloudSandboxProfilesSave,
  onSuccess: ({ environmentId }, registry) =>
    Effect.sync(() => registry.refresh(sandboxProfilesList({ environmentId, input: {} }))),
});

export const sandboxProfilesDelete = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: `${label}:delete`,
  tag: WS_METHODS.cloudSandboxProfilesDelete,
  onSuccess: ({ environmentId }, registry) =>
    Effect.sync(() => registry.refresh(sandboxProfilesList({ environmentId, input: {} }))),
});

export const cloudSandboxOperation = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "mobile:cloud-sandbox:operation",
  tag: WS_METHODS.cloudSandboxControl,
});

const managedSandboxes = createManagedSandboxAtoms(connectionAtomRuntime);
export const managedSandboxesAtom = managedSandboxes.list;
// Owner-derived resources, separate from the controller administration list.
export const selectableManagedSandboxesAtom = managedSandboxes.selectable;
// Degraded followers, collapsed per cause: one warning naming one fix.
export const managedSandboxIssuesAtom = managedSandboxes.issues;
export const deleteManagedSandbox = managedSandboxes.remove;

export const sandboxChatStart = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "mobile:cloud-sandbox-chats:start",
  tag: WS_METHODS.cloudSandboxChatStart,
});

export const sandboxChatState = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "mobile:cloud-sandbox-chats:state",
  tag: WS_METHODS.cloudSandboxChatState,
  staleTimeMs: 0,
});

export const sandboxChatResolveSettle = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "mobile:cloud-sandbox-chats:resolve-settle",
  tag: WS_METHODS.cloudSandboxChatResolveSettle,
  onSuccess: ({ environmentId, input }, registry) =>
    Effect.sync(() =>
      registry.refresh(sandboxChatState({ environmentId, input: { threadId: input.threadId } })),
    ),
});

export const sandboxChatOpen = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "mobile:cloud-sandbox-chats:open",
  tag: WS_METHODS.cloudSandboxChatOpen,
});

/** The sandbox a new task's draft will run on; null runs it on the checkout or a worktree. */
export const draftSandboxTargetAtom = Atom.family((_draftKey: string) =>
  Atom.make<SandboxPickerItem | null>(null).pipe(Atom.keepAlive),
);
