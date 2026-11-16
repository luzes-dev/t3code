import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import type {
  EnvironmentId,
  PreviewCloseInput,
  PreviewSessionSnapshot,
  ScopedThreadRef,
} from "@t3tools/contracts";

import {
  releasePreviewExternalServerUrl,
  resolveServerOwnedUrl,
} from "~/browser/browserTargetResolver";
import { rendersServerTabNatively } from "~/browser/previewRuntime";
import { useClosedViewStore } from "~/closedViewStore";
import { beginPreviewSessionClose, cancelPreviewSessionClose } from "~/previewStateStore";
import { appAtomRegistry } from "~/rpc/atomRegistry";
import { primaryEnvironmentIdAtom } from "~/state/primaryEnvironment";

interface ClosePreviewSessionInput<E> {
  readonly closePreview: (input: {
    readonly environmentId: EnvironmentId;
    readonly input: PreviewCloseInput;
  }) => Promise<AtomCommandResult<void, E>>;
  readonly snapshot: PreviewSessionSnapshot | null;
  readonly tabId: string;
  readonly threadRef: ScopedThreadRef;
}

export async function closePreviewSession<E>(
  input: ClosePreviewSessionInput<E>,
): Promise<AtomCommandResult<void, E>> {
  // Unmounting a webview destroys its page, so a tab this window renders stays mounted until the
  // server has unloaded it.
  const keepMounted = rendersServerTabNatively(
    input.threadRef.environmentId,
    appAtomRegistry.get(primaryEnvironmentIdAtom),
    input.snapshot,
  );
  if (!keepMounted) beginPreviewSessionClose(input.threadRef, input.tabId);
  const result = await input.closePreview({
    environmentId: input.threadRef.environmentId,
    input: { threadId: input.threadRef.threadId, tabId: input.tabId },
  });
  if (result._tag === "Failure") {
    cancelPreviewSessionClose(input.threadRef, input.snapshot, input.tabId);
    return result;
  }
  if (keepMounted) beginPreviewSessionClose(input.threadRef, input.tabId);
  const snapshot = input.snapshot;
  const restorable =
    snapshot === null || snapshot.navStatus._tag === "Idle"
      ? snapshot
      : {
          ...snapshot,
          navStatus: {
            ...snapshot.navStatus,
            url: resolveServerOwnedUrl(input.threadRef.environmentId, snapshot.navStatus.url),
          },
        };
  await releasePreviewExternalServerUrl(input.threadRef.environmentId, input.tabId);
  if (restorable) {
    useClosedViewStore.getState().remember({
      kind: "browser",
      threadRef: input.threadRef,
      snapshot: restorable,
    });
  }
  return result;
}
