import type { CloudSandboxSettleChoice, EnvironmentId, ThreadId } from "@t3tools/contracts";
import { CloudIcon } from "lucide-react";
import { useEffect, useEffectEvent, useState } from "react";

import {
  sandboxChatOpen,
  sandboxChatResolveSettle,
  sandboxChatState,
} from "~/state/cloudSandboxChats";
import { useEnvironmentQuery } from "~/state/query";
import { useOrchestrationCommand } from "~/state/use-orchestration-command";
import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";

/** Asks once whether a settled chat keeps its disposable sandbox (stopped) or deletes it. */
export function SandboxSettleCard({
  environmentId,
  threadId,
}: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  const state = useEnvironmentQuery(sandboxChatState({ environmentId, input: { threadId } }));
  const resolve = useOrchestrationCommand(sandboxChatResolveSettle);
  const [remember, setRemember] = useState(false);
  const [busy, setBusy] = useState<CloudSandboxSettleChoice | null>(null);
  if (state.data?.settle !== "pending") return null;

  const answer = async (choice: CloudSandboxSettleChoice) => {
    setBusy(choice);
    await resolve({ environmentId, input: { threadId, choice, remember } });
    setBusy(null);
  };

  return (
    <div
      className="mx-auto mb-2 flex w-full max-w-3xl flex-col gap-2 rounded-lg border border-border bg-card px-3 py-2.5 text-sm"
      data-testid="sandbox-settle-card"
    >
      <div className="flex items-center gap-2 font-medium">
        <CloudIcon className="size-4 text-muted-foreground" />
        Keep this chat's sandbox?
      </div>
      <p className="text-muted-foreground text-xs">
        It is stopped now. Keeping its disk costs storage; a new message resumes it. Deleting
        removes it for good.
      </p>
      <div className="flex flex-wrap items-center gap-2">
        <Button size="sm" disabled={busy !== null} onClick={() => void answer("keep")}>
          Keep snapshot
        </Button>
        <Button
          size="sm"
          variant="destructive-outline"
          disabled={busy !== null}
          onClick={() => void answer("delete")}
        >
          Delete sandbox
        </Button>
        <label className="ml-auto flex items-center gap-1.5 text-muted-foreground text-xs">
          <Checkbox checked={remember} onCheckedChange={(checked) => setRemember(checked)} />
          Remember for this project
        </label>
      </div>
    </div>
  );
}

/** Opening a sandbox chat resumes its stopped sandbox, so the next message does not wait on it. */
export function useResumeSandboxChat(
  environmentId: EnvironmentId,
  threadId: ThreadId,
  enabled: boolean,
) {
  const run = useOrchestrationCommand(sandboxChatOpen, { reportFailure: false });
  const open = useEffectEvent((target: { environmentId: EnvironmentId; threadId: ThreadId }) => {
    void run({ environmentId: target.environmentId, input: { threadId: target.threadId } });
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- asks the server to resume this chat's sandbox once per opened thread
  useEffect(() => {
    if (enabled) open({ environmentId, threadId });
  }, [enabled, environmentId, threadId]);
}
