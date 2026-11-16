import type { CloudSandboxSettleChoice, EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useEffect, useEffectEvent, useState } from "react";
import { Pressable, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import {
  sandboxChatOpen,
  sandboxChatResolveSettle,
  sandboxChatState,
} from "../../state/cloudSandboxes";
import { useEnvironmentQuery } from "../../state/query";
import { useAtomCommand } from "../../state/use-atom-command";

/** Asks once whether a settled chat keeps its disposable sandbox (stopped) or deletes it. */
export function SandboxSettleCard({
  environmentId,
  threadId,
}: {
  environmentId: EnvironmentId;
  threadId: ThreadId;
}) {
  const state = useEnvironmentQuery(sandboxChatState({ environmentId, input: { threadId } }));
  const resolve = useAtomCommand(sandboxChatResolveSettle);
  const [remember, setRemember] = useState(false);
  const [busy, setBusy] = useState(false);
  if (state.data?.settle !== "pending") return null;

  const answer = async (choice: CloudSandboxSettleChoice) => {
    setBusy(true);
    await resolve({ environmentId, input: { threadId, choice, remember } });
    setBusy(false);
  };

  return (
    <View className="mx-3 mb-2 gap-2 rounded-xl border border-border bg-screen p-3">
      <Text className="text-sm font-t3-medium text-foreground">Keep this chat's sandbox?</Text>
      <Text className="text-xs text-foreground-muted">
        It is stopped now. Keeping its disk costs storage; a new message resumes it.
      </Text>
      <View className="flex-row gap-2">
        <Button
          label="Keep snapshot"
          size="compact"
          disabled={busy}
          onPress={() => void answer("keep")}
        />
        <Button
          label="Delete sandbox"
          size="compact"
          tone="danger"
          disabled={busy}
          onPress={() => void answer("delete")}
        />
      </View>
      <Pressable
        accessibilityRole="checkbox"
        accessibilityState={{ checked: remember }}
        onPress={() => setRemember((value) => !value)}
      >
        <Text className="text-xs text-foreground-muted">
          {remember ? "☑" : "☐"} Remember for this project
        </Text>
      </Pressable>
    </View>
  );
}

/** Opening a sandbox chat resumes its stopped sandbox, so the next message does not wait on it. */
export function useResumeSandboxChat(
  environmentId: EnvironmentId,
  threadId: ThreadId,
  enabled: boolean,
) {
  const run = useAtomCommand(sandboxChatOpen, { reportFailure: false });
  const open = useEffectEvent((target: { environmentId: EnvironmentId; threadId: ThreadId }) => {
    void run({ environmentId: target.environmentId, input: { threadId: target.threadId } });
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- asks the server to resume this chat's sandbox once per opened thread
  useEffect(() => {
    if (enabled) open({ environmentId, threadId });
  }, [enabled, environmentId, threadId]);
}
