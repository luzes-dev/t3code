import type { ReactNode } from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";

vi.mock("react-native", () => ({
  View: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children: ReactNode }) => <span>{children}</span>,
}));
vi.mock("../../components/ui/AppSymbol", () => ({
  SymbolView: () => null,
}));
vi.mock("../../components/ProviderIcon", () => ({
  ProviderIcon: () => null,
  ProviderInstanceIcon: () => null,
}));
vi.mock("../../state/entities", () => ({
  useEnvironmentServerConfig: (() => null) as never,
  useProject: (() => null) as never,
  useThreadShell: (() => null) as never,
}));

import { SubagentRow } from "./SubagentRow";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const environmentId = EnvironmentId.make("environment");

function mount(ui: ReactNode): { unmount: () => void; text: () => string } {
  const container = document.createElement("div");
  const root = createRoot(container);
  act(() => root.render(ui));
  return {
    unmount: () => act(() => root.unmount()),
    text: () => container.textContent ?? "",
  };
}

const remoteTask = {
  threadId: ThreadId.make("parent"),
  childThreadId: null,
  model: null,
  modelSelection: undefined,
  driver: ProviderDriverKind.make("codex"),
  providerInstanceId: ProviderInstanceId.make("provider"),
  title: "Run remote task",
  prompt: "Run remote task",
  status: "completed" as const,
  progress: undefined,
  result: null,
  remoteChild: {
    environmentId: EnvironmentId.make("remote-env"),
    threadId: ThreadId.make("remote-thread"),
    label: "Remote VM",
  },
};

it("shows the degraded follower sandbox's issue beside a remote task's metadata", () => {
  const screen = mount(
    <SubagentRow
      environmentId={environmentId}
      subagent={{
        ...remoteTask,
        remoteChild: {
          ...remoteTask.remoteChild,
          sandboxIssue: {
            cause: "provider-auth",
            driverKind: "boat",
            profileId: "boat-demo",
            message: "The cloud provider could not complete this operation.",
            firstSeenAt: "2026-11-13T00:00:00.000Z",
          },
        },
      }}
      elapsed={null}
    />,
  );
  try {
    expect(screen.text()).toContain("on Remote VM");
    expect(screen.text()).toContain(
      "Cloud token refused: The cloud provider could not complete this operation.",
    );
  } finally {
    screen.unmount();
  }
});

it("stays quiet on the issue line when the follower sandbox is healthy", () => {
  const screen = mount(
    <SubagentRow environmentId={environmentId} subagent={remoteTask} elapsed={null} />,
  );
  try {
    expect(screen.text()).toContain("on Remote VM");
    expect(screen.text()).not.toContain("Cloud token refused");
  } finally {
    screen.unmount();
  }
});
