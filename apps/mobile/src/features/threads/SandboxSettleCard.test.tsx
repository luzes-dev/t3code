import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import type { ReactNode } from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { describe, expect, it, vi } from "vite-plus/test";

type ButtonProps = { label: string; disabled?: boolean; onPress: () => void };

const mocks = vi.hoisted(() => ({ settle: "pending", resolve: vi.fn() }));
vi.mock("react-native", () => ({
  Pressable: ({ children, onPress }: { children: ReactNode; onPress: () => void }) => (
    <button type="button" onClick={onPress}>
      {children}
    </button>
  ),
  View: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children: ReactNode }) => <span>{children}</span>,
}));
vi.mock("../../components/ui/Button", () => ({
  Button: ({ label, disabled, onPress }: ButtonProps) => (
    <button type="button" disabled={disabled} onClick={onPress}>
      {label}
    </button>
  ),
}));
vi.mock("../../state/cloudSandboxes", () => ({
  sandboxChatResolveSettle: {},
  sandboxChatState: () => ({}),
}));
vi.mock("../../state/query", () => ({
  useEnvironmentQuery: () => ({ data: { settle: mocks.settle } }),
}));
vi.mock("../../state/use-atom-command", () => ({ useAtomCommand: () => mocks.resolve }));

import { SandboxSettleCard } from "./SandboxSettleCard";

const environmentId = "host" as EnvironmentId;
const threadId = "chat" as ThreadId;

function mount(settle: string) {
  mocks.settle = settle;
  mocks.resolve.mockReset().mockResolvedValue({ _tag: "Success" });
  const container = document.createElement("div");
  const root = createRoot(container);
  act(() => root.render(<SandboxSettleCard environmentId={environmentId} threadId={threadId} />));
  return { container, [Symbol.dispose]: () => act(() => root.unmount()) };
}

const button = (container: HTMLElement, text: string) =>
  [...container.querySelectorAll("button")].find((element) => element.textContent?.includes(text))!;

describe("SandboxSettleCard", () => {
  it("keeps the sandbox and remembers the answer for the project", async () => {
    using view = mount("pending");
    await act(async () => button(view.container, "Remember").click());
    await act(async () => button(view.container, "Keep snapshot").click());
    expect(mocks.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, choice: "keep", remember: true },
    });
  });

  it("deletes without remembering", async () => {
    using view = mount("pending");
    await act(async () => button(view.container, "Delete sandbox").click());
    expect(mocks.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, choice: "delete", remember: false },
    });
  });

  it("asks nothing once the chat's answer is in", () => {
    using view = mount("deleted");
    expect(view.container.textContent).toBe("");
  });
});
