import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import "~/index.css";
import { render } from "../../../test/render";

const mocks = vi.hoisted(() => ({ settle: "pending", resolve: vi.fn() }));
vi.mock(import("~/state/query"), async (importOriginal) => ({
  ...(await importOriginal()),
  useEnvironmentQuery: (() => ({ data: { settle: mocks.settle } })) as never,
}));
vi.mock(import("~/state/use-orchestration-command"), async (importOriginal) => ({
  ...(await importOriginal()),
  useOrchestrationCommand: (() => mocks.resolve) as never,
}));

import { SandboxSettleCard } from "./SandboxSettleCard";

const environmentId = EnvironmentId.make("host");
const threadId = ThreadId.make("chat");

function setup(settle: string) {
  mocks.settle = settle;
  mocks.resolve.mockReset().mockResolvedValue({ _tag: "Success" });
}

describe("SandboxSettleCard", () => {
  it("keeps the sandbox and remembers the answer for the project", async () => {
    setup("pending");
    await using screen = await render(
      <SandboxSettleCard environmentId={environmentId} threadId={threadId} />,
    );
    await userEvent.click(screen.getByRole("checkbox"));
    await userEvent.click(screen.getByRole("button", { name: "Keep snapshot" }));
    expect(mocks.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, choice: "keep", remember: true },
    });
  });

  it("deletes without remembering", async () => {
    setup("pending");
    await using screen = await render(
      <SandboxSettleCard environmentId={environmentId} threadId={threadId} />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Delete sandbox" }));
    expect(mocks.resolve).toHaveBeenCalledWith({
      environmentId,
      input: { threadId, choice: "delete", remember: false },
    });
  });

  it("asks nothing once the chat's answer is in", async () => {
    setup("kept");
    await using screen = await render(
      <SandboxSettleCard environmentId={environmentId} threadId={threadId} />,
    );
    await expect.element(screen.getByTestId("sandbox-settle-card")).not.toBeInTheDocument();
  });
});
