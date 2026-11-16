import { ManagedCloudSandboxResourceId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import "~/index.css";
import { render } from "../../test/render";
import { resolveSandboxPickerItems } from "@t3tools/client-runtime/state/cloudSandbox";
import { BranchToolbarEnvModeSelector } from "./BranchToolbarEnvModeSelector";

describe("BranchToolbarEnvModeSelector sandbox targets", () => {
  it("picks a sandbox from the same run-target picker and back to the checkout", async () => {
    const onSandboxTargetChange = vi.fn();
    const onEnvModeChange = vi.fn();
    const items = resolveSandboxPickerItems({
      hasProfile: true,
      current: { resourceId: ManagedCloudSandboxResourceId.make("open-vm"), label: "atlas" },
    });
    await using screen = await render(
      <BranchToolbarEnvModeSelector
        envLocked={false}
        effectiveEnvMode="local"
        activeWorktreePath={null}
        onEnvModeChange={onEnvModeChange}
        sandboxItems={items}
        sandboxTarget={null}
        onSandboxTargetChange={onSandboxTargetChange}
      />,
    );
    await userEvent.click(screen.getByRole("combobox", { name: "Workspace" }));
    await expect
      .element(page.getByRole("option", { name: "Current sandbox · atlas" }))
      .toBeVisible();
    await userEvent.click(page.getByRole("option", { name: "New sandbox" }));
    expect(onSandboxTargetChange).toHaveBeenLastCalledWith({ _tag: "new" });
    expect(onEnvModeChange).not.toHaveBeenCalled();
  });

  it("shows no sandbox group without a profile or an open sandbox", async () => {
    await using screen = await render(
      <BranchToolbarEnvModeSelector
        envLocked={false}
        effectiveEnvMode="local"
        activeWorktreePath={null}
        onEnvModeChange={vi.fn()}
        sandboxItems={resolveSandboxPickerItems({ hasProfile: false, current: null })}
      />,
    );
    await userEvent.click(screen.getByRole("combobox", { name: "Workspace" }));
    await expect.element(page.getByRole("option", { name: "New worktree" })).toBeVisible();
    expect(page.getByRole("option", { name: "New sandbox" }).elements()).toHaveLength(0);
  });
});
