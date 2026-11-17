import { ThreadId, type WorktreeSetupSnapshot } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";

import { act, fakeTimers, render } from "../../../test/render";
import { WorktreeSetupCard } from "./WorktreeSetupCard";

const NOW = new Date("2026-10-26T12:00:10.000Z");

function snapshot(overrides: Partial<WorktreeSetupSnapshot>): WorktreeSetupSnapshot {
  return {
    threadId: ThreadId.make("thread-1"),
    phase: "running",
    startedAt: "2026-10-26T12:00:00.000Z",
    endedAt: null,
    branch: "t3/feature",
    baseRef: "main",
    worktreePath: "/repo/.t3/worktrees/feature",
    setupScript: null,
    sandbox: null,
    stages: [
      {
        id: "fetch",
        status: "done",
        startedAt: "2026-10-26T12:00:00.000Z",
        endedAt: "2026-10-26T12:00:03.000Z",
        percent: null,
        detail: null,
        tail: [],
      },
      {
        id: "checkout",
        status: "running",
        startedAt: "2026-10-26T12:00:03.000Z",
        endedAt: null,
        percent: null,
        detail: null,
        tail: [],
      },
    ],
    error: null,
    sequence: 1,
    ...overrides,
  };
}

const card = (value: WorktreeSetupSnapshot) => (
  <WorktreeSetupCard snapshot={value} onCancel={null} onWorkLocally={null} onOpenTerminal={null} />
);

it("ticks the running stage while finished stages keep their duration", async () => {
  using _timers = fakeTimers();
  vi.setSystemTime(NOW);
  await using screen = await render(card(snapshot({})));
  // Assert synchronously: expect.element polls on timers these tests fake.
  const section = screen.getByRole("region", { name: "Worktree setup" });

  expect(section.element().textContent).toContain("Fetch base branch3s");
  expect(section.element().textContent).toContain("Check out files7s");

  await act(async () => {
    await vi.advanceTimersByTimeAsync(5_000);
  });
  expect(section.element().textContent).toContain("Fetch base branch3s");
  expect(section.element().textContent).toContain("Check out files12s");
});

it("stops a stage interrupted by the setup ending at the setup's end", async () => {
  using _timers = fakeTimers();
  vi.setSystemTime(NOW);
  const failed = snapshot({ phase: "failed", endedAt: "2026-10-26T12:00:05.000Z" });
  await using screen = await render(card(failed));
  const section = screen.getByRole("region", { name: "Worktree setup" });

  expect(section.element().textContent).toContain("Check out files2s");
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5_000);
  });
  expect(section.element().textContent).toContain("Check out files2s");
});

it("names sandbox snapshots after the sandbox", async () => {
  using _timers = fakeTimers();
  vi.setSystemTime(NOW);
  await using screen = await render(
    card(
      snapshot({
        branch: null,
        baseRef: null,
        worktreePath: null,
        setupScript: null,
        sandbox: { profile: "boat", size: "small", sandboxId: "vm-abc123" },
        stages: [
          {
            id: "provision",
            status: "running",
            startedAt: "2026-10-26T12:00:00.000Z",
            endedAt: null,
            percent: null,
            detail: null,
            tail: [],
          },
          {
            id: "connect",
            status: "pending",
            startedAt: null,
            endedAt: null,
            percent: null,
            detail: null,
            tail: [],
          },
          {
            id: "copy",
            status: "pending",
            startedAt: null,
            endedAt: null,
            percent: null,
            detail: null,
            tail: [],
          },
        ],
      }),
    ),
  );
  const section = screen.getByRole("region", { name: "Worktree setup" });

  expect(section.element().textContent).toContain("Create sandbox");
  expect(section.element().textContent).toContain("Connect to sandbox");
  expect(section.element().textContent).toContain("Copy project");
  expect(section.element().textContent).not.toContain("worktree");
  // Details names the follower being set up.
  await screen.getByRole("button", { name: "Details" }).click();
  expect(section.element().textContent).toContain("Profile");
  expect(section.element().textContent).toContain("boat");
  expect(section.element().textContent).toContain("vm-abc123");
  // A cancelled sandbox setup gets its own header copy.
  await using cancelled = await render(
    card(
      snapshot({
        phase: "cancelled",
        endedAt: "2026-10-26T12:00:05.000Z",
        stages: [
          {
            id: "provision",
            status: "skipped",
            startedAt: null,
            endedAt: null,
            percent: null,
            detail: null,
            tail: [],
          },
        ],
      }),
    ),
  );
  expect(cancelled.getByRole("region", { name: "Worktree setup" }).element().textContent).toContain(
    "Sandbox setup cancelled",
  );
});
