import {
  EnvironmentId,
  ManagedCloudSandboxResourceId,
  SandboxProfileId,
  ThreadId,
  type CloudSandboxProfileStatus,
  type ManagedCloudSandboxResource,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Result from "effect/Result";
import { AsyncResult } from "effect/reactivity";
import { describe, expect, it } from "vite-plus/test";

import {
  cloudSandboxControlOperation,
  cloudSandboxFailureMessage,
  countRunningCloudSandboxes,
  describeCloudSandbox,
  describeCloudSandboxProfile,
  describeManagedCloudSandboxIssue,
  isSelectableEnvironment,
  managedSandboxIssueRollup,
  offersCloudSandboxControl,
  parseSandboxProfileLimits,
  selectableEnvironmentIds,
} from "./cloudSandbox.ts";

const profileStatus = (overrides: {
  readonly defaultSize?: CloudSandboxProfileStatus["profile"]["defaultSize"];
  readonly autoStopMinutes?: number;
  readonly monthlyBudgetUsd?: number | null;
  readonly spendUsd?: number | null;
  readonly maxConcurrentSandboxes?: number;
}): CloudSandboxProfileStatus => ({
  profile: {
    profileId: SandboxProfileId.make("boat-main"),
    name: "boat.dev",
    driverKind: "boat",
    apiKeySet: true,
    defaultSize: "defaultSize" in overrides ? overrides.defaultSize : "default",
    autoStopMinutes: overrides.autoStopMinutes ?? 60,
    template: null,
    monthlyBudgetUsd: overrides.monthlyBudgetUsd ?? null,
    maxConcurrentSandboxes: overrides.maxConcurrentSandboxes ?? 3,
  },
  machines: [
    { id: "small", label: "Small", hourlyUsd: 0.018 },
    { id: "default", label: "Standard", hourlyUsd: 0.036 },
    { id: "large", label: "Large", hourlyUsd: 0.072 },
    { id: "xlarge", label: "Extra Large", hourlyUsd: 0.144 },
  ],
  spend:
    overrides.spendUsd === null || overrides.spendUsd === undefined
      ? null
      : { period: "2026-11", usd: overrides.spendUsd, running: 0 },
});

const resource = (overrides: {
  readonly id: string;
  readonly phase: ManagedCloudSandboxResource["phase"];
  readonly profileId?: string;
  readonly sandboxId?: string | null;
  readonly owner?: ManagedCloudSandboxResource["owner"];
  readonly environmentId?: ManagedCloudSandboxResource["environmentId"];
  readonly issue?: ManagedCloudSandboxResource["issue"];
}): ManagedCloudSandboxResource => ({
  resourceId: ManagedCloudSandboxResourceId.make(overrides.id),
  controllerEnvironmentId: EnvironmentId.make("controller"),
  driverKind: "boat",
  profileId: SandboxProfileId.make(overrides.profileId ?? "boat-main"),
  sandboxId: overrides.sandboxId === undefined ? "sbx-1" : overrides.sandboxId,
  parentResourceId: null,
  owner: overrides.owner ?? ({ _tag: "user" } as const),
  retention:
    overrides.owner === undefined || overrides.owner._tag === "user"
      ? ("manual" as const)
      : ("stop-and-retain" as const),
  phase: overrides.phase,
  environmentId: overrides.environmentId ?? null,
  label: overrides.id,
  recordingIds: [],
  ...(overrides.issue === undefined ? {} : { issue: overrides.issue }),
});

describe("selectable environments", () => {
  it("selects user-owned resources and skips thread and task followers", () => {
    const user = resource({ id: "main", phase: "connected" });
    const threadFollower = resource({
      id: "thread-follower",
      phase: "connected",
      owner: {
        _tag: "thread",
        hostEnvironmentId: EnvironmentId.make("controller"),
        threadId: ThreadId.make("thread-1"),
      },
      environmentId: EnvironmentId.make("env-thread"),
    });
    const taskFollower = resource({
      id: "task-follower",
      phase: "connected",
      owner: {
        _tag: "task",
        hostEnvironmentId: EnvironmentId.make("controller"),
        sourceThreadId: ThreadId.make("thread-1"),
        taskId: "task-1",
      },
      environmentId: EnvironmentId.make("env-task"),
    });
    expect(isSelectableEnvironment(user)).toBe(true);
    expect(isSelectableEnvironment(threadFollower)).toBe(false);
    expect(isSelectableEnvironment(taskFollower)).toBe(false);
    expect(selectableEnvironmentIds([user, threadFollower, taskFollower])).toEqual(new Set());
    // A registered user-owned environment is offered; followers never are,
    // even when their sandbox is running.
    const registeredUserEnvironment = EnvironmentId.make("env-main");
    const registered = resource({
      id: "main",
      phase: "connected",
      environmentId: registeredUserEnvironment,
    });
    expect(selectableEnvironmentIds([registered, taskFollower])).toEqual(
      new Set([registeredUserEnvironment]),
    );
  });
});

describe("describeCloudSandboxProfile", () => {
  it("summarises size, auto-stop, budget, and concurrency", () => {
    expect(
      describeCloudSandboxProfile(
        profileStatus({ monthlyBudgetUsd: 50, spendUsd: 12.5, autoStopMinutes: 120 }),
        1,
      ),
    ).toBe(
      "Standard · Auto-stop 2 h · $12.50 from deleted sandboxes of $50.00 this month · 1 running of 3 allowed",
    );
  });

  it("names the missing default size, uses minutes for partial hours, and reads no budget as no limit", () => {
    expect(
      describeCloudSandboxProfile(profileStatus({ defaultSize: null, autoStopMinutes: 90 }), 0),
    ).toBe("No default size · Auto-stop 90 min · No budget limit · 0 running of 3 allowed");
  });

  it("reads zero spend when the period has no spend record", () => {
    expect(describeCloudSandboxProfile(profileStatus({ monthlyBudgetUsd: 20 }), 0)).toContain(
      "$0.00 from deleted sandboxes of $20.00 this month",
    );
  });

  it("shows spend between zero and a cent as under one cent", () => {
    expect(
      describeCloudSandboxProfile(profileStatus({ monthlyBudgetUsd: 20, spendUsd: 0.00118 }), 0),
    ).toContain("<$0.01 from deleted sandboxes of $20.00 this month");
  });
});

describe("describeCloudSandbox", () => {
  it("joins the phase label and sandbox id, and omits a missing id", () => {
    expect(describeCloudSandbox(resource({ id: "a", phase: "connected" }))).toBe("Running · sbx-1");
    expect(describeCloudSandbox(resource({ id: "a", phase: "stopped", sandboxId: null }))).toBe(
      "Stopped",
    );
  });
});

describe("countRunningCloudSandboxes", () => {
  it("counts a profile's sandboxes that are neither stopped nor failed", () => {
    const resources = [
      resource({ id: "running", phase: "connected" }),
      resource({ id: "stopped", phase: "stopped" }),
      resource({ id: "failed", phase: "failed" }),
      resource({ id: "other", phase: "connected", profileId: "other-profile" }),
    ];
    expect(countRunningCloudSandboxes(resources, SandboxProfileId.make("boat-main"))).toBe(1);
  });
});

describe("cloudSandboxControlOperation", () => {
  it("resumes a stopped sandbox and stops any other", () => {
    expect(cloudSandboxControlOperation(resource({ id: "a", phase: "stopped" }))).toEqual({
      _tag: "resume",
    });
    expect(cloudSandboxControlOperation(resource({ id: "a", phase: "connected" }))).toEqual({
      _tag: "stop",
    });
  });

  it("offers stop and resume only in the phases where they apply", () => {
    expect(offersCloudSandboxControl("connected")).toBe(true);
    expect(offersCloudSandboxControl("stopped")).toBe(true);
    expect(offersCloudSandboxControl("deleting")).toBe(false);
    expect(offersCloudSandboxControl("failed")).toBe(false);
  });
});

describe("parseSandboxProfileLimits", () => {
  const valid = { monthlyBudgetUsd: "", maxConcurrentSandboxes: 3, template: "" };

  it("reads a blank budget and template as no limit and no snapshot", () => {
    expect(parseSandboxProfileLimits(valid)).toEqual(
      Result.succeed({ monthlyBudgetUsd: null, maxConcurrentSandboxes: 3, template: null }),
    );
  });

  it("parses a decimal budget and trims the template", () => {
    expect(
      parseSandboxProfileLimits({ ...valid, monthlyBudgetUsd: " 12.5 ", template: " snap-1 " }),
    ).toEqual(
      Result.succeed({ monthlyBudgetUsd: 12.5, maxConcurrentSandboxes: 3, template: "snap-1" }),
    );
  });

  it.each(["0", "-4", "abc", "1e3", "12,5"])("rejects the budget %s", (monthlyBudgetUsd) => {
    expect(parseSandboxProfileLimits({ ...valid, monthlyBudgetUsd })).toEqual(
      Result.fail("Monthly budget must be a dollar amount above 0, or blank for no limit."),
    );
  });

  it.each([0, 101, 2.5, Number.NaN])(
    "rejects %s concurrent sandboxes",
    (maxConcurrentSandboxes) => {
      expect(parseSandboxProfileLimits({ ...valid, maxConcurrentSandboxes })).toEqual(
        Result.fail("Concurrent sandboxes must be a whole number from 1 to 100."),
      );
    },
  );
});

describe("cloudSandboxFailureMessage", () => {
  it("returns the server detail, then the error message, and null without a failure", () => {
    expect(cloudSandboxFailureMessage(AsyncResult.success(undefined), "fallback")).toBeNull();
    expect(
      cloudSandboxFailureMessage(
        AsyncResult.failure(
          Cause.fail({ _tag: "CloudSandboxProfileError", detail: "Key rejected" }),
        ),
        "fallback",
      ),
    ).toBe("Key rejected");
    expect(
      cloudSandboxFailureMessage(AsyncResult.failure(Cause.fail(new Error("Offline"))), "fallback"),
    ).toBe("Offline");
  });
});

describe("managedSandboxIssueRollup", () => {
  const issue = {
    cause: "connectivity",
    driverKind: "boat",
    profileId: "boat-main",
    message: "The managed cloud connection could not be established.",
    firstSeenAt: "2026-02-09T10:00:00Z",
  } as const;
  const taskOwner = {
    _tag: "task",
    hostEnvironmentId: EnvironmentId.make("controller"),
    sourceThreadId: ThreadId.make("thread-1"),
    taskId: "node-1",
  } as const;

  it("collapses every follower of one dead profile token into one warning", () => {
    const rollup = managedSandboxIssueRollup([
      resource({
        id: "a",
        phase: "connected",
        owner: taskOwner,
        issue: { ...issue, firstSeenAt: "2026-02-09T10:05:00Z" },
      }),
      resource({
        id: "b",
        phase: "failed",
        owner: taskOwner,
        issue: { ...issue, message: "The peer connection dropped." },
      }),
      resource({
        id: "c",
        phase: "failed",
        owner: taskOwner,
        issue: { ...issue, firstSeenAt: "2026-02-09T09:30:00Z" },
      }),
    ]);
    expect(rollup).toHaveLength(1);
    expect(rollup[0]?.firstSeenAt).toBe("2026-02-09T09:30:00Z");
    expect(rollup[0]?.affectedResourceIds.map(String)).toEqual(["a", "b", "c"]);
    expect(rollup[0]?.key).toBe("boat:boat-main:connectivity");
  });

  it("keeps user-owned sandboxes out and healthy rows silent", () => {
    const rollup = managedSandboxIssueRollup([
      resource({ id: "own", phase: "failed", issue: { ...issue, cause: "allocation" } }),
      resource({ id: "healthy", phase: "connected", owner: taskOwner }),
    ]);
    expect(rollup).toEqual([]);
  });

  it("splits distinct causes of the same profile into separate warnings", () => {
    const rollup = managedSandboxIssueRollup([
      resource({ id: "a", phase: "failed", owner: taskOwner, issue }),
      resource({
        id: "b",
        phase: "failed",
        owner: taskOwner,
        issue: { ...issue, cause: "provider-auth" },
      }),
    ]);
    expect(rollup.map((entry) => entry.cause).toSorted()).toEqual([
      "connectivity",
      "provider-auth",
    ]);
  });

  it("describes an issue with its cause label and the server's message", () => {
    expect(describeManagedCloudSandboxIssue(issue)).toBe(
      "Connection lost: The managed cloud connection could not be established.",
    );
  });
});
