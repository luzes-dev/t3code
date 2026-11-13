import { Atom, AsyncResult } from "effect/reactivity";
import * as Cause from "effect/Cause";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { CloudSandboxProfileError } from "@t3tools/contracts";
import { page, userEvent } from "vite-plus/test/browser";
import { describe, expect, it, vi } from "vite-plus/test";

import { render } from "../../../test/render";
import { AppAtomRegistryProvider } from "../../rpc/atomRegistry";

const mocks = vi.hoisted(() => ({
  operation: vi.fn<() => Promise<AtomCommandResult<unknown, unknown>>>(),
  deleteSandbox: vi.fn<() => Promise<AtomCommandResult<unknown, unknown>>>(),
  connectSandbox: vi.fn<() => Promise<AtomCommandResult<unknown, unknown>>>(),
  saveProfile: vi.fn<() => Promise<AtomCommandResult<unknown, unknown>>>(),
  deleteProfile: vi.fn<() => Promise<AtomCommandResult<unknown, unknown>>>(),
  removeEnvironment: vi.fn<() => Promise<AtomCommandResult<unknown, unknown>>>(),
  confirm: vi.fn(async (_message: string, _options?: unknown) => true),
  refresh: vi.fn(),
  profiles: null as null | { readonly profiles: ReadonlyArray<unknown> },
  sandboxes: null as null | ReadonlyArray<unknown>,
  issues: null as null | ReadonlyArray<unknown>,
  environments: [] as ReadonlyArray<{ readonly environmentId: string }>,
}));

vi.mock(import("../../state/use-atom-command"), async (original) => ({
  ...(await original()),
  useAtomCommand: ((command: unknown) => command) as never,
}));
vi.mock(import("~/confirmDialog"), async (original) => ({
  ...(await original()),
  requestConfirmDialog: mocks.confirm as never,
}));
vi.mock(import("~/connection/cloudSandbox"), async (original) => ({
  ...(await original()),
  // The command doubles as its own hook result, so it carries the permission atom too.
  cloudSandboxOperation: Object.assign(mocks.operation, {
    permissionAtom: () => Atom.make(true),
  }) as never,
  deleteManagedSandbox: mocks.deleteSandbox as never,
  managedSandboxesAtom: ((environmentId: unknown) => ({ sandboxesFor: environmentId })) as never,
  managedSandboxIssuesAtom: ((environmentId: unknown) => ({
    issuesFor: environmentId,
  })) as never,
}));
vi.mock(import("~/connection/onboarding"), async (original) => ({
  ...(await original()),
  connectCloudSandboxEnvironment: mocks.connectSandbox as never,
}));
vi.mock(import("~/state/cloudSandboxProfiles"), () => ({
  // A tagged input marks the profile list query; the sandbox query is tagged by its family.
  sandboxProfilesList: ((input: unknown) => input) as never,
  sandboxProfilesSave: Object.assign(mocks.saveProfile, {
    permissionAtom: () => Atom.make(true),
  }) as never,
  sandboxProfilesDelete: Object.assign(mocks.deleteProfile, {
    permissionAtom: () => Atom.make(true),
  }) as never,
}));
vi.mock(import("~/connection/catalog"), async (original) => {
  const actual = await original();
  return {
    ...actual,
    environmentCatalog: { ...actual.environmentCatalog, remove: mocks.removeEnvironment as never },
  };
});
vi.mock(import("~/state/environments"), async (original) => ({
  ...(await original()),
  usePrimaryEnvironmentId: (() => "environment-primary-1") as never,
  useEnvironments: (() => ({ environments: mocks.environments })) as never,
}));
vi.mock(import("~/state/query"), async (original) => ({
  ...(await original()),
  useEnvironmentQuery: ((query: unknown) => {
    const data =
      typeof query === "object" && query !== null
        ? "input" in query
          ? mocks.profiles
          : "sandboxesFor" in query
            ? mocks.sandboxes
            : "issuesFor" in query
              ? mocks.issues
              : null
        : null;
    return {
      data,
      dataUpdatedAt: 0,
      error: null,
      failure: null,
      isPending: false,
      isSuccess: data !== null,
      refresh: mocks.refresh,
    };
  }) as never,
}));

import { CloudSandboxesSettings } from "./CloudSandboxesSettings";

const boatProfile = {
  profile: {
    profileId: "boat-demo",
    name: "Boat demo",
    driverKind: "boat",
    apiKeySet: true,
    defaultSize: "default",
    autoStopMinutes: 120,
    template: null,
    monthlyBudgetUsd: 25,
    maxConcurrentSandboxes: 3,
  },
  spend: { period: "2026-11", usd: 4.5, running: 1 },
  machines: [
    { id: "small", label: "Small", hourlyUsd: 0.018 },
    { id: "default", label: "Standard", hourlyUsd: 0.036 },
    { id: "large", label: "Large", hourlyUsd: 0.072 },
    { id: "xlarge", label: "Extra Large", hourlyUsd: 0.144 },
  ],
};

const connectedSandbox = {
  resourceId: "resource-1",
  controllerEnvironmentId: "environment-primary-1",
  driverKind: "boat",
  profileId: "boat-demo",
  sandboxId: "sbx-1",
  parentResourceId: null,
  phase: "connected",
  environmentId: null,
  label: "Feature sandbox",
  recordingIds: [],
};

const followerIssue = {
  cause: "provider-auth",
  driverKind: "boat",
  profileId: "boat-demo",
  message: "The cloud provider could not complete this operation.",
  firstSeenAt: "2026-11-13T00:00:00.000Z",
};

const failedFollower = {
  ...connectedSandbox,
  resourceId: "follower-1",
  sandboxId: null,
  phase: "failed",
  label: "Follower sandbox",
  owner: { _tag: "task", hostEnvironmentId: "environment-primary-1" },
  issue: followerIssue,
};

const failedFollowerTwo = {
  ...connectedSandbox,
  resourceId: "follower-2",
  sandboxId: null,
  phase: "failed",
  label: "Second follower",
  owner: { _tag: "task", hostEnvironmentId: "environment-primary-1" },
  issue: { ...followerIssue, message: "The peer connection dropped." },
};

const healthyFollower = {
  ...connectedSandbox,
  resourceId: "follower-3",
  label: "Healthy follower",
  owner: { _tag: "task", hostEnvironmentId: "environment-primary-1" },
};

function setup({
  profiles,
  sandboxes,
  issues = [],
}: {
  readonly profiles: ReadonlyArray<unknown>;
  readonly sandboxes: ReadonlyArray<unknown>;
  readonly issues?: ReadonlyArray<unknown>;
}) {
  mocks.profiles = { profiles };
  mocks.sandboxes = sandboxes;
  mocks.issues = issues;
  mocks.operation.mockReset();
  mocks.operation.mockResolvedValue(AsyncResult.success(undefined));
  mocks.deleteSandbox.mockReset();
  mocks.deleteSandbox.mockResolvedValue(AsyncResult.success(undefined));
  mocks.deleteProfile.mockReset();
  mocks.removeEnvironment.mockReset();
  mocks.removeEnvironment.mockResolvedValue(AsyncResult.success(undefined));
  mocks.environments = [];
  mocks.saveProfile.mockReset();
  mocks.confirm.mockReset();
  mocks.confirm.mockResolvedValue(true);
  mocks.refresh.mockClear();
}

describe("CloudSandboxesSettings", () => {
  it("renders nothing without profiles or sandboxes", async () => {
    setup({ profiles: [], sandboxes: [] });
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await expect.element(page.getByText("Cloud sandboxes")).not.toBeInTheDocument();
  });

  it("lists a profile with its spend and a sandbox with its phase", async () => {
    setup({ profiles: [boatProfile], sandboxes: [connectedSandbox] });
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await expect
      .element(page.getByRole("heading", { name: "Cloud sandboxes" }))
      .toBeInTheDocument();
    await expect.element(page.getByText("Boat demo")).toBeInTheDocument();
    await expect
      .element(
        page.getByText(
          "Standard · Auto-stop 2 h · $4.50 from deleted sandboxes of $25.00 this month · 1 running of 3 allowed",
        ),
      )
      .toBeInTheDocument();
    await expect.element(page.getByText("Feature sandbox")).toBeInTheDocument();
    await expect.element(page.getByText("Running · sbx-1")).toBeInTheDocument();
  });

  it("stops a connected sandbox and refreshes the list", async () => {
    setup({ profiles: [], sandboxes: [connectedSandbox] });
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await userEvent.click(page.getByRole("button", { name: "Stop" }));

    expect(mocks.operation).toHaveBeenCalledWith({
      environmentId: "environment-primary-1",
      input: { resourceId: "resource-1", operation: { _tag: "stop" } },
    });
    await vi.waitFor(() => expect(mocks.refresh).toHaveBeenCalledOnce());
  });

  it("deletes a created sandbox only after the user confirms", async () => {
    setup({ profiles: [], sandboxes: [connectedSandbox] });
    mocks.confirm.mockResolvedValueOnce(false);
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await userEvent.click(page.getByRole("button", { name: "Delete" }));
    await vi.waitFor(() =>
      expect(mocks.confirm).toHaveBeenCalledWith(
        "Deletes this sandbox and its disk. Unsaved work in it is lost.",
        { variant: "destructive" },
      ),
    );

    await userEvent.click(page.getByRole("button", { name: "Delete" }));
    await vi.waitFor(() =>
      expect(mocks.deleteSandbox).toHaveBeenCalledExactlyOnceWith({
        controllerEnvironmentId: "environment-primary-1",
        resourceId: "resource-1",
      }),
    );
  });

  it("removes the environment of a registered sandbox once it is deleted", async () => {
    setup({
      profiles: [],
      sandboxes: [{ ...connectedSandbox, environmentId: "environment-sandbox-1" }],
    });
    mocks.environments = [{ environmentId: "environment-sandbox-1" }];
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await userEvent.click(page.getByRole("button", { name: "Delete" }));

    await vi.waitFor(() =>
      expect(mocks.removeEnvironment).toHaveBeenCalledExactlyOnceWith("environment-sandbox-1"),
    );
    expect(mocks.deleteSandbox).toHaveBeenCalledOnce();
  });

  it("shows the in-use detail when a profile still has sandboxes", async () => {
    setup({ profiles: [boatProfile], sandboxes: [] });
    mocks.deleteProfile.mockResolvedValueOnce(
      AsyncResult.failure(
        Cause.fail(
          new CloudSandboxProfileError({
            reason: "in-use",
            detail: "Sandboxes provisioned from this profile still exist.",
          }),
        ),
      ),
    );
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await userEvent.click(page.getByRole("button", { name: "Remove" }));

    await expect
      .element(page.getByText("Sandboxes provisioned from this profile still exist."))
      .toBeInTheDocument();
    expect(mocks.deleteProfile).toHaveBeenCalledWith({
      environmentId: "environment-primary-1",
      input: { profileId: "boat-demo" },
    });
  });

  it("shows one collapsed banner for degraded followers and marks the failing rows", async () => {
    setup({
      profiles: [boatProfile],
      sandboxes: [failedFollower, failedFollowerTwo, healthyFollower],
      issues: [
        {
          key: "boat:boat-demo:provider-auth",
          cause: "provider-auth",
          driverKind: "boat",
          profileId: "boat-demo",
          message: followerIssue.message,
          firstSeenAt: followerIssue.firstSeenAt,
          affectedResourceIds: ["follower-1", "follower-2"],
        },
      ],
    });
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await expect.element(page.getByText("Follower issues")).toBeInTheDocument();
    // The same describe line appears twice: once on the collapsed banner, once
    // on the failing follower's own row.
    expect(
      await page
        .getByText("Cloud token refused: The cloud provider could not complete this operation.")
        .elements(),
    ).toHaveLength(2);
    await expect
      .element(page.getByText(/^2 follower sandboxes affected since/))
      .toBeInTheDocument();
    // The second failing follower's row shows its own issue line only.
    expect(
      await page.getByText("Cloud token refused: The peer connection dropped.").elements(),
    ).toHaveLength(1);
  });

  it("omits the follower-issues banner when nothing is degraded", async () => {
    setup({ profiles: [boatProfile], sandboxes: [healthyFollower] });
    await using _screen = await render(
      <AppAtomRegistryProvider>
        <CloudSandboxesSettings />
      </AppAtomRegistryProvider>,
    );

    await expect.element(page.getByText("Follower issues")).not.toBeInTheDocument();
  });
});
