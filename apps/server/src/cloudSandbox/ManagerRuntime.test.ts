import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  type ManagedCloudSandboxIssue,
  ManagedCloudSandboxResourceId,
  SandboxProfileId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as ManagerRuntime from "./ManagerRuntime.ts";
import * as Store from "./CloudSandboxStore.ts";

const baseRecord = (): Store.CloudSandboxRecord => ({
  resourceId: ManagedCloudSandboxResourceId.make("follower-1"),
  controllerEnvironmentId: EnvironmentId.make("source"),
  driverKind: "boat",
  profileId: SandboxProfileId.make("boat-main"),
  sandboxId: "sbx-1",
  parentResourceId: null,
  owner: {
    _tag: "task",
    hostEnvironmentId: EnvironmentId.make("source"),
    sourceThreadId: ThreadId.make("owner"),
    taskId: "task-1",
  },
  retention: "stop-and-retain",
  environmentId: null,
  label: "follower",
  recordingIds: [],
  lifecycle: "active",
  power: "running",
  intent: "connected",
  selection: { name: "test", size: "small", autoStopMinutes: 15 },
  snapshotName: null,
  snapshotReady: false,
  deletionOperationId: null,
  finalSpendUsd: null,
  enableLinkedEnvironments: false,
});

/** An in-memory stand-in for the sqlite store; the save count is the assertion surface. */
const makeStore = (seeded: ReadonlyArray<Store.CloudSandboxRecord> = []) => {
  const records = new Map<string, Store.CloudSandboxRecord>(
    seeded.map((entry) => [entry.resourceId as string, entry]),
  );
  let saves = 0;
  const store: ManagerRuntime.ManagerRuntime["store"] = {
    get: (resourceId: string) => Effect.succeed(Option.fromNullishOr(records.get(resourceId))),
    list: Effect.succeed([...records.values()]),
    save: (entry: Store.CloudSandboxRecord): Effect.Effect<void, Store.CloudSandboxManagerError> =>
      Effect.sync(() => {
        saves += 1;
        records.set(entry.resourceId, entry);
      }),
  };
  return { store, records, saveCount: () => saves };
};

const issue = (firstSeenAt: string): ManagedCloudSandboxIssue => ({
  cause: "connectivity",
  driverKind: "boat",
  profileId: "boat-main",
  message: "The managed cloud connection could not be established.",
  firstSeenAt,
});

it.effect("stampIssue saves a fresh span once and keeps it identical on repeats", () =>
  Effect.gen(function* () {
    const { store, records, saveCount } = makeStore();
    yield* ManagerRuntime.stampIssue(store, baseRecord(), issue("2026-02-09T10:00:00Z"));
    expect(saveCount()).toBe(1);
    expect(records.get("follower-1")?.issue).toEqual(issue("2026-02-09T10:00:00Z"));
    // A repeat of the span, reworded or re-dated, saves nothing: the record's
    // span is immutable, so the card reading it can never drift.
    const recorded = records.get("follower-1")!;
    yield* ManagerRuntime.stampIssue(store, recorded, {
      ...issue("2026-02-09T11:00:00Z"),
      message: "An updated wording.",
    });
    expect(saveCount()).toBe(1);
    expect(records.get("follower-1")?.issue).toEqual(issue("2026-02-09T10:00:00Z"));
  }),
);

it.effect("stampConnectIssue classifies the connect failure's cause", () =>
  Effect.gen(function* () {
    const { store, records, saveCount } = makeStore([baseRecord()]);
    const resourceId = ManagedCloudSandboxResourceId.make("follower-1");
    yield* ManagerRuntime.stampConnectIssue(
      store,
      resourceId,
      new Store.CloudSandboxManagerError({ reason: "transport" }),
    );
    expect(records.get("follower-1")?.issue?.cause).toBe("connectivity");
    yield* ManagerRuntime.stampConnectIssue(
      store,
      resourceId,
      new Store.CloudSandboxManagerError({ reason: "provider" }),
    );
    expect(records.get("follower-1")?.issue?.cause).toBe("provider-auth");
    yield* ManagerRuntime.stampConnectIssue(
      store,
      resourceId,
      new Store.CloudSandboxManagerError({ reason: "budget" }),
    );
    expect(records.get("follower-1")?.issue?.cause).toBe("allocation");
    // A missing record reads only a logged warning.
    yield* ManagerRuntime.stampConnectIssue(
      store,
      ManagedCloudSandboxResourceId.make("ghost"),
      new Store.CloudSandboxManagerError({ reason: "provider" }),
    );
    expect(saveCount()).toBe(3);
  }),
);

it.effect("stampBindIssue stamps the refused bind on a recovered record", () =>
  Effect.gen(function* () {
    const { store, records } = makeStore([baseRecord()]);
    yield* ManagerRuntime.stampBindIssue(
      store,
      ManagedCloudSandboxResourceId.make("follower-1"),
      new Store.CloudSandboxManagerError({ reason: "ownership" }),
    );
    expect(records.get("follower-1")?.issue?.cause).toBe("bind");
  }),
);

it.effect("withoutIssue strips the issue so recovery saves a clean span", () =>
  Effect.gen(function* () {
    const { store, records, saveCount } = makeStore();
    yield* ManagerRuntime.stampIssue(store, baseRecord(), issue("2026-02-09T10:00:00Z"));
    expect(saveCount()).toBe(1);
    const clean = ManagerRuntime.withoutIssue(records.get("follower-1")!);
    expect("issue" in clean).toBe(false);
    // After the recovery, the same cause is a span of its own: it saves anew.
    yield* ManagerRuntime.stampIssue(store, clean, issue("2026-02-09T11:00:00Z"));
    expect(saveCount()).toBe(2);
    expect(records.get("follower-1")?.issue?.firstSeenAt).toBe("2026-02-09T11:00:00Z");
  }),
);
