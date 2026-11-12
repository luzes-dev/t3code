import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { HttpClient } from "effect/http";
import { CloudSandboxDriverFactory, CloudSandboxError } from "@t3tools/cloud-sandbox";
import type {
  CloudSandboxProfileError,
  ManagedCloudSandboxIssue,
  ManagedCloudSandboxResource,
  ManagedCloudSandboxResourceId,
  SandboxProfileId,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import type { ManagedCloudSandboxRoute } from "./CloudSandboxManager.ts";
import * as Profiles from "./CloudSandboxProfiles.ts";
import * as Store from "./CloudSandboxStore.ts";
import * as Transport from "./CloudSandboxTransport.ts";

const isManagerError = Schema.is(Store.CloudSandboxManagerError);
const isCloudSandboxError = Schema.is(CloudSandboxError);

export const toError = (cause: unknown) => {
  if (isManagerError(cause)) return cause;
  const rateLimit =
    isCloudSandboxError(cause) && cause.rateLimit !== undefined ? cause.rateLimit : undefined;
  return rateLimit === undefined
    ? new Store.CloudSandboxManagerError({ reason: "provider", cause })
    : new Store.CloudSandboxManagerError({ reason: "rate-limited", cause, rateLimit });
};

/** Profile lookup failures read as provisioning failures to workspace callers. */
export const toManagerError = (cause: CloudSandboxProfileError) =>
  new Store.CloudSandboxManagerError({
    reason: cause.reason === "budget" ? "budget" : "provider",
    cause,
  });

/** The record without its degradation note, as a recovery saves it. */
export const withoutIssue = (record: Store.CloudSandboxRecord): Store.CloudSandboxRecord => {
  if (record.issue === undefined) return record;
  const { issue: _issue, ...rest } = record;
  return rest;
};

/**
 * Saves `issue` on the record as presence, not history: an identical failure
 * span saves nothing. Best effort: a save that cannot land is a logged warning.
 */
export const stampIssue = (
  store: ManagerRuntime["store"],
  record: Store.CloudSandboxRecord,
  issue: ManagedCloudSandboxIssue,
): Effect.Effect<void> => {
  const current = record.issue;
  const sameCause =
    current !== undefined &&
    current.cause === issue.cause &&
    current.driverKind === issue.driverKind &&
    current.profileId === issue.profileId;
  if (sameCause) return Effect.void;
  return store.save({ ...record, issue }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Unable to stamp a follower sandbox issue on its record.", {
        resourceId: record.resourceId,
        cause,
      }),
    ),
  );
};

/** Stamps a failed connect: transport misses read as connectivity, the rest as allocation losses. */
export const stampConnectIssue = Effect.fn("ManagerRuntime.stampConnectIssue")(function* (
  store: ManagerRuntime["store"],
  resourceId: ManagedCloudSandboxResourceId,
  error: Store.CloudSandboxManagerError,
) {
  const row = yield* Effect.option(store.get(resourceId));
  if (Option.isNone(row)) {
    yield* Effect.logWarning("Unable to read the follower sandbox record to stamp an issue.", {
      resourceId,
    });
    return;
  }
  const found = row.value;
  if (Option.isNone(found)) return;
  yield* stampIssue(store, found.value, {
    cause:
      error.reason === "transport" || error.reason === "not-found"
        ? "connectivity"
        : error.reason === "provider"
          ? "provider-auth"
          : "allocation",
    driverKind: found.value.driverKind,
    profileId: found.value.profileId,
    message: error.message,
    firstSeenAt: DateTime.formatIso(yield* DateTime.now),
  });
});

/** Stamps a refused ownership bind. */
export const stampBindIssue = Effect.fn("ManagerRuntime.stampBindIssue")(function* (
  store: ManagerRuntime["store"],
  resourceId: ManagedCloudSandboxResourceId,
  error: Store.CloudSandboxManagerError,
) {
  const row = yield* Effect.option(store.get(resourceId));
  if (Option.isNone(row)) {
    yield* Effect.logWarning("Unable to read the follower sandbox record to stamp an issue.", {
      resourceId,
    });
    return;
  }
  const found = row.value;
  if (Option.isNone(found)) return;
  yield* stampIssue(store, found.value, {
    cause: "bind",
    driverKind: found.value.driverKind,
    profileId: found.value.profileId,
    message: error.message,
    firstSeenAt: DateTime.formatIso(yield* DateTime.now),
  });
});

const publicPhase = (
  record: Store.CloudSandboxRecord,
  connected: boolean,
): ManagedCloudSandboxResource["phase"] =>
  record.lifecycle !== "active"
    ? record.lifecycle
    : record.power === "stopped"
      ? "stopped"
      : record.intent === "disconnected"
        ? "disconnected"
        : connected
          ? "connected"
          : "connecting";

export const isDeletedOrDeleting = (record: Store.CloudSandboxRecord) =>
  record.lifecycle === "deleted" || record.lifecycle === "deleting";

export const isConnectIntent = (record: Store.CloudSandboxRecord) =>
  record.lifecycle === "active" && record.power === "running" && record.intent === "connected";

// The user asked for no connection, but the provider can still stop the VM; track it for display and the cap.
export const isDisconnectedButRunning = (record: Store.CloudSandboxRecord) =>
  record.lifecycle === "active" && record.power === "running" && record.intent === "disconnected";

export const makeManagerRuntime = Effect.fn("CloudSandboxManager.makeRuntime")(function* (
  renewalsEnabled: boolean,
) {
  const store = yield* Store.CloudSandboxStore;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const profiles = yield* Profiles.CloudSandboxProfiles;
  const transport = yield* Transport.CloudSandboxTransport;
  const drivers = yield* CloudSandboxDriverFactory;
  const httpClient = yield* HttpClient.HttpClient;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const controllerEnvironmentId = yield* environment.getEnvironmentId;
  const scope = yield* Scope.Scope;
  // Allocation and lifecycle locks are process-local; one controller server may
  // share a provider profile. A second controller server against the same
  // profile would not see these locks or this budget ledger.
  const locks = yield* KeyedLock.make<string>();
  const routes = new Map<string, ManagedCloudSandboxRoute>();
  const healthFailures = new Map<string, number>();
  const workers = new Map<string, Fiber.Fiber<void>>();

  const publicResource = (record: Store.CloudSandboxRecord): ManagedCloudSandboxResource => {
    const {
      selection: _selection,
      snapshotName: _snapshotName,
      snapshotReady: _snapshotReady,
      deletionOperationId: _deletionOperationId,
      finalSpendUsd: _finalSpendUsd,
      enableLinkedEnvironments: _enableLinkedEnvironments,
      lifecycle: _lifecycle,
      power: _power,
      intent: _intent,
      ...resource
    } = record;
    return { ...resource, phase: publicPhase(record, routes.has(record.resourceId)) };
  };

  const requireRecord = Effect.fn("CloudSandboxManager.requireRecord")(function* (
    id: ManagedCloudSandboxResourceId,
  ) {
    const found = yield* store.get(id);
    if (Option.isNone(found))
      return yield* new Store.CloudSandboxManagerError({ reason: "not-found" });
    return found.value;
  });

  const resolveProfile = (profileId: SandboxProfileId) =>
    profiles.resolve(profileId).pipe(Effect.mapError(toManagerError));

  const driverFor = Effect.fn("CloudSandboxManager.driver")(function* (
    record: Store.CloudSandboxRecord,
  ) {
    const { apiKey } = yield* resolveProfile(record.profileId);
    return yield* drivers
      .create(record.driverKind, {
        apiKey,
        httpClient,
      })
      .pipe(Effect.mapError(toError));
  });

  const requireSandbox = (record: Store.CloudSandboxRecord) =>
    record.sandboxId === null
      ? Effect.fail(new Store.CloudSandboxManagerError({ reason: "conflict" }))
      : Effect.succeed(record.sandboxId);

  const stopWorker = Effect.fn("CloudSandboxManager.stopWorker")(function* (id: string) {
    const worker = workers.get(id);
    if (worker) yield* Fiber.interrupt(worker);
    workers.delete(id);
  });

  return {
    renewalsEnabled,
    store,
    secrets,
    profiles,
    transport,
    drivers,
    httpClient,
    controllerEnvironmentId,
    scope,
    locks,
    routes,
    workers,
    healthFailures,
    publicResource,
    requireRecord,
    resolveProfile,
    driverFor,
    requireSandbox,
    stopWorker,
  };
});

export type ManagerRuntime = Effect.Success<ReturnType<typeof makeManagerRuntime>>;
