import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { cloudSandboxCapabilities } from "@t3tools/cloud-sandbox";
import {
  MANAGED_CLOUD_SOURCE_PORT,
  ManagedSandboxOwner,
  retentionForOwner,
  type EnvironmentId,
  type ManagedCloudSandboxControlOperation,
  type ManagedCloudSandboxControlResult,
  type ManagedCloudSandboxResourceId,
  type ManagedCloudSandboxSourceOperation,
  type ManagedCloudSandboxSourceResult,
  type ManagedSandboxFollowerOwner,
  type ManagedSandboxRetention,
} from "@t3tools/contracts";
import * as Store from "./CloudSandboxStore.ts";
import { isDeletedOrDeleting, toError, type ManagerRuntime } from "./ManagerRuntime.ts";
import type { ManagerAllocation } from "./ManagerAllocation.ts";
import type { ManagerConnections } from "./ManagerConnections.ts";

export const makeControl = (
  rt: ManagerRuntime,
  connections: ManagerConnections,
  allocation: ManagerAllocation,
) => {
  const {
    renewalsEnabled,
    store,
    locks,
    transport,
    publicResource,
    requireRecord,
    requireSandbox,
    driverFor,
  } = rt;
  const { sourceContext, connectUnlocked, disconnectUnlocked } = connections;
  const { fork, release, reserveResume } = allocation;
  const ownerEquivalence = Schema.toEquivalence(ManagedSandboxOwner);
  /** Stamps ownership onto an unbound or identically-owned record. The host id
   * is always derived by the controller; callers never assert it. */
  const bindOwnerLocked = Effect.fn("CloudSandboxManager.bindOwnerLocked")(function* (
    id: ManagedCloudSandboxResourceId,
    owner: ManagedSandboxFollowerOwner,
    hostEnvironmentId: EnvironmentId,
    retention?: ManagedSandboxRetention | undefined,
  ) {
    const record = yield* requireRecord(id);
    if (isDeletedOrDeleting(record))
      return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
    // The host identity comes from the record's controller, not the caller.
    const canonical: ManagedSandboxOwner =
      owner._tag === "thread"
        ? { _tag: "thread", hostEnvironmentId, threadId: owner.threadId }
        : {
            _tag: "task",
            hostEnvironmentId,
            sourceThreadId: owner.sourceThreadId,
            taskId: owner.taskId,
          };
    // Rebinding to a different owner is refused: one task cannot take over
    // another's sandbox, and a retry with the same owner stays idempotent.
    if (record.owner._tag !== "user" && !ownerEquivalence(record.owner, canonical))
      return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
    const next: Store.CloudSandboxRecord = {
      ...record,
      owner: canonical,
      retention: retention ?? retentionForOwner(canonical),
    };
    yield* store.save(next);
    return publicResource(next);
  });
  const executeUnlocked = Effect.fn("CloudSandboxManager.execute")(function* (
    id: ManagedCloudSandboxResourceId,
    operation: ManagedCloudSandboxControlOperation,
  ): Effect.fn.Return<ManagedCloudSandboxControlResult, Store.CloudSandboxManagerError> {
    const record = yield* requireRecord(id);
    if (isDeletedOrDeleting(record))
      return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
    const sandboxId = yield* requireSandbox(record);
    const driver = yield* driverFor(record);
    const unsupported = () => new Store.CloudSandboxManagerError({ reason: "provider" });
    switch (operation._tag) {
      case "get":
        return {
          _tag: "status",
          sandbox: yield* driver.get(sandboxId).pipe(Effect.mapError(toError)),
          capabilities: cloudSandboxCapabilities(driver),
        };
      case "stop": {
        if (driver.recording)
          for (const recordingId of record.recordingIds)
            yield* driver.recording
              .stop(sandboxId, recordingId)
              .pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning(
                    "Cloud recording finalization failed; stopping sandbox.",
                    cause,
                  ),
                ),
              );
        yield* disconnectUnlocked(id);
        const sandbox = yield* driver.stop(sandboxId).pipe(Effect.mapError(toError));
        yield* store.save({ ...record, intent: "disconnected", power: "stopped" });
        return { _tag: "status", sandbox, capabilities: cloudSandboxCapabilities(driver) };
      }
      case "resume": {
        yield* disconnectUnlocked(id);
        yield* reserveResume(record);
        const status = yield* driver.get(sandboxId).pipe(Effect.mapError(toError));
        if (status.state === "stopped")
          yield* driver.resume(sandboxId).pipe(Effect.mapError(toError));
        yield* store.save({ ...record, power: "running", intent: "connected" });
        yield* connectUnlocked(id, true);
        return {
          _tag: "status",
          sandbox: yield* driver.get(sandboxId).pipe(Effect.mapError(toError)),
          capabilities: cloudSandboxCapabilities(driver),
        };
      }
      case "desktop":
        if (!driver.streaming) return yield* unsupported();
        return {
          _tag: "desktop",
          session: yield* (
            operation.viewMode === "browser"
              ? driver.streaming.getBrowserStreamSession
                ? driver.streaming.getBrowserStreamSession(sandboxId)
                : Effect.fail(unsupported())
              : driver.streaming.getDesktopStreamSession(sandboxId)
          ).pipe(Effect.mapError(toError)),
        };
      case "extend":
        if (!renewalsEnabled)
          return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
        if (!driver.keepalive) return yield* unsupported();
        return {
          _tag: "status",
          sandbox: yield* driver.keepalive
            .extendAutoStop(sandboxId, operation.hours)
            .pipe(Effect.mapError(toError)),
          capabilities: cloudSandboxCapabilities(driver),
        };
      case "host":
        if (
          !Number.isInteger(operation.port) ||
          operation.port < 1 ||
          operation.port > 65_535 ||
          operation.port === MANAGED_CLOUD_SOURCE_PORT
        )
          return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
        if (!driver.hosting) return yield* unsupported();
        if (!operation.isPublic && !driver.hosting.protectedAccess) return yield* unsupported();
        return {
          _tag: "host",
          port: yield* driver.hosting
            .exposePort(sandboxId, operation.port, operation.isPublic)
            .pipe(Effect.mapError(toError)),
        };
      case "usage":
        if (!driver.usage) return yield* unsupported();
        return {
          _tag: "usage",
          usage: yield* driver.usage.getUsage(sandboxId).pipe(Effect.mapError(toError)),
        };
      case "recordingStart":
        if (!driver.recording) return yield* unsupported();
        yield* store.save({
          ...record,
          recordingIds: [...new Set([...record.recordingIds, operation.recordingId])],
        });
        return {
          _tag: "recording",
          recording: yield* driver.recording
            .start(sandboxId, operation.recordingId)
            .pipe(Effect.mapError(toError)),
        };
      case "recordingStatus":
        if (!record.recordingIds.includes(operation.recordingId))
          return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
        if (!driver.recording) return yield* unsupported();
        return {
          _tag: "recording",
          recording: yield* driver.recording
            .status(sandboxId, operation.recordingId)
            .pipe(Effect.mapError(toError)),
        };
      case "recordingStop":
        if (!record.recordingIds.includes(operation.recordingId))
          return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
        if (!driver.recording) return yield* unsupported();
        return {
          _tag: "recording",
          recording: yield* driver.recording
            .stop(sandboxId, operation.recordingId)
            .pipe(Effect.mapError(toError)),
        };
      case "resize":
        if (!driver.resizing) return yield* unsupported();
        yield* disconnectUnlocked(id);
        return {
          _tag: "status",
          sandbox: yield* driver.resizing
            .resize(sandboxId, operation.size)
            .pipe(Effect.mapError(toError)),
          capabilities: cloudSandboxCapabilities(driver),
        };
    }
  });

  const execute = Effect.fn("CloudSandboxManager.executeLocked")(
    (id: ManagedCloudSandboxResourceId, operation: ManagedCloudSandboxControlOperation) =>
      locks.withLock(id, executeUnlocked(id, operation)),
  );

  const artifact = Effect.fn("CloudSandboxManager.artifact")(function* (
    id: ManagedCloudSandboxResourceId,
    recordingId: string,
  ) {
    const record = yield* requireRecord(id);
    if (!record.recordingIds.includes(recordingId))
      return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
    const driver = yield* driverFor(record);
    if (!driver.recording) return yield* new Store.CloudSandboxManagerError({ reason: "provider" });
    return yield* driver.recording
      .getArtifact(yield* requireSandbox(record), recordingId)
      .pipe(Effect.mapError(toError));
  });

  const executeSource = Effect.fn("CloudSandboxManager.executeSource")(function* (
    id: ManagedCloudSandboxResourceId,
    token: string,
    operation: ManagedCloudSandboxSourceOperation,
  ): Effect.fn.Return<ManagedCloudSandboxSourceResult, Store.CloudSandboxManagerError> {
    yield* sourceContext(id, token);
    const requireOwnedChild = Effect.fn("CloudSandboxManager.requireOwnedChild")(function* (
      childId: ManagedCloudSandboxResourceId,
    ) {
      const child = yield* requireRecord(childId);
      if (child.parentResourceId !== id)
        return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
      return child;
    });
    switch (operation._tag) {
      case "get":
        return {
          _tag: "resource",
          resource: publicResource(
            yield* operation.resourceId === undefined
              ? requireRecord(id)
              : requireOwnedChild(operation.resourceId),
          ),
        };
      case "status":
        yield* requireOwnedChild(operation.resourceId);
        return { _tag: "result", result: yield* execute(operation.resourceId, { _tag: "get" }) };
      case "host":
        return {
          _tag: "result",
          result: yield* execute(id, { _tag: "host", port: operation.port, isPublic: false }),
        };
      case "fork": {
        const created = yield* fork({ ...operation, sourceResourceId: id });
        return {
          _tag: "connection",
          connection: created.connection,
          pairingToken: yield* transport.pairingToken(created.connection.resourceId),
        };
      }
      case "connect": {
        yield* requireOwnedChild(operation.resourceId);
        // One critical section, so a concurrent stop or delete can't be overwritten by the reservation.
        const connected = yield* locks.withLock(
          operation.resourceId,
          Effect.gen(function* () {
            yield* reserveResume(yield* requireRecord(operation.resourceId));
            return yield* connectUnlocked(operation.resourceId, true);
          }),
        );
        return {
          _tag: "connection",
          connection: connected.connection,
          pairingToken: yield* transport.pairingToken(operation.resourceId),
        };
      }
      case "delete":
        yield* requireOwnedChild(operation.resourceId);
        yield* release(operation.resourceId);
        return { _tag: "deleted" };
      case "bindOwner": {
        // A child may bind only its own fork, and only to an owner hosted on
        // this controller derived from the authenticating resource's identity:
        // the host id arrives from the record, never the child.
        const auth = yield* requireRecord(id);
        if (auth.environmentId === null)
          return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
        yield* requireOwnedChild(operation.resourceId);
        const bound = yield* locks.withLock(
          operation.resourceId,
          bindOwnerLocked(
            operation.resourceId,
            operation.owner,
            auth.environmentId,
            operation.retention,
          ),
        );
        return { _tag: "resource", resource: bound };
      }
    }
  });

  return {
    execute,
    artifact,
    executeSource,
    bindOwnerLocked,
  };
};
