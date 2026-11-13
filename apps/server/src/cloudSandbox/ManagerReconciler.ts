import * as Effect from "effect/Effect";
import type { ManagedCloudSandboxResourceId } from "@t3tools/contracts";
import * as Store from "./CloudSandboxStore.ts";
import { isConnectIntent, toError, type ManagerRuntime } from "./ManagerRuntime.ts";
import type { ManagerAllocation } from "./ManagerAllocation.ts";
import type { ManagerConnections } from "./ManagerConnections.ts";

const HEALTH_FAILURES_BEFORE_RECONNECT = 2;
export const makeReconciler = (
  rt: ManagerRuntime,
  connections: ManagerConnections,
  allocation: ManagerAllocation,
) => {
  const {
    store,
    locks,
    routes,
    healthFailures,
    transport,
    requireRecord,
    requireSandbox,
    driverFor,
    stopWorker,
  } = rt;
  const { connectRecord, connectUnlocked, recoverUnallocated } = connections;
  const { release } = allocation;
  const observeSandbox = Effect.fn("CloudSandboxManager.observeSandbox")(function* (
    record: Store.CloudSandboxRecord,
  ) {
    const driver = yield* driverFor(record);
    return yield* driver.get(yield* requireSandbox(record)).pipe(
      Effect.catchTags({
        CloudSandboxError: (error) =>
          error.reason === "not-found" ? Effect.succeed(null) : Effect.fail(error),
      }),
      Effect.mapError(toError),
    );
  });
  const checkRoute = Effect.fn("CloudSandboxManager.checkRoute")(function* (
    record: Store.CloudSandboxRecord,
  ) {
    const id = record.resourceId;
    const status = yield* observeSandbox(record);
    if (status === null) return true;
    if (status.state === "stopped") {
      healthFailures.delete(id);
      yield* stopWorker(id);
      routes.delete(id);
      yield* transport.disconnect(id);
      yield* store.save({ ...record, power: "stopped" });
      return;
    }
    if (status.state !== "ready") return;
    const connected = yield* transport.isConnected(id).pipe(Effect.orElseSucceed(() => false));
    if (connected) {
      healthFailures.delete(id);
      return;
    }
    const failures = (healthFailures.get(id) ?? 0) + 1;
    if (failures < HEALTH_FAILURES_BEFORE_RECONNECT) {
      healthFailures.set(id, failures);
      return;
    }
    healthFailures.delete(id);
    yield* stopWorker(id);
    routes.delete(id);
    yield* transport.disconnect(id);
    yield* connectRecord(record, false);
  });

  /** Returns true when the record must be released, which takes its own lock. */
  const observePower = Effect.fn("CloudSandboxManager.observePower")(function* (
    record: Store.CloudSandboxRecord,
  ) {
    const status = yield* observeSandbox(record);
    if (status === null) return true;
    if (status.state === "stopped") yield* store.save({ ...record, power: "stopped" });
  });

  const reconcileLocked = Effect.fn("CloudSandboxManager.reconcileLocked")(function* (
    id: ManagedCloudSandboxResourceId,
  ) {
    const record = yield* requireRecord(id);
    if (record.lifecycle === "deleting") return true;
    if (record.lifecycle === "allocating" && record.sandboxId === null) {
      // A lost create response may have left a billable VM behind: rediscover it
      // by the durable name before giving up on the record.
      yield* recoverUnallocated(record);
      return false;
    }
    if (
      record.lifecycle === "allocating" &&
      record.sandboxId !== null &&
      record.environmentId === null
    ) {
      yield* connectRecord(record, false);
      return false;
    }
    if (routes.has(id)) {
      return (yield* checkRoute(record)) === true;
    }
    if (record.lifecycle === "active" && (yield* observePower(record))) return true;
    if (isConnectIntent(record)) yield* connectUnlocked(id, false);
    return false;
  });

  const reconcileOne = Effect.fn("CloudSandboxManager.reconcileOne")(function* (
    id: ManagedCloudSandboxResourceId,
  ) {
    const releasing = yield* locks.withLock(id, reconcileLocked(id));
    if (releasing) yield* release(id);
  });

  const reconcile = store.list.pipe(
    Effect.flatMap((records) =>
      Effect.forEach(
        records.filter(
          (record) =>
            record.lifecycle === "allocating" ||
            record.lifecycle === "deleting" ||
            record.lifecycle === "active",
        ),
        (record) => reconcileOne(record.resourceId).pipe(Effect.ignore),
        // A fork holds its resource lock for minutes and must not stall the other sandboxes.
        { concurrency: 8 },
      ),
    ),
    Effect.asVoid,
  );

  return { reconcile };
};
