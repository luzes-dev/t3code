import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  ManagedCloudSandboxProvisionInput,
  legacyOwner,
  legacyRetention,
  type EnvironmentId,
  type ManagedCloudSandboxForkInput,
  type ManagedCloudSandboxResourceId,
} from "@t3tools/contracts";
import * as Store from "./CloudSandboxStore.ts";
import {
  isDeletedOrDeleting,
  toError,
  toManagerError,
  type ManagerRuntime,
} from "./ManagerRuntime.ts";
import type { ManagerConnections } from "./ManagerConnections.ts";
import type { ManagerReservations } from "./ManagerReservations.ts";

export const makeAllocation = (
  rt: ManagerRuntime,
  connections: ManagerConnections,
  reservations: ManagerReservations,
) => {
  const {
    store,
    secrets,
    profiles,
    transport,
    controllerEnvironmentId,
    locks,
    routes,
    requireRecord,
    resolveProfile,
    driverFor,
    stopWorker,
  } = rt;
  const { connectUnlocked, connectRecord } = connections;
  const { reserveAllocation, reserveResume } = reservations;

  const recoverAllocationOwnership = Effect.fn("CloudSandboxManager.recoverAllocationOwnership")(
    function* (
      record: Store.CloudSandboxRecord,
      ownership: ManagedCloudSandboxProvisionInput["ownership"],
      hostEnvironmentId: EnvironmentId,
    ) {
      if (record.allocationOwnership !== undefined) {
        if (
          record.allocationOwnership.sourceThreadId !== ownership?.sourceThreadId ||
          record.allocationOwnership.retention !== ownership?.retention
        )
          return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
        return record;
      }
      if (ownership === undefined) return record;
      const owner = record.owner;
      if (
        owner._tag !== "user" &&
        (owner.hostEnvironmentId !== hostEnvironmentId ||
          (owner._tag === "thread" ? owner.threadId : owner.sourceThreadId) !==
            ownership.sourceThreadId)
      )
        return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
      const recovered: Store.CloudSandboxRecord = {
        ...record,
        allocationOwnership: ownership,
        owner:
          owner._tag === "user"
            ? { _tag: "thread", hostEnvironmentId, threadId: ownership.sourceThreadId }
            : owner,
        retention: owner._tag === "user" ? ownership.retention : record.retention,
      };
      yield* store.save(recovered);
      return recovered;
    },
  );

  const provision = Effect.fn("CloudSandboxManager.provision")(function* (
    input: ManagedCloudSandboxProvisionInput,
  ) {
    const { profile } = yield* resolveProfile(input.profile);
    return yield* locks.withLock(
      input.operationId,
      Effect.gen(function* () {
        const existing = yield* store.get(input.operationId);
        let record: Store.CloudSandboxRecord;
        if (Option.isSome(existing)) {
          if (
            !Schema.toEquivalence(ManagedCloudSandboxProvisionInput.fields.sandbox)(
              existing.value.initialSelection ?? existing.value.selection,
              input.sandbox,
            ) ||
            existing.value.driverKind !== profile.driverKind ||
            existing.value.profileId !== input.profile
          )
            return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
          if (isDeletedOrDeleting(existing.value))
            return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
          record = yield* recoverAllocationOwnership(
            existing.value,
            input.ownership,
            controllerEnvironmentId,
          );
          if (record.lifecycle !== "failed") {
            yield* reserveResume(record);
            const route = yield* connectUnlocked(input.operationId, true);
            return { ...route, pairingToken: yield* transport.pairingToken(input.operationId) };
          }
          record = { ...record, lifecycle: "allocating" };
        } else {
          record = {
            resourceId: input.operationId,
            controllerEnvironmentId,
            driverKind: profile.driverKind,
            profileId: input.profile,
            sandboxId: null,
            parentResourceId: null,
            owner:
              input.ownership === undefined
                ? legacyOwner()
                : {
                    _tag: "thread",
                    hostEnvironmentId: controllerEnvironmentId,
                    threadId: input.ownership.sourceThreadId,
                  },
            retention: input.ownership?.retention ?? legacyRetention(),
            lifecycle: "allocating",
            power: "running",
            intent: "connected",
            environmentId: null,
            label: input.sandbox.name,
            selection: input.sandbox,
            initialSelection: input.sandbox,
            ...(input.ownership === undefined ? {} : { allocationOwnership: input.ownership }),
            snapshotName: null,
            snapshotReady: false,
            deletionOperationId: null,
            enableLinkedEnvironments: input.enableLinkedEnvironments === true,
            finalSpendUsd: null,
            recordingIds: [],
          };
        }
        record = yield* reserveAllocation(
          profile,
          input.sandbox.size,
          input.sandbox.autoStopMinutes,
          record,
        );
        return yield* connectRecord(record, true);
      }),
    );
  });

  const forkUnlocked = Effect.fn("CloudSandboxManager.fork")(function* (
    input: ManagedCloudSandboxForkInput,
    options?: { readonly inheritOwner?: boolean; readonly hostEnvironmentId?: EnvironmentId },
  ) {
    return yield* Effect.gen(function* () {
      const source = yield* requireRecord(input.sourceResourceId);
      if (isDeletedOrDeleting(source))
        return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
      let existing = yield* store.get(input.operationId);
      if (Option.isSome(existing)) {
        const selection = existing.value.initialSelection ?? existing.value.selection;
        if (
          existing.value.parentResourceId !== input.sourceResourceId ||
          selection.name !== input.name ||
          selection.size !== (input.size ?? "small") ||
          selection.autoStopMinutes !== (input.autoStopMinutes ?? 15)
        )
          return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
        if (isDeletedOrDeleting(existing.value))
          return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
        const recovered = yield* recoverAllocationOwnership(
          existing.value,
          input.ownership,
          options?.hostEnvironmentId ?? controllerEnvironmentId,
        );
        existing = Option.some(recovered);
        if (recovered.lifecycle !== "failed") {
          yield* reserveResume(recovered);
          return yield* connectUnlocked(input.operationId, true);
        }
      }
      const { profile } = yield* resolveProfile(source.profileId);
      const driver = yield* driverFor(source);
      if (!driver.forking || !driver.snapshots)
        return yield* new Store.CloudSandboxManagerError({ reason: "provider" });
      const name = `t3-${input.operationId}`;
      const size = input.size ?? "small";
      const autoStopMinutes = input.autoStopMinutes ?? 15;
      const selection = {
        requestId: input.operationId,
        template: name,
        name: input.name,
        size,
        autoStopMinutes,
      };
      const record: Store.CloudSandboxRecord = Option.isSome(existing)
        ? { ...existing.value, lifecycle: "allocating" }
        : {
            resourceId: input.operationId,
            controllerEnvironmentId,
            driverKind: source.driverKind,
            profileId: source.profileId,
            sandboxId: null,
            parentResourceId: source.resourceId,
            owner:
              input.ownership === undefined
                ? options?.inheritOwner === false
                  ? legacyOwner()
                  : source.owner
                : {
                    _tag: "thread",
                    hostEnvironmentId: options?.hostEnvironmentId ?? controllerEnvironmentId,
                    threadId: input.ownership.sourceThreadId,
                  },
            retention:
              input.ownership?.retention ??
              (options?.inheritOwner === false ? legacyRetention() : source.retention),
            lifecycle: "allocating",
            power: "running",
            intent: "connected",
            environmentId: null,
            label: input.name,
            selection,
            initialSelection: selection,
            ...(input.ownership === undefined ? {} : { allocationOwnership: input.ownership }),
            snapshotName: name,
            snapshotReady: false,
            deletionOperationId: null,
            finalSpendUsd: null,
            recordingIds: [],
            enableLinkedEnvironments: input.enableLinkedEnvironments ?? false,
          };
      const reserved = yield* reserveAllocation(profile, size, autoStopMinutes, record);
      return yield* connectRecord(reserved, true);
    });
  });

  const fork = Effect.fn("CloudSandboxManager.forkLocked")((
    input: ManagedCloudSandboxForkInput,
    options?: { readonly inheritOwner?: boolean; readonly hostEnvironmentId?: EnvironmentId },
  ) => {
    if (input.sourceResourceId === input.operationId)
      return Effect.fail(new Store.CloudSandboxManagerError({ reason: "conflict" }));
    const first =
      input.sourceResourceId < input.operationId ? input.sourceResourceId : input.operationId;
    const second =
      input.sourceResourceId < input.operationId ? input.operationId : input.sourceResourceId;
    return locks.withLock(first, locks.withLock(second, forkUnlocked(input, options)));
  });

  const release = Effect.fn("CloudSandboxManager.release")(function* (
    id: ManagedCloudSandboxResourceId,
  ) {
    return yield* locks
      .withLock(
        id,
        Effect.gen(function* () {
          let record = yield* requireRecord(id);
          if (record.lifecycle === "deleted") return;
          yield* stopWorker(id);
          routes.delete(id);
          yield* transport.disconnect(id);
          record = { ...record, lifecycle: "deleting" };
          yield* store.save(record);
          const driver = yield* driverFor(record);
          const estimateLifetimeUsd = Effect.fn("CloudSandboxManager.estimateLifetimeUsd")(
            function* (sandboxId: string) {
              const now = yield* Clock.currentTimeMillis;
              const details = yield* driver.get(sandboxId);
              const createdAt = details.createdAt
                ? DateTime.make(details.createdAt)
                : Option.none();
              if (Option.isNone(createdAt)) return null;
              const rate = details.hourlyRateUsd;
              if (rate === null || rate === undefined || !Number.isFinite(rate) || rate < 0)
                return null;
              const elapsedMs = Math.max(0, now - DateTime.toEpochMillis(createdAt.value));
              return (rate * elapsedMs) / 3_600_000;
            },
          );
          if (record.finalSpendUsd === null && record.sandboxId !== null) {
            const sandboxId = record.sandboxId;
            const lifetimeUsd = yield* (
              driver.usage
                ? driver.usage.getUsage(sandboxId).pipe(
                    Effect.map((usage) => usage.listValueUsd),
                    Effect.catch(() => estimateLifetimeUsd(sandboxId)),
                  )
                : estimateLifetimeUsd(sandboxId)
            ).pipe(Effect.option);
            record = {
              ...record,
              finalSpendUsd: Option.getOrNull(Option.filter(lifetimeUsd, Number.isFinite)),
            };
            yield* store.save(record);
          }
          if (record.sandboxId !== null) {
            const sandboxId = record.sandboxId;
            if (driver.recording)
              for (const recordingId of record.recordingIds)
                yield* driver.recording.stop(record.sandboxId, recordingId).pipe(Effect.ignore);
            if (driver.deletion) {
              if (record.deletionOperationId === null) {
                const deletion = yield* driver.deletion.start(record.sandboxId).pipe(
                  Effect.catchTags({
                    CloudSandboxError: (error) =>
                      error.reason === "not-found" ? Effect.succeed(null) : Effect.fail(error),
                  }),
                );
                if (deletion !== null) {
                  record = { ...record, deletionOperationId: deletion.operationId };
                  yield* store.save(record);
                }
              }
              if (record.deletionOperationId !== null)
                yield* driver.deletion.wait(record.deletionOperationId).pipe(
                  Effect.catchTags({
                    CloudSandboxError: (error) =>
                      error.reason !== "not-found"
                        ? Effect.fail(error)
                        : driver.get(sandboxId).pipe(
                            Effect.catchTags({
                              CloudSandboxError: (statusError) =>
                                statusError.reason === "not-found"
                                  ? Effect.succeed(null)
                                  : Effect.fail(statusError),
                            }),
                            Effect.flatMap((sandbox) => {
                              if (sandbox === null) return Effect.void;
                              record = { ...record, deletionOperationId: null };
                              return store.save(record).pipe(Effect.andThen(Effect.fail(error)));
                            }),
                          ),
                  }),
                );
            } else
              yield* driver.delete(record.sandboxId).pipe(
                Effect.catchTags({
                  CloudSandboxError: (error) =>
                    error.reason === "not-found" ? Effect.void : Effect.fail(error),
                }),
              );
          }
          if (record.snapshotName !== null && driver.snapshots)
            yield* driver.snapshots.delete(record.snapshotName).pipe(
              Effect.catchTags({
                CloudSandboxError: (error) =>
                  error.reason === "not-found" ? Effect.void : Effect.fail(error),
              }),
            );
          if (
            record.finalSpendUsd !== null &&
            record.finalSpendUsd > 0 &&
            Number.isFinite(record.finalSpendUsd)
          )
            yield* profiles
              // A follower's VM spend stays its own per-resource line and is
              // attributed once to its controller host, never twice.
              .recordSpend(
                record.profileId,
                record.resourceId,
                record.finalSpendUsd,
                record.parentResourceId ?? undefined,
              )
              .pipe(Effect.mapError(toManagerError));
          yield* store.save({ ...record, lifecycle: "deleted", snapshotName: null });
          yield* secrets.remove(`cloud-source-${id}`).pipe(Effect.mapError(toError));
        }),
      )
      .pipe(Effect.mapError(toError));
  });

  return { provision, fork, release };
};

export type ManagerAllocation = ReturnType<typeof makeAllocation>;
