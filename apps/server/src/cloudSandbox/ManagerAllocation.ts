import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { cloudSandboxSupportsMachine } from "@t3tools/cloud-sandbox";
import {
  ManagedCloudSandboxProvisionInput,
  legacyOwner,
  legacyRetention,
  type CloudSandboxProfile,
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

export const makeAllocation = (rt: ManagerRuntime, connections: ManagerConnections) => {
  const {
    store,
    secrets,
    profiles,
    transport,
    httpClient,
    controllerEnvironmentId,
    locks,
    routes,
    requireRecord,
    resolveProfile,
    driverFor,
    stopWorker,
    drivers,
  } = rt;
  const { connectUnlocked, connectRecord } = connections;
  const reserveAllocation = Effect.fn("CloudSandboxManager.reserveAllocation")(function* (
    profile: CloudSandboxProfile,
    size: string | undefined,
    autoStopMinutes: number,
    record: Store.CloudSandboxRecord,
  ) {
    const { apiKey } = yield* resolveProfile(profile.profileId);
    const driver = yield* drivers
      .create(profile.driverKind, {
        apiKey,
        httpClient,
      })
      .pipe(Effect.mapError(toError));
    // The selected provider owns machine validation; a reported list catches
    // invalid sizes before any save or snapshot capture.
    if (!cloudSandboxSupportsMachine(driver, size ?? "default"))
      return yield* new Store.CloudSandboxManagerError({
        reason: "provider",
        cause: new Error(`The provider does not offer this machine type (${size ?? "default"}).`),
      });
    yield* locks.withLock(
      `profile:${profile.profileId}`,
      Effect.gen(function* () {
        const live = (yield* store.list).filter(
          (candidate) => candidate.profileId === profile.profileId && Store.isLiveRecord(candidate),
        );
        const running = live.filter(
          (candidate) =>
            candidate.power === "running" && candidate.resourceId !== record.resourceId,
        );
        if (running.length >= profile.maxConcurrentSandboxes)
          return yield* new Store.CloudSandboxManagerError({ reason: "concurrency" });
        if (profile.monthlyBudgetUsd !== null) {
          // An unpriced machine counts as unknown cost, never free: admission is refused.
          const rateUsdPerHour = (requestSize: string | undefined): number | null => {
            const rate = driver.pricing?.hourlyUsd(requestSize ?? "default");
            return rate !== null && rate !== undefined && Number.isFinite(rate) && rate >= 0
              ? rate
              : null;
          };
          const reservationUsd = (
            requestSize: string | undefined,
            minutes: number,
          ): number | null => {
            const rate = rateUsdPerHour(requestSize);
            return rate === null ? null : (rate * minutes) / 60;
          };
          const spend = yield* profiles
            .spendOf(profile.profileId)
            .pipe(Effect.mapError(toManagerError));
          if (spend !== null && !Number.isFinite(spend.usd))
            return yield* new Store.CloudSandboxManagerError({ reason: "budget" });
          const requestedUsd = reservationUsd(size, autoStopMinutes);
          if (requestedUsd === null)
            return yield* new Store.CloudSandboxManagerError({
              reason: "budget",
              cause: new Error(
                `The provider does not report a price for this machine (${size ?? "default"}).`,
              ),
            });
          let committed = spend?.usd ?? 0;
          for (const candidate of live) {
            const fallback = reservationUsd(
              candidate.selection.size,
              candidate.selection.autoStopMinutes,
            );
            const knownUsd =
              candidate.sandboxId !== null && driver.usage
                ? yield* driver.usage.getUsage(candidate.sandboxId).pipe(
                    Effect.map((usage) => usage.listValueUsd),
                    Effect.orElseSucceed(() => fallback),
                  )
                : fallback;
            if (knownUsd === null || !Number.isFinite(knownUsd))
              return yield* new Store.CloudSandboxManagerError({
                reason: "budget",
                cause: new Error("The running cost of an existing sandbox cannot be confirmed."),
              });
            committed += knownUsd;
          }
          const total = committed + requestedUsd;
          if (total > profile.monthlyBudgetUsd)
            return yield* new Store.CloudSandboxManagerError({ reason: "budget" });
        }
        yield* store.save(record);
      }),
    );
  });

  // A stopped record holds no running slot, so starting it again passes the same limits.
  const reserveResume = Effect.fn("CloudSandboxManager.reserveResume")(function* (
    record: Store.CloudSandboxRecord,
  ) {
    if (record.lifecycle !== "active" || record.power !== "stopped") return;
    const { profile } = yield* resolveProfile(record.profileId);
    yield* reserveAllocation(profile, record.selection.size, record.selection.autoStopMinutes, {
      ...record,
      power: "running",
      intent: "connected",
    });
  });

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
              existing.value.selection,
              input.sandbox,
            ) ||
            existing.value.driverKind !== profile.driverKind ||
            existing.value.profileId !== input.profile
          )
            return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
          if (isDeletedOrDeleting(existing.value))
            return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
          if (existing.value.lifecycle !== "failed") {
            yield* reserveResume(existing.value);
            const route = yield* connectUnlocked(input.operationId, true);
            return { ...route, pairingToken: yield* transport.pairingToken(input.operationId) };
          }
          record = { ...existing.value, lifecycle: "allocating" };
        } else {
          record = {
            resourceId: input.operationId,
            controllerEnvironmentId,
            driverKind: profile.driverKind,
            profileId: input.profile,
            sandboxId: null,
            parentResourceId: null,
            // Fresh resources start user-owned; delegated followers are bound
            // to their task/thread owner after the child task exists.
            owner: legacyOwner(),
            retention: legacyRetention(),
            lifecycle: "allocating",
            power: "running",
            intent: "connected",
            environmentId: null,
            label: input.sandbox.name,
            selection: input.sandbox,
            snapshotName: null,
            snapshotReady: false,
            deletionOperationId: null,
            enableLinkedEnvironments: input.enableLinkedEnvironments === true,
            finalSpendUsd: null,
            recordingIds: [],
          };
        }
        yield* reserveAllocation(
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
    options?: { readonly inheritOwner?: boolean },
  ) {
    return yield* Effect.gen(function* () {
      const source = yield* requireRecord(input.sourceResourceId);
      if (isDeletedOrDeleting(source))
        return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
      const existing = yield* store.get(input.operationId);
      if (Option.isSome(existing)) {
        if (
          existing.value.parentResourceId !== input.sourceResourceId ||
          existing.value.selection.name !== input.name ||
          existing.value.selection.size !== (input.size ?? "small") ||
          existing.value.selection.autoStopMinutes !== (input.autoStopMinutes ?? 15)
        )
          return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
        if (isDeletedOrDeleting(existing.value))
          return yield* new Store.CloudSandboxManagerError({ reason: "conflict" });
        if (existing.value.lifecycle !== "failed")
          return yield* connectUnlocked(input.operationId, true);
      }
      const { profile } = yield* resolveProfile(source.profileId);
      const driver = yield* driverFor(source);
      if (!driver.forking || !driver.snapshots)
        return yield* new Store.CloudSandboxManagerError({ reason: "provider" });
      const name = `t3-${input.operationId}`;
      const size = input.size ?? "small";
      const autoStopMinutes = input.autoStopMinutes ?? 15;
      const record: Store.CloudSandboxRecord = Option.isSome(existing)
        ? { ...existing.value, lifecycle: "allocating" }
        : {
            resourceId: input.operationId,
            controllerEnvironmentId,
            driverKind: source.driverKind,
            profileId: source.profileId,
            sandboxId: null,
            parentResourceId: source.resourceId,
            // A root-initiated fork inherits its source's ownership; a
            // child-initiated fork starts unbound and is bound to its task.
            owner: options?.inheritOwner === false ? legacyOwner() : source.owner,
            retention: options?.inheritOwner === false ? legacyRetention() : source.retention,
            lifecycle: "allocating",
            power: "running",
            intent: "connected",
            environmentId: null,
            label: input.name,
            selection: {
              requestId: input.operationId,
              template: name,
              name: input.name,
              size,
              autoStopMinutes,
            },
            snapshotName: name,
            snapshotReady: false,
            deletionOperationId: null,
            finalSpendUsd: null,
            recordingIds: [],
            enableLinkedEnvironments: input.enableLinkedEnvironments ?? false,
          };
      yield* reserveAllocation(profile, size, autoStopMinutes, record);
      return yield* connectRecord(record, true);
    });
  });

  const fork = Effect.fn("CloudSandboxManager.forkLocked")((
    input: ManagedCloudSandboxForkInput,
  ) => {
    if (input.sourceResourceId === input.operationId)
      return Effect.fail(new Store.CloudSandboxManagerError({ reason: "conflict" }));
    const first =
      input.sourceResourceId < input.operationId ? input.sourceResourceId : input.operationId;
    const second =
      input.sourceResourceId < input.operationId ? input.operationId : input.sourceResourceId;
    return locks.withLock(first, locks.withLock(second, forkUnlocked(input)));
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
            if (driver.recording)
              for (const recordingId of record.recordingIds)
                yield* driver.recording.stop(record.sandboxId, recordingId).pipe(Effect.ignore);
            if (driver.deletion) {
              if (record.deletionOperationId === null) {
                const deletion = yield* driver.deletion.start(record.sandboxId);
                record = { ...record, deletionOperationId: deletion.operationId };
                yield* store.save(record);
              }
              yield* driver.deletion.wait(record.deletionOperationId!);
            } else yield* driver.delete(record.sandboxId);
          }
          if (record.snapshotName !== null && driver.snapshots)
            yield* driver.snapshots.delete(record.snapshotName);
          if (
            record.finalSpendUsd !== null &&
            record.finalSpendUsd > 0 &&
            Number.isFinite(record.finalSpendUsd)
          )
            yield* profiles
              .recordSpend(record.profileId, record.resourceId, record.finalSpendUsd)
              .pipe(Effect.mapError(toManagerError));
          yield* store.save({ ...record, lifecycle: "deleted", snapshotName: null });
          yield* secrets.remove(`cloud-source-${id}`).pipe(Effect.mapError(toError));
          // The VM and its identity are gone, so its SSH host-key pin may go too
          // and a correctly provisioned replacement can bootstrap anew.
          if (record.sandboxId !== null) yield* transport.forgetHostKey(id);
        }),
      )
      .pipe(Effect.mapError(toError));
  });

  return { reserveAllocation, reserveResume, provision, fork, release };
};

export type ManagerAllocation = ReturnType<typeof makeAllocation>;
