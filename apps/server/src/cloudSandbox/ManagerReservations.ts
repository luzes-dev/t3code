import * as Effect from "effect/Effect";
import { cloudSandboxSupportsMachine } from "@t3tools/cloud-sandbox";
import type { CloudSandboxProfile, ManagedCloudSandboxResourceId } from "@t3tools/contracts";
import * as Store from "./CloudSandboxStore.ts";
import { toError, toManagerError, type ManagerRuntime } from "./ManagerRuntime.ts";

export const makeReservations = (rt: ManagerRuntime) => {
  const {
    store,
    profiles,
    drivers,
    httpClient,
    locks,
    resolveProfile,
    requireRecord,
    requireSandbox,
    driverFor,
  } = rt;
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
    const rateUsdPerHour = (requestSize: string | undefined): number | null => {
      const rate = driver.pricing?.hourlyUsd(requestSize ?? "default");
      return rate !== null && rate !== undefined && Number.isFinite(rate) && rate >= 0
        ? rate
        : null;
    };
    let reservationSize = size ?? "default";
    // A lost resize response can leave either size billable until observation succeeds.
    for (const candidate of [record.selection.size, record.reservation?.size]) {
      if (candidate === undefined) continue;
      const rate = rateUsdPerHour(candidate);
      const reservedRate = rateUsdPerHour(reservationSize);
      if (rate === null || (reservedRate !== null && rate > reservedRate))
        reservationSize = candidate;
    }
    return yield* locks.withLock(
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
          const requestedUsd = reservationUsd(reservationSize, autoStopMinutes);
          if (requestedUsd === null)
            return yield* new Store.CloudSandboxManagerError({
              reason: "budget",
              cause: new Error(
                `The provider does not report a price for this machine (${size ?? "default"}).`,
              ),
            });
          let committed = spend?.usd ?? 0;
          for (const candidate of live) {
            const selectedUsd = reservationUsd(
              candidate.selection.size,
              candidate.selection.autoStopMinutes,
            );
            const reservedUsd =
              candidate.power === "running" && candidate.reservation !== undefined
                ? reservationUsd(candidate.reservation.size, candidate.reservation.autoStopMinutes)
                : 0;
            if (selectedUsd === null || reservedUsd === null)
              return yield* new Store.CloudSandboxManagerError({ reason: "budget" });
            const fallback = Math.max(selectedUsd, reservedUsd);
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
            committed += candidate.power === "running" ? Math.max(knownUsd, fallback) : knownUsd;
          }
          const total = committed + requestedUsd;
          if (total > profile.monthlyBudgetUsd)
            return yield* new Store.CloudSandboxManagerError({ reason: "budget" });
        }
        const reserved = {
          ...record,
          reservation: {
            size: reservationSize,
            autoStopMinutes: Math.ceil(
              Math.max(autoStopMinutes, record.reservation?.autoStopMinutes ?? 0),
            ),
          },
        };
        yield* store.save(reserved);
        return reserved;
      }),
    );
  });

  // A stopped record holds no running slot, so starting it again passes the same limits.
  const reserveResume = Effect.fn("CloudSandboxManager.reserveResume")(function* (
    record: Store.CloudSandboxRecord,
  ) {
    if (record.lifecycle !== "active") return;
    if (record.power !== "stopped") {
      const driver = yield* driverFor(record);
      const observed = yield* driver
        .get(yield* requireSandbox(record))
        .pipe(Effect.mapError(toError));
      if (observed.state !== "stopped" && observed.state !== "stopping") return;
    }
    const { profile } = yield* resolveProfile(record.profileId);
    yield* reserveAllocation(
      profile,
      record.selection.size,
      Math.max(record.selection.autoStopMinutes, record.reservation?.autoStopMinutes ?? 0),
      {
        ...record,
        power: "running",
        intent: "connected",
      },
    );
  });

  const extend = Effect.fn("CloudSandboxManager.extend")(function* (
    id: ManagedCloudSandboxResourceId,
    hours: number,
  ) {
    const record = yield* requireRecord(id);
    if (!rt.renewalsEnabled || record.lifecycle === "deleted" || record.lifecycle === "deleting")
      return yield* new Store.CloudSandboxManagerError({ reason: "ownership" });
    const driver = yield* driverFor(record);
    if (!driver.keepalive) return yield* new Store.CloudSandboxManagerError({ reason: "provider" });
    const { profile } = yield* resolveProfile(record.profileId);
    const reserved = yield* reserveAllocation(profile, record.selection.size, hours * 60, record);
    const sandbox = yield* driver.keepalive
      .extendAutoStop(yield* requireSandbox(record), hours)
      .pipe(Effect.mapError(toError));
    yield* store.save({
      ...reserved,
      reservation: { ...reserved.reservation, autoStopMinutes: Math.ceil(hours * 60) },
    });
    return sandbox;
  });

  return { reserveAllocation, reserveResume, extend };
};

export type ManagerReservations = ReturnType<typeof makeReservations>;
