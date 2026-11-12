import type { FollowerHostSettingsSnapshot } from "@t3tools/contracts";
import { WorktreeMcpFailure } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as ServerConfig from "../config.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { CloudSandboxController } from "../workspace/CloudSandboxController.ts";
import * as FollowerSettingsSync from "./FollowerSettingsSync.ts";

type SnapshotFixture = FollowerHostSettingsSnapshot;

const snapshotOf = (revision: number, fields: Record<string, unknown>): SnapshotFixture =>
  ({ revision, fields }) as SnapshotFixture;

/** A controller whose availability and approved snapshot the test drives. */
const fakeController = Effect.fn("fakeController")(function* () {
  const available = yield* Ref.make(true);
  // "fail" models an offline controller channel; otherwise the set snapshot
  // is what the host currently approves.
  const mode = yield* Ref.make<"fail" | "serve">("serve");
  const snapshot = yield* Ref.make<SnapshotFixture | null>(null);
  let requests = 0;
  const service = CloudSandboxController.of({
    available: Ref.get(available),
    request: Effect.fn("fakeController.request")(function* () {
      requests += 1;
      if ((yield* Ref.get(available)) === false)
        return yield* new WorktreeMcpFailure({
          code: "invalid_request",
          message: "Sandbox isolation requires a managed cloud environment.",
        });
      if ((yield* Ref.get(mode)) === "fail")
        return yield* new WorktreeMcpFailure({
          code: "operation_failed",
          message: "The controller channel is offline.",
        });
      const current = yield* Ref.get(snapshot);
      if (current === null)
        return yield* new WorktreeMcpFailure({
          code: "invalid_request",
          message: "No follower settings snapshot is available.",
        });
      return { _tag: "hostSettings", snapshot: current };
    }),
  });
  return { service, available, mode, snapshot, requestCount: () => requests };
});

const envFor = (controller: CloudSandboxController["Service"]) => {
  const controllerLayer = Layer.succeed(CloudSandboxController, controller);
  const settingsLayer = ServerSettingsService.layerTest();
  const configLayer = ServerConfig.ServerConfig.layerTest(process.cwd(), {
    prefix: "t3-follower-sync-",
  }).pipe(Layer.provide(NodeServices.layer));
  const syncLayer = FollowerSettingsSync.layer.pipe(
    Layer.provide(controllerLayer),
    Layer.provide(settingsLayer),
    Layer.provide(configLayer),
    Layer.provide(NodeServices.layer),
  );
  // All four layers merge into one environment so the test body can read
  // any of them directly.
  return Layer.mergeAll(controllerLayer, settingsLayer, configLayer, syncLayer);
};

describe("follower settings sync", () => {
  it.effect("seeds allowed non-default fields when the follower first fetches", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Effect.gen(function* () {
        const settings = yield* ServerSettingsService;
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        yield* Ref.set(controller.snapshot, snapshotOf(5, { defaultAutoPull: true }));
        expect(yield* sync.sync).toEqual({ _tag: "applied", revision: 5 });
        expect(yield* sync.lastApplied).toEqual({
          revision: 5,
          fields: { defaultAutoPull: true },
        });
        expect((yield* settings.getSettings).defaultAutoPull).toBe(true);
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );

  it.effect("resets inherited keys to defaults when the host reverts them", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Effect.gen(function* () {
        const settings = yield* ServerSettingsService;
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        yield* Ref.set(controller.snapshot, snapshotOf(5, { defaultAutoPull: true }));
        yield* sync.sync;
        yield* Ref.set(controller.snapshot, snapshotOf(6, {}));
        expect(yield* sync.sync).toEqual({ _tag: "applied", revision: 6 });
        expect((yield* settings.getSettings).defaultAutoPull).toBe(false);
        expect(yield* sync.lastApplied).toEqual({ revision: 6, fields: {} });
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );

  it.effect("refuses out-of-order revisions and keeps its state", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Effect.gen(function* () {
        const settings = yield* ServerSettingsService;
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        yield* Ref.set(controller.snapshot, snapshotOf(6, { defaultAutoPull: true }));
        yield* sync.sync;
        yield* Ref.set(controller.snapshot, snapshotOf(5, { defaultAutoPull: true }));
        expect(yield* sync.sync).toEqual({ _tag: "refused", reason: "out-of-order" });
        expect((yield* settings.getSettings).defaultAutoPull).toBe(true);
        expect(yield* sync.lastApplied).toEqual({
          revision: 6,
          fields: { defaultAutoPull: true },
        });
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );

  it.effect("refuses unclassified fields and keeps settings untouched", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Effect.gen(function* () {
        const settings = yield* ServerSettingsService;
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        yield* Ref.set(
          controller.snapshot,
          snapshotOf(7, { hostCodePath: "/srv/host-code", defaultAutoPull: true }),
        );
        expect(yield* sync.sync).toEqual({ _tag: "refused", reason: "unclassified-field" });
        expect((yield* settings.getSettings).defaultAutoPull).toBe(false);
        expect(yield* sync.lastApplied).toEqual({ revision: 0, fields: {} });
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );

  it.effect("refuses fields that violate the settings schema", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Effect.gen(function* () {
        const settings = yield* ServerSettingsService;
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        yield* Ref.set(controller.snapshot, snapshotOf(8, { defaultRuntimeMode: "not-a-mode" }));
        expect(yield* sync.sync).toEqual({ _tag: "refused", reason: "invalid-field" });
        const current = yield* settings.getSettings;
        expect(current.defaultRuntimeMode).not.toBe("not-a-mode");
        expect(yield* sync.lastApplied).toEqual({ revision: 0, fields: {} });
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );

  it.effect("skips a snapshot equal to the last applied revision", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Effect.gen(function* () {
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        yield* Ref.set(controller.snapshot, snapshotOf(5, { defaultAutoPull: true }));
        yield* sync.sync;
        expect(yield* sync.sync).toEqual({ _tag: "unchanged" });
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );

  it.effect("does nothing where the environment is not a follower", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Ref.set(controller.available, false);
      yield* Ref.set(controller.snapshot, snapshotOf(9, { defaultAutoPull: true }));
      yield* Effect.gen(function* () {
        const settings = yield* ServerSettingsService;
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        expect(yield* sync.sync).toEqual({ _tag: "not-follower" });
        expect((yield* settings.getSettings).defaultAutoPull).toBe(false);
        expect(controller.requestCount()).toBe(0);
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );

  it.effect("replays the latest approved revision after a controller outage", () =>
    Effect.gen(function* () {
      const controller = yield* fakeController();
      yield* Effect.gen(function* () {
        const settings = yield* ServerSettingsService;
        const sync = yield* FollowerSettingsSync.FollowerSettingsSync;
        yield* Ref.set(controller.snapshot, snapshotOf(11, { defaultAutoPull: true }));
        yield* Ref.set(controller.mode, "fail");
        const failed = yield* Effect.exit(sync.sync);
        expect(failed._tag).toBe("Failure");
        expect(yield* sync.lastApplied).toEqual({ revision: 0, fields: {} });
        // Reconnect: the same request succeeds and the follower catches up.
        yield* Ref.set(controller.mode, "serve");
        expect(yield* sync.sync).toEqual({ _tag: "applied", revision: 11 });
        expect((yield* settings.getSettings).defaultAutoPull).toBe(true);
        expect(yield* sync.lastApplied).toEqual({
          revision: 11,
          fields: { defaultAutoPull: true },
        });
      }).pipe(Effect.provide(envFor(controller.service)), Effect.scoped);
    }),
  );
});
