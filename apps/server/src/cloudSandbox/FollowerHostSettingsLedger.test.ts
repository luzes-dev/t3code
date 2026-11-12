import {
  DEFAULT_SERVER_SETTINGS,
  ProviderInstanceId,
  type ServerSettings,
  type ServerSettingsPatch,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import * as ServerConfig from "../config.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as FollowerHostSettings from "./FollowerHostSettingsLedger.ts";

type LedgerRecord = {
  readonly revision: number;
  readonly fields: Record<string, unknown>;
};

const readLedgerFile = (filePath: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const raw = yield* fs.readFileString(filePath).pipe(Effect.orElseSucceed(() => null));
    if (raw === null) return undefined;
    return JSON.parse(raw) as LedgerRecord;
  });

const fieldKeys = (record: LedgerRecord) => JSON.stringify(Object.keys(record.fields).sort());

const hostOverridesPatch = (): ServerSettingsPatch => ({
  defaultModelSelection: {
    instanceId: ProviderInstanceId.make("provider-core-codex"),
    model: "custom-model",
  },
  defaultAutoPull: true,
});

const hostRevertPatch = (): ServerSettingsPatch => ({
  defaultModelSelection: {
    instanceId: ProviderInstanceId.make("provider-core-codex"),
    model: "custom-model",
  },
  defaultAutoPull: false,
});

const hostSeeded = (): ServerSettings =>
  applyServerSettingsPatch(
    DEFAULT_SERVER_SETTINGS,
    hostOverridesPatch(),
  ) as unknown as ServerSettings;

/** A settings service whose saves persist then publish, like the real one. */
const observableSettings = Effect.fn("observableSettings")(function* (initial?: ServerSettings) {
  const state = yield* Ref.make<ServerSettings>({ ...(initial ?? DEFAULT_SERVER_SETTINGS) });
  const changes = yield* PubSub.unbounded<ServerSettings>();
  const service = ServerSettingsService.of({
    start: Effect.void,
    ready: Effect.void,
    managedPolicy: undefined,
    getSettings: Ref.get(state),
    updateSettings: (patch) =>
      Effect.gen(function* () {
        const next = applyServerSettingsPatch(yield* Ref.get(state), patch);
        // Persist, then publish: propagation may only see saved settings.
        yield* Ref.set(state, next);
        yield* PubSub.publish(changes, next);
        return next;
      }),
    updateProviderInstance: () => Effect.die("not used"),
    withSettingsSnapshot: (use) => Ref.get(state).pipe(Effect.flatMap(use)),
    streamChanges: Stream.fromPubSub(changes),
    subscribeChanges: PubSub.subscribe(changes).pipe(
      Effect.map((subscription) => Stream.fromSubscription(subscription)),
    ),
  });
  return { service, state, changes };
});

const layerWith = (service: ServerSettingsService["Service"], baseDir: string) =>
  FollowerHostSettings.layer.pipe(
    Layer.provide(Layer.succeed(ServerSettingsService, service)),
    Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
    Layer.provide(NodeServices.layer),
  );

/**
 * The approval announcement marks the milestone: the ledger announces a
 * revision only after persisting it, and the replay buffer delivers it to
 * subscribers that attach afterwards.
 */
const waitForApproval = (matches: (record: LedgerRecord) => boolean) =>
  Effect.gen(function* () {
    const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
    const head = yield* ledger.changes.pipe(
      Stream.filter((snapshot) => matches(snapshot as LedgerRecord)),
      Stream.take(1),
      Stream.runHead,
    );
    return Option.getOrUndefined(head)!;
  });

describe("follower host settings ledger", () => {
  it.effect("reports a not-initialized error before start", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-follower-ledger-fresh-" });
      const settings = yield* observableSettings();
      const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger.pipe(
        Effect.provide(layerWith(settings.service, baseDir)),
      );
      const outcome = yield* Effect.exit(ledger.current);
      expect(outcome._tag).toBe("Failure");
      yield* ledger.start;
      expect((yield* ledger.current).revision).toBeGreaterThanOrEqual(1);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("adopts current host settings the ledger has not recorded", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-follower-ledger-adopt-" });
      const ledgerPath = path.join(baseDir, "managed-follower-host-settings.json");
      const settings = yield* observableSettings(hostSeeded());
      yield* Effect.gen(function* () {
        const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
        // No ledger file exists; the boot catch-up adopts the host settings.
        yield* ledger.start;
        const record = yield* readLedgerFile(ledgerPath);
        expect(record).toBeDefined();
        expect(fieldKeys(record!)).toBe(
          JSON.stringify(["defaultAutoPull", "defaultModelSelection"]),
        );
        expect(record!.revision).toBeGreaterThanOrEqual(1);
      }).pipe(Effect.provide(layerWith(settings.service, baseDir)), Effect.scoped);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("approves one revision per genuine content change", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-follower-ledger-" });
      const ledgerPath = path.join(baseDir, "managed-follower-host-settings.json");
      const settings = yield* observableSettings();
      yield* Effect.gen(function* () {
        const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
        yield* ledger.start;
        yield* settings.service.updateSettings(hostOverridesPatch());
        const first = yield* waitForApproval(
          (record) =>
            fieldKeys(record) === JSON.stringify(["defaultAutoPull", "defaultModelSelection"]),
        );
        expect(first.revision).toBeGreaterThanOrEqual(1);
        yield* readLedgerFile(ledgerPath).pipe(
          Effect.map((record) => {
            expect(record?.revision).toBe(first.revision);
            return record;
          }),
        );

        // An identical re-save is skipped; the following genuine change
        // proves no revision was consumed in between.
        yield* settings.service.updateSettings(hostOverridesPatch());
        yield* settings.service.updateSettings(hostRevertPatch());
        const second = yield* waitForApproval(
          (record) => fieldKeys(record) === JSON.stringify(["defaultModelSelection"]),
        );
        expect(second.revision).toBe(first.revision + 1);
        yield* readLedgerFile(ledgerPath).pipe(
          Effect.map((record) => {
            expect(record?.revision).toBe(second.revision);
            return record;
          }),
        );
      }).pipe(Effect.provide(layerWith(settings.service, baseDir)), Effect.scoped);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("survives a restart on the same state directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-follower-ledger-restart-" });
      const baseDir = `${parent}/base`;
      const ledgerPath = path.join(baseDir, "managed-follower-host-settings.json");
      yield* fs.makeDirectory(baseDir);

      const beforeRestart = yield* Effect.gen(function* () {
        const settings = yield* observableSettings();
        return yield* Effect.gen(function* () {
          const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
          yield* ledger.start;
          yield* settings.service.updateSettings(hostOverridesPatch());
          yield* waitForApproval(
            (record) =>
              fieldKeys(record) === JSON.stringify(["defaultAutoPull", "defaultModelSelection"]),
          );
          return yield* readLedgerFile(ledgerPath);
        }).pipe(Effect.provide(layerWith(settings.service, baseDir)), Effect.scoped);
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped);

      // A restarted server restores its settings service from the persisted
      // host settings before the ledger re-reads them.
      const afterRestart = yield* Effect.gen(function* () {
        const settings = yield* observableSettings(hostSeeded());
        return yield* Effect.gen(function* () {
          const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
          yield* ledger.start;
          return yield* readLedgerFile(ledgerPath);
        }).pipe(Effect.provide(layerWith(settings.service, baseDir)), Effect.scoped);
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped);

      expect(afterRestart).toBeDefined();
      expect(afterRestart!.revision).toBeGreaterThanOrEqual(beforeRestart!.revision);
      expect(afterRestart!.fields).toEqual(beforeRestart!.fields);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("heals a corrupt ledger file from the clock", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-follower-ledger-heal-" });
      const baseDir = `${parent}/base`;
      yield* fs.makeDirectory(baseDir);
      yield* fs.writeFileString(
        path.join(baseDir, "managed-follower-host-settings.json"),
        "{not-json",
      );
      const settings = yield* observableSettings();
      yield* Effect.gen(function* () {
        const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
        yield* ledger.start;
        const snapshot = yield* ledger.current;
        expect(snapshot.revision).toBeGreaterThanOrEqual(1);
        expect(snapshot.fields).toEqual({});
      }).pipe(Effect.provide(layerWith(settings.service, baseDir)), Effect.scoped);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("keeps advancing and never fails when its ledger file cannot persist", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-follower-ledger-denied-" });
      const baseDir = `${parent}/base`;
      yield* fs.makeDirectory(baseDir, { recursive: true });
      // A directory occupying the ledger file's path: the atomic rename fails.
      yield* fs.makeDirectory(path.join(baseDir, "managed-follower-host-settings.json"), {
        recursive: true,
      });
      const settings = yield* observableSettings();
      yield* Effect.gen(function* () {
        const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
        yield* ledger.start;
        // The seed could not persist but still advanced the in-memory revision.
        const seeded = yield* ledger.current;
        expect(seeded.revision).toBeGreaterThanOrEqual(1);
        // A direct save advances in memory and stays silent about the failure.
        yield* ledger.applySettings(hostSeeded());
        const snapshot = yield* ledger.current;
        expect(fieldKeys({ revision: snapshot.revision, fields: snapshot.fields })).toBe(
          JSON.stringify(["defaultAutoPull", "defaultModelSelection"]),
        );
        expect(snapshot.revision).toBeGreaterThan(seeded.revision);
      }).pipe(Effect.provide(layerWith(settings.service, baseDir)), Effect.scoped);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("propagates after one host save without blocking or failing the save", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-follower-ledger-save-" });
      const ledgerPath = path.join(baseDir, "managed-follower-host-settings.json");
      const settings = yield* observableSettings();
      yield* Effect.gen(function* () {
        const ledger = yield* FollowerHostSettings.FollowerHostSettingsLedger;
        yield* ledger.start;
        const exit = yield* Effect.exit(settings.service.updateSettings(hostOverridesPatch()));
        expect(exit._tag).toBe("Success");
        const announced = yield* waitForApproval(
          (record) =>
            fieldKeys(record) === JSON.stringify(["defaultAutoPull", "defaultModelSelection"]),
        );
        expect(announced.revision).toBeGreaterThanOrEqual(1);
        const allowlist = [
          "defaultAutoPull",
          "defaultModelSelection",
          "defaultRuntimeMode",
          "responseStreamingMode",
          "fallbackModelSelection",
        ];
        for (const field of Object.keys(announced.fields)) expect(allowlist).toContain(field);
        yield* readLedgerFile(ledgerPath).pipe(
          Effect.map((record) => {
            expect(record?.revision).toBe(announced.revision);
            return record;
          }),
        );
      }).pipe(Effect.provide(layerWith(settings.service, baseDir)), Effect.scoped);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );
});
