import { FollowerHostSettingsSnapshot, type ServerSettings } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";

const hostFileWrite = writeFileStringAtomically;
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as ServerConfig from "../config.ts";
import * as Settings from "../serverSettings.ts";
import {
  followerHostSettingsFields,
  followerSettingsFieldsEqual,
} from "./FollowerSettingsInheritance.ts";

export class FollowerHostSettingsLedgerError extends Schema.TaggedError<FollowerHostSettingsLedgerError>()(
  "FollowerHostSettingsLedgerError",
  {
    reason: Schema.Literals(["not-initialized", "storage"]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "not-initialized"
      ? "The controller has not approved follower settings yet."
      : "The controller could not read its follower settings ledger.";
  }
}

/**
 * The controller's ledger of approved follower settings inheritance. Every
 * host settings save that changes approved content issues exactly one new
 * revision; the save itself never waits on or fails from this ledger.
 */
export class FollowerHostSettingsLedger extends Context.Service<
  FollowerHostSettingsLedger,
  {
    readonly current: Effect.Effect<FollowerHostSettingsSnapshot, FollowerHostSettingsLedgerError>;
    /** Approved snapshots, announced only after their ledger record persisted. */
    readonly changes: Stream.Stream<FollowerHostSettingsSnapshot>;
    /** Non-blocking recompute after one host settings save; errors are logged, never thrown. */
    readonly applySettings: (settings: ServerSettings) => Effect.Effect<void>;
    /** Initializes from disk and follows settings saves. Call once per server start. */
    readonly start: Effect.Effect<void>;
  }
>()("t3/cloudSandbox/FollowerHostSettingsLedger") {}

const stateFromJsonString = fromLenientJson(FollowerHostSettingsSnapshot);
const encodeSnapshot = Schema.encodeSync(stateFromJsonString);
const decodeSnapshotOption = Schema.decodeOption(stateFromJsonString);

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  // writeFileStringAtomically needs the filesystem services at run time, but
  // the ledger's service methods must keep their requirement set closed.
  const fileServices = yield* Effect.context<
    FileSystem.FileSystem | Path.Path | Effect.Services<ReturnType<typeof hostFileWrite>>
  >();
  const settings = yield* Settings.ServerSettingsService;

  const file = pathService.join(config.baseDir, "managed-follower-host-settings.json");
  const watcherScope = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(watcherScope, Exit.void));

  const state = yield* Ref.make<FollowerHostSettingsSnapshot | null>(null);
  const writes = yield* Semaphore.make(1);
  // Replay keeps late subscribers (tests, future consumers) seeing the
  // recent approvals instead of missing announcements made before attach.
  const approvals = yield* PubSub.unbounded<FollowerHostSettingsSnapshot>({
    replay: 128,
  });

  const readPersisted = Effect.gen(function* () {
    const exists = yield* fs.exists(file).pipe(Effect.orElseSucceed(() => false));
    if (!exists) return null;
    const raw = yield* fs.readFileString(file).pipe(Effect.option);
    if (Option.isNone(raw)) return null;
    return Option.getOrUndefined(decodeSnapshotOption(raw.value)) ?? null;
  }).pipe(Effect.orElseSucceed(() => null));

  const save = (snapshot: FollowerHostSettingsSnapshot) =>
    writeFileStringAtomically({ filePath: file, contents: encodeSnapshot(snapshot) }).pipe(
      Effect.provide(fileServices),
    );

  const initialize = Effect.gen(function* () {
    const persisted = yield* readPersisted;
    if (persisted !== null) {
      yield* Ref.set(state, persisted);
      return;
    }
    // A missing or corrupt ledger re-derives from the clock, which is already
    // past any revision an existing follower last applied.
    const hostSettings = yield* settings.getSettings.pipe(Effect.option);
    if (Option.isNone(hostSettings)) return;
    const millis = yield* Clock.currentTimeMillis;
    const snapshot: FollowerHostSettingsSnapshot = {
      revision: Math.max(1, millis),
      fields: followerHostSettingsFields(hostSettings.value),
    };
    yield* Ref.set(state, snapshot);
    yield* save(snapshot).pipe(
      Effect.tapError(() =>
        Effect.logWarning("Follower settings ledger could not persist its seed."),
      ),
      Effect.ignore,
    );
  });

  const current = Effect.gen(function* () {
    const snapshot = yield* Ref.get(state);
    if (snapshot === null)
      return yield* new FollowerHostSettingsLedgerError({ reason: "not-initialized" });
    return { revision: snapshot.revision, fields: { ...snapshot.fields } };
  }).pipe(Effect.withSpan("FollowerHostSettingsLedger.current"));

  const applySettingsUnlocked = Effect.fn("FollowerHostSettingsLedger.applySettings")(function* (
    hostSettings: ServerSettings,
  ) {
    const before = yield* Ref.get(state);
    const fields = followerHostSettingsFields(hostSettings);
    if (before !== null && followerSettingsFieldsEqual(before.fields, fields)) return;
    const millis = yield* Clock.currentTimeMillis;
    // Same-millis saves stay ordered; a reset clock never rewinds a revision.
    const revision = Math.max(millis, (before?.revision ?? 0) + 1);
    const next: FollowerHostSettingsSnapshot = { revision, fields };
    yield* Ref.set(state, next);
    yield* save(next).pipe(
      // The host save already persisted; a failed ledger write must not fail it.
      Effect.tapError(() => Effect.logWarning("Follower settings propagation skipped this save.")),
      Effect.ignore,
    );
    yield* PubSub.publish(approvals, next);
  }, Effect.provide(fileServices));

  // Saves and the boot catch-up serialize so a catch-up that read settings
  // before an in-flight save cannot clobber the newer content afterwards.
  const applySettings = (hostSettings: ServerSettings) =>
    writes.withPermits(1)(applySettingsUnlocked(hostSettings));

  const start = Effect.gen(function* () {
    yield* initialize.pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning(
          "Follower settings ledger start failed; will retry on the next save.",
          cause,
        ),
      ),
    );
    // The subscription attaches before start returns, because a publish
    // racing a later attach would be dropped by the PubSub. The catch-up
    // runs inline, under the same lock as saves, before the drain loop
    // forks, so content equality keeps both covered paths idempotent.
    const changes = yield* settings.subscribeChanges.pipe(
      Effect.provideService(Scope.Scope, watcherScope),
    );
    yield* writes.withPermits(1)(
      Effect.gen(function* () {
        const current = yield* settings.getSettings.pipe(Effect.option);
        if (Option.isSome(current)) yield* applySettingsUnlocked(current.value);
      }),
    );
    yield* changes.pipe(
      Stream.runForEach((value) => applySettings(value)),
      Effect.forkIn(watcherScope),
    );
  });

  return FollowerHostSettingsLedger.of({
    current,
    changes: Stream.fromPubSub(approvals),
    applySettings,
    start,
  });
});

export const layer = Layer.effect(FollowerHostSettingsLedger, make);
