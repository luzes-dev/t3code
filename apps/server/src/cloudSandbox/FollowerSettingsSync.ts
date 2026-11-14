import type { FollowerHostSettingsSnapshot } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";

const hostFileWrite = writeFileStringAtomically;
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Controller from "../workspace/CloudSandboxController.ts";
import {
  followerSettingsInheritancePatch,
  LastAppliedFollowerSettings,
  type LastAppliedFollowerSettings as LastAppliedFollowerSettingsValue,
} from "./FollowerSettingsInheritance.ts";

export class FollowerSettingsSyncError extends Schema.TaggedError<FollowerSettingsSyncError>()(
  "FollowerSettingsSyncError",
  {
    reason: Schema.Literals(["controller", "storage"]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "controller"
      ? "The sandbox controller did not answer the follower settings request."
      : "The follower could not persist its inherited settings state.";
  }
}

export type FollowerSettingsSyncOutcome =
  | { _tag: "not-follower" }
  | { _tag: "unchanged" }
  | { _tag: "applied"; revision: number }
  | { _tag: "refused"; reason: "out-of-order" | "unclassified-field" | "invalid-field" };

/**
 * Follower-side loop: one settings snapshot per revision, pulled over the
 * existing controller channel. The controller is never blocked by an offline
 * follower; a reconnect simply re-runs this pull and replays whatever the
 * host has approved since.
 */
export class FollowerSettingsSync extends Context.Service<
  FollowerSettingsSync,
  {
    /** Pulls once and applies/refuses/skips the approved snapshot. */
    readonly sync: Effect.Effect<FollowerSettingsSyncOutcome, FollowerSettingsSyncError>;
    /** Last snapshot this follower applied, read from its state file. */
    readonly lastApplied: Effect.Effect<
      LastAppliedFollowerSettingsValue,
      FollowerSettingsSyncError
    >;
    /** Forks the pull loop; a no-op where this environment is not a follower. */
    readonly start: Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/cloudSandbox/FollowerSettingsSync") {}

const stateFromJsonString = fromLenientJson(LastAppliedFollowerSettings);
const encodeState = Schema.encodeSync(stateFromJsonString);
const decodeStateOption = Schema.decodeOption(stateFromJsonString);
const FRESH_STATE: LastAppliedFollowerSettingsValue = { revision: 0, fields: {} };

const make = Effect.gen(function* () {
  const controller = yield* Controller.CloudSandboxController;
  const settings = yield* ServerSettings.ServerSettingsService;
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  // writeFileStringAtomically needs the filesystem services at run time, but
  // the sync service methods keep their requirement set closed.
  const fileServices = yield* Effect.context<
    FileSystem.FileSystem | Path.Path | Effect.Services<ReturnType<typeof hostFileWrite>>
  >();

  const file = pathService.join(config.baseDir, "managed-follower-inheritance.json");

  const readState = Effect.fn("FollowerSettingsSync.readState")(function* () {
    const exists = yield* fs.exists(file).pipe(Effect.orElseSucceed(() => false));
    if (!exists) return FRESH_STATE;
    const raw = yield* fs.readFileString(file).pipe(Effect.option);
    if (Option.isNone(raw)) return FRESH_STATE;
    const decoded = decodeStateOption(raw.value);
    return Option.getOrElse(decoded, () => FRESH_STATE);
  });

  const saveState = (state: LastAppliedFollowerSettingsValue) =>
    writeFileStringAtomically({ filePath: file, contents: encodeState(state) }).pipe(
      Effect.provide(fileServices),
      Effect.mapError((cause) => new FollowerSettingsSyncError({ reason: "storage", cause })),
    );

  const lastApplied = readState();

  const sync = Effect.gen(function* () {
    if (
      !(yield* controller.available.pipe(
        Effect.mapError((cause) => new FollowerSettingsSyncError({ reason: "controller", cause })),
      ))
    )
      return { _tag: "not-follower" } as const;
    const state = yield* readState().pipe(
      Effect.mapError((cause) => new FollowerSettingsSyncError({ reason: "storage", cause })),
    );
    const pulled = yield* controller
      .request({ _tag: "hostSettings" })
      .pipe(
        Effect.mapError(
          (failure) => new FollowerSettingsSyncError({ reason: "controller", cause: failure }),
        ),
      );
    if (pulled._tag !== "hostSettings")
      return yield* new FollowerSettingsSyncError({
        reason: "controller",
        cause: new Error("The controller answered an unexpected response."),
      });
    const snapshot: FollowerHostSettingsSnapshot = pulled.snapshot;
    const decision = followerSettingsInheritancePatch(snapshot, state);
    if (decision._tag === "skip") return { _tag: "unchanged" } as const;
    if (decision._tag === "refuse") return { _tag: "refused", reason: decision.reason } as const;
    yield* settings
      .updateSettings(decision.patch)
      .pipe(
        Effect.mapError((cause) => new FollowerSettingsSyncError({ reason: "storage", cause })),
      );
    yield* saveState({ revision: snapshot.revision, fields: snapshot.fields });
    return { _tag: "applied", revision: snapshot.revision } as const;
  }).pipe(Effect.withSpan("FollowerSettingsSync.sync"));

  const start = Effect.gen(function* () {
    const available = yield* controller.available.pipe(Effect.orElseSucceed(() => false));
    if (!available) return;
    const tick = sync.pipe(
      Effect.tapError((error) =>
        Effect.logWarning("Follower settings pull failed.", { reason: error.reason }),
      ),
      Effect.ignore,
      Effect.andThen(Effect.sleep("30 seconds")),
      Effect.forever,
      Effect.catchCause((cause) => Effect.logWarning("Follower settings sync stopped.", cause)),
      Effect.forkScoped,
      Effect.asVoid,
    );
    yield* tick;
  });

  return FollowerSettingsSync.of({ sync, start, lastApplied });
});

export const layer = Layer.effect(FollowerSettingsSync, make);
