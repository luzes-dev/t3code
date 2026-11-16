import { SANDBOX_BRIDGE_MODEL, type ServerProvider, ServerProviderModel } from "@t3tools/contracts";
import { writeFileStringAtomically } from "@t3tools/shared/atomicWrite";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import type { AcpRegistryLiveConfiguration } from "./probe.ts";

const ModelsJson = Schema.fromJsonString(Schema.Array(ServerProviderModel));
const decodeModels = Schema.decodeUnknownEffect(ModelsJson);
const encodeModels = Schema.encodeEffect(ModelsJson);

const isListed = (model: ServerProviderModel) =>
  !model.isCustom && model.slug !== SANDBOX_BRIDGE_MODEL;

/**
 * Every model a sandbox chat reported, each as its newest report has it. One instance serves every
 * sandbox chat, each session reads its own sandbox, and one that does not answer reports nothing
 * or only the bridge's `sandbox` placeholder, so a report adds to the list and never shrinks it.
 */
const mergeSandboxBridgeModels = (
  known: ReadonlyArray<ServerProviderModel>,
  reported: ReadonlyArray<ServerProviderModel>,
  defaultModelId: string | undefined,
): ReadonlyArray<ServerProviderModel> => {
  const fresh = new Map(reported.filter(isListed).map((model) => [model.slug, model]));
  if (fresh.size === 0) return known;
  const knownSlugs = new Set(known.map((model) => model.slug));
  const merged = [
    ...known.map((model) => fresh.get(model.slug) ?? model),
    ...[...fresh.values()].filter((model) => !knownSlugs.has(model.slug)),
  ];
  // The marker names what a chat gets with nothing picked, never the reporting chat's own pick.
  const defaultSlug = [defaultModelId, known.find((model) => model.isDefault)?.slug].find(
    (slug) => slug !== undefined && merged.some((model) => model.slug === slug),
  );
  return merged.map(({ isDefault: _isDefault, ...model }) =>
    model.slug === defaultSlug ? { ...model, isDefault: true } : model,
  );
};

/** The provider listing the known models instead of the bridge's placeholder, once any is known. */
const withSandboxBridgeModels = (
  provider: ServerProvider,
  known: ReadonlyArray<ServerProviderModel>,
): ServerProvider => {
  if (known.length === 0) return provider;
  const slugs = new Set(known.map((model) => model.slug));
  return {
    ...provider,
    models: [
      ...known,
      ...provider.models.filter((model) => model.isCustom && !slugs.has(model.slug)),
    ],
  };
};

/**
 * The `sandboxes` instance's model list, kept beside the provider status cache so a host restart
 * lists what sandboxes reported before instead of the probe's placeholder.
 */
export const makeSandboxBridgeModels = Effect.fn("SandboxBridgeModels.make")(function* (
  cacheDir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const filePath = path.join(cacheDir, "sandbox-bridge-models.json");
  const saved = yield* fs.readFileString(filePath).pipe(
    Effect.flatMap(decodeModels),
    Effect.map((models) => models.filter(isListed)),
    Effect.orElseSucceed((): ReadonlyArray<ServerProviderModel> => []),
  );
  const known = yield* Ref.make(saved);
  const lock = yield* Semaphore.make(1);
  const persist = (models: ReadonlyArray<ServerProviderModel>) =>
    encodeModels(models).pipe(
      Effect.flatMap((contents) => writeFileStringAtomically({ filePath, contents })),
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.tapError(() => Effect.logWarning("Could not save the sandbox models.")),
      Effect.ignore,
    );
  return {
    known: Ref.get(known),
    publish: (provider: ServerProvider) =>
      Ref.get(known).pipe(Effect.map((models) => withSandboxBridgeModels(provider, models))),
    /** Takes a session's report, its live configuration already applied to `provider`. */
    report: (provider: ServerProvider, configuration: AcpRegistryLiveConfiguration) =>
      lock.withPermit(
        Effect.gen(function* () {
          const previous = yield* Ref.get(known);
          // An empty or placeholder-only report falls back to the probe's list: nothing to learn.
          const next = configuration.models.some((model) => model.id !== SANDBOX_BRIDGE_MODEL)
            ? mergeSandboxBridgeModels(previous, provider.models, configuration.defaultModelId)
            : previous;
          if (!Equal.equals(next, previous)) {
            yield* Ref.set(known, next);
            yield* persist(next);
          }
          return withSandboxBridgeModels(provider, next);
        }),
      ),
  };
});
