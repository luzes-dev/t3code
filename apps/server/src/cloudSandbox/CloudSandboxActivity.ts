import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";

export class CloudSandboxActivity extends Context.Service<
  CloudSandboxActivity,
  {
    readonly isActive: Effect.Effect<boolean, ProjectionStore.ProjectionStoreV2Error>;
  }
>()("t3/cloudSandbox/CloudSandboxActivity") {}
const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  return CloudSandboxActivity.of({ isActive: Effect.suspend(() => projections.hasPendingWork()) });
});
export const layer = Layer.effect(CloudSandboxActivity, make);
