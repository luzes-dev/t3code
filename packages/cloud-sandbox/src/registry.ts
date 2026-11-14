import { createBoatApiClient } from "@t3tools/boat";
import type { CloudSandboxDriverKind } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { HttpClient } from "effect/http";
import { CloudSandboxError, type BaseCloudSandboxDriver } from "./driver.ts";
import { createBoatCloudSandboxDriver } from "./drivers/boat/boatDriver.ts";

export const supportedCloudSandboxDrivers = [{ kind: "boat", label: "Boat.dev" }] as const;

export const createCloudSandboxDriver = Effect.fn("CloudSandboxDriverRegistry.create")(function* (
  kind: CloudSandboxDriverKind,
  options: { readonly apiKey: string; readonly httpClient?: HttpClient.HttpClient },
) {
  if (kind !== "boat")
    return yield* new CloudSandboxError({
      reason: "configuration",
      cause: new Error("Unsupported cloud sandbox provider."),
    });
  return createBoatCloudSandboxDriver(
    createBoatApiClient({ ...options, apiKey: options.apiKey.trim() }),
  );
});

/**
 * Drivers come from the registry by default. Services resolve this factory so
 * tests can stand in a second, non-registry driver without touching the manager.
 */
export interface CloudSandboxDriverFactoryService {
  readonly create: (
    kind: CloudSandboxDriverKind,
    options: { readonly apiKey: string; readonly httpClient?: HttpClient.HttpClient },
  ) => Effect.Effect<BaseCloudSandboxDriver, CloudSandboxError>;
}
export class CloudSandboxDriverFactory extends Context.Service<
  CloudSandboxDriverFactory,
  CloudSandboxDriverFactoryService
>()("@t3tools/cloud-sandbox/registry/CloudSandboxDriverFactory") {}

export const defaultCloudSandboxDriverFactory = CloudSandboxDriverFactory.of({
  create: createCloudSandboxDriver,
});
export const cloudSandboxDriverFactoryLayer = Layer.succeed(
  CloudSandboxDriverFactory,
  defaultCloudSandboxDriverFactory,
);
