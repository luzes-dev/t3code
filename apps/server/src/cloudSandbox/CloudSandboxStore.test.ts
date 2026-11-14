import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId, ManagedCloudSandboxResourceId, SandboxProfileId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Store from "./CloudSandboxStore.ts";

it.effect("reads legacy records and preserves valid cost reservations through persistence", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.CloudSandboxStore;
    const legacy: Store.CloudSandboxRecord = {
      resourceId: ManagedCloudSandboxResourceId.make("legacy"),
      controllerEnvironmentId: EnvironmentId.make("controller"),
      driverKind: "boat",
      profileId: SandboxProfileId.make("boat"),
      sandboxId: "sandbox",
      parentResourceId: null,
      owner: { _tag: "user" },
      retention: "manual",
      environmentId: null,
      label: "Sandbox",
      recordingIds: [],
      lifecycle: "active",
      power: "running",
      intent: "connected",
      selection: { name: "sandbox", size: "small", autoStopMinutes: 15 },
      snapshotName: null,
      snapshotReady: false,
      deletionOperationId: null,
      finalSpendUsd: null,
      enableLinkedEnvironments: false,
    };
    yield* sql`INSERT INTO managed_cloud_sandboxes(resource_id, payload_json) VALUES (${legacy.resourceId}, ${JSON.stringify(legacy)})`;
    const loaded = Option.getOrThrow(yield* store.get(legacy.resourceId));
    expect(loaded).toEqual(legacy);
    const reservation = { size: "xlarge", autoStopMinutes: 120 };
    yield* store.save({ ...loaded, reservation });
    expect(Option.getOrThrow(yield* store.get(legacy.resourceId))).toEqual({
      ...legacy,
      reservation,
    });
    yield* sql`UPDATE managed_cloud_sandboxes SET payload_json = ${JSON.stringify({ ...legacy, reservation: { ...reservation, autoStopMinutes: 0 } })}`;
    expect(yield* store.get(legacy.resourceId).pipe(Effect.result)).toMatchObject({
      _tag: "Failure",
      failure: { reason: "storage" },
    });
  }).pipe(
    Effect.provide(
      Store.layer.pipe(Layer.provideMerge(Sqlite.layerMemory), Layer.provide(NodeServices.layer)),
    ),
    Effect.scoped,
  ),
);
