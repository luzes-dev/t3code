/**
 * A minimal WebSocket RPC client for a sandbox's T3 server, holding only the bridge's bearer.
 * client-runtime's session stack carries browser state the bridge has no use for, so this speaks
 * `WsRpcGroup` directly, the way the outdated-host updater does.
 */
import {
  AuthWebSocketTicketResult,
  type CheckpointId,
  type MessageId,
  type NodeId,
  ORCHESTRATION_PROTOCOL_QUERY_PARAM,
  ORCHESTRATION_PROTOCOL_VERSION,
  ORCHESTRATION_V2_WS_METHODS,
  type OrchestrationV2Checkpoint,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadStreamItem,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import { boundedSnapshotProjection } from "@t3tools/shared/orchestrationV2BoundedSnapshot";
import {
  type DedupedBodySource,
  restoreSubagentBodies,
  restoreTurnItemBodies,
} from "@t3tools/shared/orchestrationV2DedupedBodies";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import * as RpcClient from "effect/rpc/RpcClient";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Socket from "effect/socket/Socket";

import { SandboxBridgeError, type SandboxSession } from "./bridge.ts";
import { RELAYED_IMAGE_MAX_BYTES, RELAYED_IMAGE_TYPES, relayedImageExtension } from "./images.ts";
import type { SandboxBridgeLink } from "./link.ts";

const OPEN_TIMEOUT = "15 seconds";
const PING_TIMEOUT = "15 seconds";

const failed = (message: string) => (cause: unknown) => new SandboxBridgeError({ message, cause });

// Twice what the server's deduper remembers, so every body it blanks is still held here.
const RESTORE_MEMORY = 64;

const remember = <K, V>(memory: Map<K, V>, key: K, value: V) => {
  memory.delete(key);
  memory.set(key, value);
  if (memory.size > RESTORE_MEMORY) memory.delete(memory.keys().next().value as K);
};

/**
 * Undoes a compact subscription, per stream: the snapshot's omitted rows and bodies, and each live
 * item's body the server blanked because a record it sent just before carries it.
 */
const makeStreamRestorer = () => {
  const messageText = new Map<MessageId, string>();
  const checkpointFiles = new Map<CheckpointId, OrchestrationV2Checkpoint["files"]>();
  const subagents = new Map<NodeId, OrchestrationV2Subagent>();
  const source: DedupedBodySource = {
    messageText: (id) => messageText.get(id),
    checkpointFiles: (id) => checkpointFiles.get(id),
    subagent: (id) => subagents.get(id),
  };
  return (item: OrchestrationV2ThreadStreamItem): OrchestrationV2ThreadStreamItem => {
    if (item.kind === "snapshot") {
      const { turnItemsOmitLocalVisible: _omitted, ...rest } = item;
      const projection = boundedSnapshotProjection(item);
      for (const message of projection.messages) remember(messageText, message.id, message.text);
      for (const checkpoint of projection.checkpoints) {
        remember(checkpointFiles, checkpoint.id, checkpoint.files);
      }
      for (const subagent of projection.subagents) remember(subagents, subagent.id, subagent);
      return { ...rest, projection };
    }
    if (item.kind !== "event") return item;
    const event = item.event;
    switch (event.type) {
      case "message.updated":
        remember(messageText, event.payload.id, event.payload.text);
        return item;
      case "checkpoint.captured":
        remember(checkpointFiles, event.payload.id, event.payload.files);
        return item;
      case "subagent.updated": {
        const payload = restoreSubagentBodies(event.payload, subagents.get(event.payload.id));
        remember(subagents, payload.id, payload);
        return payload === event.payload ? item : { ...item, event: { ...event, payload } };
      }
      case "turn-item.updated": {
        const payload = restoreTurnItemBodies(event.payload, source);
        return payload === event.payload ? item : { ...item, event: { ...event, payload } };
      }
      default:
        return item;
    }
  };
};

/**
 * Mirrors the clients: a bare origin serves RPC at `/ws`, and the socket names the orchestration
 * protocol it speaks, without which the server refuses the upgrade with 426.
 */
export const sandboxSocketUrl = (httpBaseUrl: string, ticket: string): string => {
  const url = new URL(httpBaseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  if (url.pathname === "" || url.pathname === "/") url.pathname = "/ws";
  url.searchParams.set("wsTicket", ticket);
  url.searchParams.set(ORCHESTRATION_PROTOCOL_QUERY_PARAM, String(ORCHESTRATION_PROTOCOL_VERSION));
  return url.toString();
};

const issueTicket = Effect.fn("SandboxBridge.issueTicket")(function* (link: SandboxBridgeLink) {
  const http = yield* HttpClient.HttpClient;
  const base = link.httpBaseUrl.replace(/\/+$/u, "");
  const response = yield* http
    .execute(
      HttpClientRequest.post(`${base}/api/auth/websocket-ticket/rpc`).pipe(
        HttpClientRequest.bearerToken(link.token),
      ),
    )
    .pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.flatMap(HttpClientResponse.schemaBodyJson(AuthWebSocketTicketResult)),
      Effect.mapError(failed(`Sandbox ${link.sandboxId} did not issue a connection ticket.`)),
    );
  return Redacted.value(response.ticket);
});

export const connectSandbox = Effect.fn("SandboxBridge.connectSandbox")(function* (
  link: SandboxBridgeLink,
): Effect.fn.Return<SandboxSession, SandboxBridgeError, Scope.Scope | HttpClient.HttpClient> {
  const http = yield* HttpClient.HttpClient;
  const ticket = yield* issueTicket(link);
  const protocol = yield* Layer.build(
    Layer.effect(
      RpcClient.Protocol,
      RpcClient.makeProtocolSocket({
        pingTimeout: PING_TIMEOUT,
        retryTransientErrors: false,
        retryPolicy: Schedule.recurs(0),
      }),
    ).pipe(
      Layer.provide(
        Layer.mergeAll(
          Socket.layerWebSocket(sandboxSocketUrl(link.httpBaseUrl, ticket), {
            openTimeout: OPEN_TIMEOUT,
          }).pipe(Layer.provide(Socket.layerWebSocketConstructorGlobal)),
          RpcSerialization.layerJson,
        ),
      ),
    ),
  );
  const client = yield* RpcClient.make(WsRpcGroup).pipe(Effect.provide(protocol));
  const config = yield* client[WS_METHODS.serverGetConfig]({}).pipe(
    Effect.mapError(failed(`Sandbox ${link.sandboxId} did not answer.`)),
  );
  return {
    threadStop: config.environment.capabilities.threadStop === true,
    defaultModelSelection: config.settings.defaultModelSelection ?? null,
    providers: config.providers,
    projection: (threadId) =>
      client[ORCHESTRATION_V2_WS_METHODS.getThreadProjection]({ threadId }).pipe(
        Effect.asSome,
        // The sandbox reports a missing thread as this error; anything else is a real failure.
        Effect.catchTags({ OrchestrationV2GetThreadProjectionError: () => Effect.succeedNone }),
        Effect.mapError(failed("Could not read the thread on the sandbox.")),
      ),
    launch: (input) =>
      client[ORCHESTRATION_V2_WS_METHODS.launchThread](input).pipe(
        Effect.asVoid,
        Effect.mapError(failed("Could not start the thread on the sandbox.")),
      ),
    dispatch: (command) =>
      client[ORCHESTRATION_V2_WS_METHODS.dispatchCommand](command).pipe(
        Effect.asVoid,
        Effect.mapError(failed("The sandbox refused the command.")),
      ),
    persistAttachments: (input) =>
      client[WS_METHODS.assetsPersistChatAttachments]({
        threadId: input.threadId,
        messageId: input.messageId,
        attachments: [...input.attachments],
      }).pipe(
        Effect.map((result) => result.attachments),
        Effect.mapError(failed("The sandbox did not accept the images.")),
      ),
    // Compact, then restored here, so the bridge and the mirror read whole items.
    subscribe: (threadId) =>
      Stream.suspend(() => {
        const restore = makeStreamRestorer();
        let started = false;
        return client[ORCHESTRATION_V2_WS_METHODS.subscribeThread]({
          threadId,
          acceptBoundedSnapshot: true,
          acceptCompactTurnItems: true,
          acceptDedupedBodies: true,
        }).pipe(
          Stream.map((item) => {
            started = true;
            return restore(item);
          }),
          // The sandbox reports a missing thread as this error, before any item.
          Stream.catchIf(
            (error) => !started && error._tag === "OrchestrationV2GetThreadProjectionError",
            () => Stream.empty,
          ),
          Stream.mapError(failed("Lost the sandbox's thread stream.")),
        );
      }),
    // No cause: an encoding failure could quote the value it carried.
    answerSecret: (input) =>
      client[WS_METHODS.secretsAnswerRequest](input).pipe(
        Effect.mapError(
          () => new SandboxBridgeError({ message: "The sandbox did not take the secret." }),
        ),
      ),
    // Signed by the sandbox for its own clients' <img>, so the download carries no bearer.
    readImage: Effect.fn("SandboxBridge.readImage")(function* (threadId, path) {
      const notAnImage = new SandboxBridgeError({ message: "Not an image T3 shows." });
      const extension = relayedImageExtension(path);
      if (extension === undefined) return yield* notAnImage;
      const asset = yield* client[WS_METHODS.assetsCreateUrl]({
        resource: { _tag: "media-file", threadId, path },
      }).pipe(Effect.mapError(failed("The sandbox would not share the image.")));
      const response = yield* http
        .execute(
          HttpClientRequest.get(`${link.httpBaseUrl.replace(/\/+$/u, "")}${asset.relativeUrl}`),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.mapError(failed("Could not download the image from the sandbox.")),
        );
      const type = response.headers["content-type"]?.split(";", 1)[0]?.trim().toLowerCase();
      const declared = Number(response.headers["content-length"] ?? 0);
      if (type !== RELAYED_IMAGE_TYPES[extension] || declared > RELAYED_IMAGE_MAX_BYTES) {
        return yield* notAnImage;
      }
      const body = yield* response.arrayBuffer.pipe(
        Effect.mapError(failed("Could not download the image from the sandbox.")),
      );
      if (body.byteLength === 0 || body.byteLength > RELAYED_IMAGE_MAX_BYTES) {
        return yield* notAnImage;
      }
      return { bytes: new Uint8Array(body), extension };
    }),
  } satisfies SandboxSession;
});
