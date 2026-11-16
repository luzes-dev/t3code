/**
 * `t3 sandbox-bridge`: the ACP agent behind the `sandboxes` provider instance. It runs on the host,
 * one process per thread, and forwards the thread to its sandbox's own T3 server. It reads the
 * sandbox's address and bearer from this server's secret store, named by the instance environment,
 * so no credential ever appears on a command line.
 */
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect has no synchronous digest.
import * as NodeCrypto from "node:crypto";

import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ModelSelection, ProviderInteractionMode, RuntimeMode, ThreadId } from "@t3tools/contracts";
import {
  connectSandbox,
  type HostMcp,
  type HostSecretAnswer,
  type HostSecretRequest,
  runSandboxBridge,
  SandboxBridgeError,
  type SandboxBridgePorts,
} from "@t3tools/sandbox-bridge";
import {
  bridgeLinkSecretName,
  bridgeResumeFailureSecretName,
  SANDBOX_BRIDGE_ATTACHMENTS_DIR_ENV,
  SANDBOX_BRIDGE_SECRETS_DIR_ENV,
  SANDBOX_BRIDGE_TRACE_FILE_ENV,
  SandboxBridgeLinkJson,
  SandboxBridgeResumeFailureJson,
} from "@t3tools/sandbox-bridge/link";
import { makeLocalFileTracer } from "@t3tools/shared/observability";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as RcMap from "effect/RcMap";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as Tracer from "effect/Tracer";
import { FetchHttpClient } from "effect/http";
import * as AcpAgent from "effect-acp/agent";

import { createDeterministicAttachmentId } from "../attachmentStore.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import { mcpToolSession } from "../mcp/AcpMcpStdioBridge.ts";

const HostThreadConfiguration = Schema.Struct({
  threadId: ThreadId,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
});
const decodeHostThread = Schema.decodeUnknownEffect(
  Schema.Struct({ structuredContent: HostThreadConfiguration }),
);
// `request_secret` with `delivery: "file"`; only the status and the file matter here.
const decodeSecretResult = Schema.decodeUnknownEffect(
  Schema.Struct({
    structuredContent: Schema.Struct({
      status: Schema.Literals(["saved", "declined", "cancelled", "timed_out"]),
      file: Schema.optional(Schema.String),
    }),
  }),
);
// Below fetch's five-minute wait for response headers, which the blocking call sends only at the end.
const SECRET_CARD_TIMEOUT_MS = 280_000;
const decodeLink = Schema.decodeEffect(SandboxBridgeLinkJson);
const decodeResumeFailure = Schema.decodeEffect(SandboxBridgeResumeFailureJson);
// Opening an MCP session costs three requests and ending it a fourth; reads come every turn.
const HOST_SESSION_IDLE_TTL = "10 minutes";

const program = Effect.fn("SandboxBridgeCli.program")(function* (
  attachmentsDir: string | undefined,
) {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const hostMcpByKey = new Map<string, HostMcp>();
  const hostSessions = yield* RcMap.make({
    lookup: (key: string) => {
      const mcp = hostMcpByKey.get(key);
      return mcp === undefined
        ? Effect.die("No host MCP credential for this session.")
        : mcpToolSession({ ...mcp, clientName: "t3-sandbox-bridge" });
    },
    idleTimeToLive: HOST_SESSION_IDLE_TTL,
  });
  const callHostOnce = (key: string) =>
    RcMap.get(hostSessions, key).pipe(
      Effect.flatMap((session) => session.call("t3_thread_configure", {})),
      Effect.tapError(() => RcMap.invalidate(hostSessions, key)),
      Effect.scoped,
    );
  // A file request hands back an owner-only file; the bridge takes the value and removes it.
  const takeSecretFile = (file: string) =>
    fileSystem.readFileString(file).pipe(
      Effect.ensuring(fileSystem.remove(file, { force: true }).pipe(Effect.ignore)),
      Effect.map((value) => ({ status: "saved" as const, value: Redacted.make(value) })),
    );
  const ports: SandboxBridgePorts = {
    // The host's thread-scoped MCP credential answers for exactly the thread this process serves.
    // A session the host ended (a restart, its expiry) gets one retry on a new one.
    readHostThread: (mcp) => {
      const key = `${mcp.endpoint}\u0000${mcp.authorization}`;
      hostMcpByKey.set(key, mcp);
      return callHostOnce(key).pipe(
        Effect.catch(() => callHostOnce(key)),
        Effect.flatMap(decodeHostThread),
        Effect.map((result) => result.structuredContent),
        Effect.mapError(
          (cause) => new SandboxBridgeError({ message: "Could not read the host thread.", cause }),
        ),
      );
    },
    findLink: (threadId) =>
      secrets.get(bridgeLinkSecretName(threadId)).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeedNone,
            onSome: (bytes) => decodeLink(new TextDecoder().decode(bytes)).pipe(Effect.asSome),
          }),
        ),
        Effect.mapError(
          (cause) =>
            new SandboxBridgeError({
              message: "Could not read this thread's sandbox link.",
              cause,
            }),
        ),
      ),
    findResumeFailure: (threadId) =>
      secrets.get(bridgeResumeFailureSecretName(threadId)).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeedNone,
            onSome: (bytes) =>
              decodeResumeFailure(new TextDecoder().decode(bytes)).pipe(Effect.asSome),
          }),
        ),
        Effect.mapError(
          (cause) =>
            new SandboxBridgeError({
              message: "Could not read whether this thread's sandbox failed to start.",
              cause,
            }),
        ),
      ),
    connect: (link) => connectSandbox(link).pipe(Effect.provide(FetchHttpClient.layer)),
    requestSecret: Effect.fn("SandboxBridge.requestSecret")(
      function* (mcp: HostMcp, request: HostSecretRequest) {
        const answer = yield* mcpToolSession({ ...mcp, clientName: "t3-sandbox-bridge" }).pipe(
          Effect.flatMap((session) =>
            session.call("request_secret", {
              ...request,
              delivery: "file",
              timeoutMs: SECRET_CARD_TIMEOUT_MS,
            }),
          ),
          Effect.scoped,
          Effect.flatMap(decodeSecretResult),
        );
        const { status, file } = answer.structuredContent;
        const unsaved: HostSecretAnswer | undefined = status === "saved" ? undefined : { status };
        if (unsaved !== undefined) return unsaved;
        if (file === undefined) {
          return yield* new SandboxBridgeError({
            message: "The host kept no file for the secret.",
          });
        }
        return yield* takeSecretFile(file);
      },
      // No cause: nothing about the answer leaves this function but the value itself.
      Effect.mapError(
        () => new SandboxBridgeError({ message: "Could not ask for the sandbox's secret." }),
      ),
    ),
    // Named by the thread and the bytes, so a reply shown again reuses its copy.
    ...(attachmentsDir === undefined
      ? {}
      : {
          saveImage: Effect.fn("SandboxBridge.saveImage")(
            function* (threadId, image) {
              const digest = NodeCrypto.createHash("sha256").update(image.bytes).digest("hex");
              const id = createDeterministicAttachmentId(threadId, `sandbox-image:${digest}`);
              if (id === null) {
                return yield* new SandboxBridgeError({ message: "No file name for the image." });
              }
              const target = path.join(attachmentsDir, `${id}${image.extension}`);
              if (yield* fileSystem.exists(target)) return target;
              // `.part`: a copy cut short is swept with the host's other partial uploads.
              const partial = `${target}.part`;
              yield* fileSystem.writeFile(partial, image.bytes);
              yield* fileSystem.rename(partial, target);
              return target;
            },
            Effect.mapError(
              (cause) =>
                new SandboxBridgeError({ message: "Could not keep the image on the host.", cause }),
            ),
          ),
        }),
  };
  return yield* runSandboxBridge(ports);
}, Effect.scoped);

/**
 * The bridge's spans (a turn, its connect, each sandbox RPC and host read) in the server's trace
 * format, in the file the host names; every bridge process appends to it. A file that cannot be
 * opened leaves the bridge untraced rather than stopped.
 */
const layerTracer = (filePath: string | undefined) =>
  filePath === undefined
    ? Layer.empty
    : Layer.effect(
        Tracer.Tracer,
        makeLocalFileTracer({
          filePath,
          maxBytes: 4 * 1024 * 1024,
          maxFiles: 2,
          batchWindowMs: 200,
        }).pipe(Effect.catchCause(() => Effect.succeed(Tracer.nativeTracer))),
      );

export async function runSandboxBridgeCli(): Promise<void> {
  const secretsDir = process.env[SANDBOX_BRIDGE_SECRETS_DIR_ENV];
  if (secretsDir === undefined || secretsDir === "") {
    process.stderr.write(`sandbox-bridge requires ${SANDBOX_BRIDGE_SECRETS_DIR_ENV}.\n`);
    process.exitCode = 2;
    return;
  }
  // stdout carries ACP, so every log line, including a failure runMain reports, goes to stderr.
  console.log = console.info = console.debug = console.error;
  // The secret store only reads `secretsDir`; the rest of the server's config does not apply here.
  const config = Layer.succeed(ServerConfig.ServerConfig, {
    secretsDir,
  } as unknown as ServerConfig.ServerConfig["Service"]);
  const secrets = Layer.effect(ServerSecretStore.ServerSecretStore, ServerSecretStore.make).pipe(
    Layer.provide(Layer.mergeAll(config, SecureRandom.layerWebCrypto)),
  );
  // The agent answers stdin as soon as it is built, so everything the handlers use is built first.
  const services = AcpAgent.layerStdio().pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        secrets,
        SecureRandom.layerWebCrypto,
        layerTracer(process.env[SANDBOX_BRIDGE_TRACE_FILE_ENV] || undefined),
      ),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
  const attachmentsDir = process.env[SANDBOX_BRIDGE_ATTACHMENTS_DIR_ENV] || undefined;
  program(attachmentsDir).pipe(Effect.provide(services), NodeRuntime.runMain);
}
