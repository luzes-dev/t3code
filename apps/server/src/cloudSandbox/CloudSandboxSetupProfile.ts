import type { CloudSandboxSetup } from "@t3tools/contracts";
import { shellSingleQuote } from "@t3tools/ssh/command";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Base64 from "effect/encoding/Base64";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

/** The copy goes over one SSH stdin; larger working sets belong in a template snapshot. */
const SETUP_PROFILE_MAX_BYTES = 16 * 1024 * 1024;
const SETUP_PROFILE_MAX_FILES = 5_000;

export class CloudSandboxSetupError extends Schema.TaggedError<CloudSandboxSetupError>()(
  "CloudSandboxSetupError",
  { message: Schema.String, cause: Schema.optionalKey(Schema.Defect()) },
) {}

export interface SetupFile {
  /** Relative to the home directory, with `/` separators. */
  readonly path: string;
  readonly bytes: Uint8Array;
  readonly executable: boolean;
}

/** Reads the listed home paths, walking directories. A path missing on this machine is skipped. */
export const collectSetupFiles = Effect.fn("CloudSandboxSetupProfile.collect")(function* (
  home: string,
  paths: ReadonlyArray<string>,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const files: Array<SetupFile> = [];
  let total = 0;
  // One call per file and folder; the collect span already covers the walk.
  const visit: (relative: string) => Effect.Effect<void, CloudSandboxSetupError> =
    Effect.fnUntraced(function* (relative: string) {
      const absolute = path.join(home, ...relative.split("/"));
      const info = yield* fs.stat(absolute).pipe(Effect.option);
      if (info._tag === "None") return;
      if (info.value.type === "Directory") {
        const names = yield* fs
          .readDirectory(absolute)
          .pipe(
            Effect.mapError(
              (cause) =>
                new CloudSandboxSetupError({ message: `Could not read ~/${relative}.`, cause }),
            ),
          );
        for (const name of names.toSorted()) yield* visit(`${relative}/${name}`);
        return;
      }
      if (info.value.type !== "File") return;
      const bytes = yield* fs
        .readFile(absolute)
        .pipe(
          Effect.mapError(
            (cause) =>
              new CloudSandboxSetupError({ message: `Could not read ~/${relative}.`, cause }),
          ),
        );
      total += bytes.byteLength;
      if (total > SETUP_PROFILE_MAX_BYTES || files.length >= SETUP_PROFILE_MAX_FILES)
        return yield* new CloudSandboxSetupError({
          message: "The setup files are too large to copy. Put large tools in a template snapshot.",
        });
      files.push({ path: relative, bytes, executable: (info.value.mode & 0o111) !== 0 });
    });
  for (const entry of paths) yield* visit(entry.replaceAll("\\", "/").replace(/\/+$/u, ""));
  return files;
});

/**
 * A script the sandbox runs on every connect: it returns at once when its marker names this
 * working set, and otherwise writes the files, runs the setup script, and records the marker.
 * A failed setup script leaves the marker alone, so the next connect tries again.
 */
export const setupProfileScript = Effect.fn("CloudSandboxSetupProfile.script")(function* (
  files: ReadonlyArray<SetupFile>,
  setup: CloudSandboxSetup,
) {
  const digest = yield* Digest.Digest;
  const encoder = new TextEncoder();
  const parts = [
    ...files.flatMap((file) => [
      encoder.encode(
        `${file.path}\u0000${file.executable ? "x" : "-"}\u0000${file.bytes.byteLength}\u0000`,
      ),
      file.bytes,
    ]),
    encoder.encode(`script\u0000${setup.script ?? ""}`),
  ];
  const joined = new Uint8Array(parts.reduce((size, part) => size + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    joined.set(part, offset);
    offset += part.byteLength;
  }
  const hash = Hex.encode(yield* digest.digest(Digest.sha256, joined));
  const home = (relative: string) => `"$HOME"/${shellSingleQuote(relative)}`;
  const lines = [
    "set -eu",
    "umask 077",
    'marker="$HOME/.t3/setup-profile.sha256"',
    `if [ "$(cat "$marker" 2>/dev/null || true)" = ${hash} ]; then exit 0; fi`,
    'mkdir -p "$HOME/.t3"',
  ];
  for (const file of files) {
    const parent = file.path.includes("/") ? file.path.slice(0, file.path.lastIndexOf("/")) : null;
    if (parent !== null) lines.push(`mkdir -p ${home(parent)}`);
    lines.push(`base64 -d > ${home(file.path)} <<'T3EOF'`, Base64.encode(file.bytes), "T3EOF");
    if (file.executable) lines.push(`chmod 700 ${home(file.path)}`);
  }
  if (setup.script !== null) {
    lines.push(
      `base64 -d > "$HOME/.t3/setup-profile.sh" <<'T3EOF'`,
      Base64.encode(encoder.encode(setup.script)),
      "T3EOF",
      'if ! sh "$HOME/.t3/setup-profile.sh" > "$HOME/.t3/setup-profile.log" 2>&1; then tail -n 20 "$HOME/.t3/setup-profile.log" >&2; exit 1; fi',
    );
  }
  lines.push(`printf %s ${hash} > "$marker"`);
  return { hash, script: `${lines.join("\n")}\n` };
});
