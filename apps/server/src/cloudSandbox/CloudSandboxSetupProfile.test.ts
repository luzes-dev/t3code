import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import type { CloudSandboxSetup } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { collectSetupFiles, setupProfileScript } from "./CloudSandboxSetupProfile.ts";

// The script runs under a real shell against a scratch HOME standing in for the sandbox.
it.live("copies the working set once, again when it drifts, and retries a failed setup", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const controller = yield* fs.makeTempDirectoryScoped({ prefix: "t3-setup-controller-" });
    const sandbox = yield* fs.makeTempDirectoryScoped({ prefix: "t3-setup-sandbox-" });
    yield* fs.makeDirectory(`${controller}/.claude/skills/review`, { recursive: true });
    yield* fs.makeDirectory(`${controller}/bin`);
    yield* fs.writeFileString(`${controller}/.claude/CLAUDE.md`, "Use pnpm.\n");
    yield* fs.writeFileString(`${controller}/.claude/skills/review/SKILL.md`, "it's 'quoted'\n");
    yield* fs.writeFileString(`${controller}/bin/tool`, "#!/bin/sh\necho tool\n");
    yield* fs.chmod(`${controller}/bin/tool`, 0o755);

    const apply = (setup: CloudSandboxSetup) =>
      Effect.gen(function* () {
        const files = yield* collectSetupFiles(controller, setup.files);
        const { script } = yield* setupProfileScript(files, setup);
        yield* fs.writeFileString(`${sandbox}/apply.sh`, script);
        const child = yield* spawner.spawn(
          ChildProcess.make("sh", [`${sandbox}/apply.sh`], {
            env: { HOME: sandbox },
            extendEnv: true,
          }),
        );
        return yield* child.exitCode;
      }).pipe(Effect.scoped);
    const runs = fs
      .readFileString(`${sandbox}/runs`)
      .pipe(Effect.map((text) => text.trim().split("\n").length));
    const setup: CloudSandboxSetup = {
      files: [".claude", "bin/tool", ".not-on-this-machine"],
      script: 'echo run >> "$HOME/runs"',
    };

    expect(yield* apply(setup)).toBe(0);
    expect(yield* fs.readFileString(`${sandbox}/.claude/CLAUDE.md`)).toBe("Use pnpm.\n");
    expect(yield* fs.readFileString(`${sandbox}/.claude/skills/review/SKILL.md`)).toBe(
      "it's 'quoted'\n",
    );
    expect(((yield* fs.stat(`${sandbox}/bin/tool`)).mode & 0o100) !== 0).toBe(true);
    expect(yield* runs).toBe(1);

    // An unchanged working set is a no-op on later connects.
    expect(yield* apply(setup)).toBe(0);
    expect(yield* runs).toBe(1);

    yield* fs.writeFileString(`${controller}/.claude/CLAUDE.md`, "Use bun.\n");
    expect(yield* apply(setup)).toBe(0);
    expect(yield* fs.readFileString(`${sandbox}/.claude/CLAUDE.md`)).toBe("Use bun.\n");
    expect(yield* runs).toBe(2);

    // A failing setup records nothing, so the next connect runs it again.
    const failing = { ...setup, script: 'echo run >> "$HOME/runs"; exit 3' };
    expect(yield* apply(failing)).not.toBe(0);
    expect(yield* apply(failing)).not.toBe(0);
    expect(yield* runs).toBe(4);
  }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)), Effect.scoped),
);
