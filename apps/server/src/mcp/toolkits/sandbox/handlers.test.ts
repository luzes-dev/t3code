import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";
import * as Worktree from "../../WorktreeMcpService.ts";
import * as Handlers from "./handlers.ts";
import { ManagedSandboxToolkit } from "./tools.ts";

it.effect.each(["user", "other-link", "same-link", "full-access-link"] as const)(
  "authorizes sandbox setup against the %s target thread",
  (target) => {
    let setups = 0;
    const threadId = ThreadId.make("thread:setup-target");
    const access = target === "full-access-link" ? "full-access" : "approval-required";
    const dependencies = Layer.mergeAll(
      Layer.succeed(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment:sandbox"),
        requestNamespace: "linked:controller",
        thread: undefined,
        client: { sessionId: "controller", label: "Controller", access, linked: true },
        capabilities: new Set(["worktree", "orchestration"] as const),
        issuedAt: 0,
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () =>
          Effect.succeed({
            ...liveThreadShell(threadId, { runtimeMode: "approval-required" }),
            ...(target === "user" || target === "full-access-link"
              ? {}
              : {
                  linkOrigin: {
                    sessionId: target === "same-link" ? "controller" : "another-controller",
                    label: "Controller",
                    access: "approval-required" as const,
                  },
                }),
          }),
      }),
      Layer.mock(Worktree.WorktreeMcpService)({
        runSandboxSetup: () =>
          Effect.sync(() => {
            setups++;
            return { completed: true as const };
          }),
      }),
    );
    return Effect.gen(function* () {
      const toolkit = yield* ManagedSandboxToolkit;
      const results = yield* toolkit
        .handle("t3_worktree_run_sandbox_setup", {
          projectId: ProjectId.make("project:setup"),
          threadId,
        })
        .pipe(Stream.unwrap, Stream.runCollect);
      if (target === "same-link" || target === "full-access-link") {
        expect(results.at(-1)?.result).toEqual({ completed: true });
        expect(setups).toBe(1);
      } else {
        expect(results.at(-1)?.result).toMatchObject({ code: "capability_denied" });
        expect(setups).toBe(0);
      }
    }).pipe(
      Effect.provide(
        McpToolAccess.HandlersLayer.layer(Handlers.layer).pipe(Layer.provideMerge(dependencies)),
      ),
    );
  },
);
