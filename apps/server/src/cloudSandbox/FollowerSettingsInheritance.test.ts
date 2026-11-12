import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  FollowerHostSettingsSnapshot,
  ServerSettings,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  FOLLOWER_INHERITED_SETTINGS_FIELDS,
  followerHostSettingsFields,
  followerSettingsFieldClass,
  followerSettingsInheritancePatch,
} from "./FollowerSettingsInheritance.ts";

const schemaField = (name: string): unknown =>
  ServerSettings.fields[name as keyof typeof ServerSettings.fields];
const settingsKeys = Object.keys(ServerSettings.fields);

const decodeSnapshotJson = Schema.decodeSync(Schema.fromJsonString(FollowerHostSettingsSnapshot));

const snapshot = (
  revision: number,
  fields: Record<string, unknown>,
): FollowerHostSettingsSnapshot => decodeSnapshotJson(JSON.stringify({ revision, fields }));

const MODEL_SELECTION = { instanceId: "provider-core-codex", model: "custom-model" };

describe("follower settings inheritance", () => {
  it("classifies every ServerSettings field, failing closed on unknown fields", () => {
    // The allowlist names only real settings fields.
    for (const field of FOLLOWER_INHERITED_SETTINGS_FIELDS)
      expect(schemaField(field), field).toBeDefined();
    for (const key of settingsKeys) {
      expect(followerSettingsFieldClass(key), key).toBe(
        (FOLLOWER_INHERITED_SETTINGS_FIELDS as readonly string[]).includes(key)
          ? "inherit"
          : "host-only",
      );
    }
    // A field no release added to the allowlist is host-only.
    expect(followerSettingsFieldClass("someSettingsFieldAddedTomorrow")).toBe("host-only");
    // The settings schema keys are the full population; nothing unclassified is possible.
    expect(settingsKeys.length).toBeGreaterThan(70);
  });

  it("keeps secrets, secret references, host paths, provider instances, source-control credentials, and identity config host-only", () => {
    const hostOnly = (key: string) => expect(followerSettingsFieldClass(key)).toBe("host-only");
    // Secrets and secret references.
    hostOnly("cloudSpeechCredentials");
    hostOnly("secretProviders");
    hostOnly("providerInstances");
    hostOnly("textGenerators");
    // Host paths.
    hostOnly("worktreesDirectory");
    hostOnly("previousWorktreesDirectories");
    hostOnly("addProjectBaseDirectory");
    // Provider instances and pricing/limit records.
    hostOnly("usageLimitSources");
    hostOnly("usagePriceOverrides");
    hostOnly("usageModelAliases");
    // Source-control credentials.
    hostOnly("sourceControlHosts");
    hostOnly("sourceControlWritingStyle");
    // Identity config and appearance.
    hostOnly("defaultTheme");
    hostOnly("defaultThemeSetAt");
    hostOnly("environmentIcon");
    hostOnly("observability");
    // Device credentials and access toggles.
    hostOnly("deviceHosts");
    hostOnly("easDeviceHosts");
    hostOnly("simlockDeviceHosts");
    // Sandbox-local data the child may own (project words live with it).
    hostOnly("speechProjectCustomWords");
    hostOnly("projectSettingsOverrides");
  });

  it("seeds only allowed non-default fields from the host", () => {
    const host = {
      ...DEFAULT_SERVER_SETTINGS,
      defaultModelSelection: MODEL_SELECTION,
      defaultRuntimeMode: "approval-required" as const,
      defaultAutoPull: DEFAULT_SERVER_SETTINGS.defaultAutoPull,
      // Non-allowlisted values, even changed, never enter the snapshot.
      worktreesDirectory: "/host/only/path",
      cloudSpeechCredentials: { ...DEFAULT_SERVER_SETTINGS.cloudSpeechCredentials, openai: "key" },
      sourceControlHosts: { "github.com": {} },
      defaultTheme: "dark",
    } as unknown as ServerSettings;
    const fields = followerHostSettingsFields(host);
    expect(Object.keys(fields).sort()).toEqual(["defaultModelSelection", "defaultRuntimeMode"]);
    expect(fields.defaultModelSelection).toEqual(MODEL_SELECTION);
    // A snapshot with nothing non-default is empty, never a reset instruction
    // disguised as content.
    const unchanged = followerHostSettingsFields(DEFAULT_SERVER_SETTINGS);
    expect(unchanged).toEqual({});
  });

  it("applies an approved snapshot and never writes outside the allowlist", () => {
    const decision = followerSettingsInheritancePatch(
      snapshot(7, { defaultAutoPull: true, defaultModelSelection: MODEL_SELECTION }),
      { revision: 0, fields: {} },
    );
    expect(decision._tag).toBe("apply");
    if (decision._tag !== "apply") return;
    const patchKeys = Object.keys(decision.patch);
    for (const key of patchKeys)
      expect((FOLLOWER_INHERITED_SETTINGS_FIELDS as readonly string[]).includes(key)).toBe(true);
    expect(decision.patch.defaultAutoPull).toBe(true);
    expect(decision.patch.defaultModelSelection).toMatchObject(MODEL_SELECTION);
  });

  it("resets inherited keys to defaults when the host reverts them", () => {
    const decision = followerSettingsInheritancePatch(
      // The host no longer approves anything.
      snapshot(9, {}),
      { revision: 8, fields: { defaultAutoPull: true } },
    );
    expect(decision._tag).toBe("apply");
    if (decision._tag !== "apply") return;
    expect(decision.patch.defaultAutoPull).toBe(DEFAULT_SERVER_SETTINGS.defaultAutoPull);
    expect(decision.patch.defaultModelSelection).toBe(
      DEFAULT_SERVER_SETTINGS.defaultModelSelection,
    );
    // Sandbox-local fields are untouched: the patch never mentions them.
    expect("worktreesDirectory" in decision.patch).toBe(false);
    expect("speechProjectCustomWords" in decision.patch).toBe(false);
  });

  it("refuses out-of-order updates", () => {
    const decision = followerSettingsInheritancePatch(snapshot(3, { defaultAutoPull: true }), {
      revision: 8,
      fields: { defaultAutoPull: true },
    });
    expect(decision).toEqual({ _tag: "refuse", reason: "out-of-order" });
  });

  it("skips an identical revision and replays drifted content at the same revision", () => {
    const same = followerSettingsInheritancePatch(snapshot(8, { defaultAutoPull: true }), {
      revision: 8,
      fields: { defaultAutoPull: true },
    });
    expect(same).toEqual({ _tag: "skip" });
    // Same revision, different content replays instead of wedging the follower.
    const replayed = followerSettingsInheritancePatch(snapshot(8, { defaultAutoPull: false }), {
      revision: 8,
      fields: { defaultAutoPull: true },
    });
    expect(replayed._tag).toBe("apply");
    if (replayed._tag !== "apply") return;
    expect(replayed.patch.defaultAutoPull).toBe(false);
  });

  it("refuses a snapshot carrying unclassified fields", () => {
    for (const field of [
      "cloudSpeechCredentials",
      "worktreesDirectory",
      "unknownLater",
      "providerInstances",
    ]) {
      const decision = followerSettingsInheritancePatch(snapshot(9, { [field]: "value" }), {
        revision: 8,
        fields: {},
      });
      expect(decision, field).toEqual({ _tag: "refuse", reason: "unclassified-field" });
    }
  });

  it("refuses a snapshot whose approved value fails its own schema", () => {
    const decision = followerSettingsInheritancePatch(
      snapshot(9, { defaultRuntimeMode: "not-a-mode" }),
      { revision: 0, fields: {} },
    );
    expect(decision).toEqual({ _tag: "refuse", reason: "invalid-field" });
  });
});
