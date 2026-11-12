/**
 * Follower settings inheritance: the closed allowlist of `ServerSettings`
 * fields a managed cloud sandbox follower may take from its controller host.
 * Unclassified fields, secrets, host paths, provider instances, source-control
 * credentials, and identity configuration stay host-only by rule.
 */
import {
  DEFAULT_SERVER_SETTINGS,
  NonNegativeInt,
  ServerSettings,
  ServerSettingsPatch,
  type FollowerHostSettingsSnapshot,
  type ServerSettingsPatch as ServerSettingsPatchValue,
} from "@t3tools/contracts";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

export const FOLLOWER_INHERITED_SETTINGS_FIELDS = [
  "defaultModelSelection",
  "fallbackModelSelection",
  "defaultRuntimeMode",
  "responseStreamingMode",
  "defaultAutoPull",
] as const;

export type FollowerInheritedSettingsField = (typeof FOLLOWER_INHERITED_SETTINGS_FIELDS)[number];

export type FollowerSettingsFieldClass = "inherit" | "host-only";

const inheritedFields: ReadonlySet<string> = new Set(FOLLOWER_INHERITED_SETTINGS_FIELDS);

export const followerSettingsFieldClass = (field: string): FollowerSettingsFieldClass =>
  inheritedFields.has(field) ? "inherit" : "host-only";

/** Patch validator for exactly the inherited keys; unknown keys fail closed. */
export const FollowerInheritedSettingsPatch = Schema.Struct({
  defaultModelSelection: ServerSettingsPatch.fields.defaultModelSelection,
  fallbackModelSelection: ServerSettingsPatch.fields.fallbackModelSelection,
  defaultRuntimeMode: ServerSettingsPatch.fields.defaultRuntimeMode,
  responseStreamingMode: ServerSettingsPatch.fields.responseStreamingMode,
  defaultAutoPull: ServerSettingsPatch.fields.defaultAutoPull,
});

/** What a follower persists about the last host snapshot it applied. */
export const LastAppliedFollowerSettings = Schema.Struct({
  revision: NonNegativeInt,
  fields: Schema.Record(Schema.String, Schema.Json),
});
export type LastAppliedFollowerSettings = typeof LastAppliedFollowerSettings.Type;

const lastAppliedEquivalence = Schema.toEquivalence(LastAppliedFollowerSettings);
const fieldsEquivalence = Schema.toEquivalence(Schema.Record(Schema.String, Schema.Json));
const decodePatchExit = Schema.decodeUnknownExit(FollowerInheritedSettingsPatch, {
  onExcessProperty: "error",
});

/** Whether two batched field sets carry identical values; revisions compare separately. */
export const followerSettingsFieldsEqual = (
  a: Record<string, unknown>,
  b: Record<string, unknown>,
) =>
  fieldsEquivalence(
    a as FollowerHostSettingsSnapshot["fields"],
    b as FollowerHostSettingsSnapshot["fields"],
  );

/**
 * Approved follower inheritance for one host's settings: allowlisted fields
 * whose values differ from the defaults. Everything else stays absent, so a
 * follower seeds only explicit host deltas and never defaults-by-omission.
 */
export const followerHostSettingsFields = (
  settings: ServerSettings,
): FollowerHostSettingsSnapshot["fields"] => {
  const fields: Record<string, unknown> = {};
  for (const field of FOLLOWER_INHERITED_SETTINGS_FIELDS) {
    if (Equal.equals(settings[field], DEFAULT_SERVER_SETTINGS[field])) continue;
    try {
      // Model selections and mode strings are plain JSON; a future allowlisted
      // field that is not JSON drops out and stays host-only.
      fields[field] = JSON.parse(JSON.stringify(settings[field]));
    } catch {
      continue;
    }
  }
  return fields as FollowerHostSettingsSnapshot["fields"];
};

export type FollowerSettingsInheritanceDecision =
  | { _tag: "apply"; revision: number; patch: ServerSettingsPatchValue }
  | { _tag: "refuse"; reason: "out-of-order" | "unclassified-field" | "invalid-field" }
  | { _tag: "skip" };

/**
 * The patch one approved snapshot becomes, given the last applied one. A replay
 * covers the whole allowlist — host value or schema default — so sandbox-local
 * fields are never written; unclassified fields refuse the whole snapshot.
 */
export const followerSettingsInheritancePatch = (
  snapshot: FollowerHostSettingsSnapshot,
  lastApplied: LastAppliedFollowerSettings,
): FollowerSettingsInheritanceDecision => {
  if (snapshot.revision < lastApplied.revision) return { _tag: "refuse", reason: "out-of-order" };
  if (
    snapshot.revision === lastApplied.revision &&
    lastAppliedEquivalence({ revision: snapshot.revision, fields: snapshot.fields }, lastApplied)
  )
    return { _tag: "skip" };
  for (const field of Object.keys(snapshot.fields))
    if (followerSettingsFieldClass(field) === "host-only")
      return { _tag: "refuse", reason: "unclassified-field" };
  const composed: Record<string, unknown> = {};
  for (const field of FOLLOWER_INHERITED_SETTINGS_FIELDS)
    composed[field] =
      field in snapshot.fields
        ? snapshot.fields[field]
        : DEFAULT_SERVER_SETTINGS[field as keyof typeof DEFAULT_SERVER_SETTINGS];
  const decoded = decodePatchExit(composed);
  if (!Exit.isSuccess(decoded)) return { _tag: "refuse", reason: "invalid-field" };
  return { _tag: "apply", revision: snapshot.revision, patch: decoded.value };
};
