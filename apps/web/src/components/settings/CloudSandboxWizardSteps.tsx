import { SandboxMachineType } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { CheckIcon } from "lucide-react";
import { Radio as RadioPrimitive } from "@base-ui/react/radio";
import { RadioGroup as RadioGroupPrimitive } from "@base-ui/react/radio-group";
import { Input } from "../ui/input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { SecretInput } from "./SecretInput";
import { SettingsGroup } from "./SettingsGroup";
import { SettingsRow } from "./settingsLayout";
import type { CloudSandboxDraft } from "./AddEnvironmentDialog.logic";

const isSandboxMachineType = Schema.is(SandboxMachineType);

const AUTO_STOP_OPTIONS = [
  { value: 15, label: "15 minutes" },
  { value: 30, label: "30 minutes" },
  { value: 60, label: "1 hour" },
  { value: 120, label: "2 hours" },
  { value: 240, label: "4 hours" },
  { value: 480, label: "8 hours" },
] as const;

// boat.dev's machines; rates mirror the boat driver's published pricing.
const BOAT_SIZES = [
  { value: "small", label: "Small", specs: "2 vCPU · 4 GB RAM", hourlyUsd: 0.018 },
  { value: "default", label: "Standard", specs: "4 vCPU · 8 GB RAM", hourlyUsd: 0.036 },
  { value: "large", label: "Large", specs: "8 vCPU · 16 GB RAM", hourlyUsd: 0.072 },
  { value: "xlarge", label: "Extra Large", specs: "16 vCPU · 32 GB RAM", hourlyUsd: 0.144 },
] as const satisfies ReadonlyArray<{
  value: SandboxMachineType;
  label: string;
  specs: string;
  hourlyUsd: number;
}>;

const formatHourlyUsd = (hourlyUsd: number) => `$${hourlyUsd.toFixed(3)}/h`;

/**
 * Step 2 of the sandbox wizard: the API key for the provider picked on the
 * type step. The key lives in the server-side profile for that provider;
 * typing a new one updates it, and an already stored key can be reused.
 */
export function CloudSandboxConfiguration({
  draft,
  disabled,
  storedKeyAvailable,
  onChange,
}: {
  readonly draft: CloudSandboxDraft;
  readonly disabled: boolean;
  readonly storedKeyAvailable: boolean;
  readonly onChange: (draft: CloudSandboxDraft) => void;
}) {
  return (
    <SettingsGroup variant="plain">
      <SettingsRow
        title={<label htmlFor="boat-profile-key">API key</label>}
        description={
          storedKeyAvailable
            ? "A key is already stored on this server. Leave this blank to keep it."
            : "The key stays on this server. Agents never see it."
        }
        control={
          <SecretInput
            id="boat-profile-key"
            aria-label="API key"
            size="sm"
            configured={storedKeyAvailable}
            disabled={disabled}
            value={draft.apiKey}
            placeholder="boat_k3Xz9f…"
            onChange={(event) => onChange({ ...draft, apiKey: event.target.value })}
          />
        }
      />
    </SettingsGroup>
  );
}

/**
 * Step 3 of the sandbox wizard: what every sandbox created by this server
 * starts with. Allocation itself is up to the application and its agents.
 */
export function CloudSandboxDefaults({
  draft,
  disabled,
  onChange,
}: {
  readonly draft: CloudSandboxDraft;
  readonly disabled: boolean;
  readonly onChange: (draft: CloudSandboxDraft) => void;
}) {
  const autoStopLabel =
    AUTO_STOP_OPTIONS.find((option) => option.value === draft.autoStopMinutes)?.label ?? "";
  return (
    <SettingsGroup variant="plain">
      {/* Same inset and heading style as the settings row below, so both titles align. */}
      <div className="rounded-xl px-3 py-3 sm:px-4">
        <div id="boat-size-label" className="min-h-5 text-sm font-medium text-foreground">
          Machine size
        </div>
        <p className="mt-1 max-w-2xl text-balance text-xs leading-normal text-muted-foreground">
          Compute resources allocated to your sandbox.
        </p>
        <div className="pt-3">
          <RadioGroupPrimitive
            aria-labelledby="boat-size-label"
            disabled={disabled}
            value={draft.defaultSize}
            onValueChange={(size) => {
              if (isSandboxMachineType(size)) {
                onChange({ ...draft, defaultSize: size });
              }
            }}
            className="grid grid-cols-2 gap-2 lg:grid-cols-4"
          >
            {BOAT_SIZES.map((size) => (
              <RadioPrimitive.Root
                key={size.value}
                value={size.value}
                aria-label={size.label}
                className="relative flex cursor-pointer flex-col gap-1 rounded-lg bg-card p-3 text-left text-muted-foreground outline-none ring-1 ring-black/5 focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring data-checked:bg-primary/8 data-checked:ring-2 data-checked:ring-inset data-checked:ring-primary dark:ring-white/5"
              >
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-foreground">{size.label}</span>
                  <RadioPrimitive.Indicator className="grid size-4 place-items-center rounded-full bg-primary text-primary-foreground">
                    <CheckIcon className="size-3" />
                  </RadioPrimitive.Indicator>
                </div>
                <span className="text-xs">{size.specs}</span>
                <span className="text-xs">{formatHourlyUsd(size.hourlyUsd)}</span>
              </RadioPrimitive.Root>
            ))}
          </RadioGroupPrimitive>
        </div>
      </div>
      <SettingsRow
        title={<label htmlFor="boat-sandbox-autostop">Auto-stop window</label>}
        description="Idle sandboxes stop after this long. Agent activity resets it."
        control={
          <Select
            disabled={disabled}
            value={draft.autoStopMinutes}
            onValueChange={(minutes) => onChange({ ...draft, autoStopMinutes: Number(minutes) })}
          >
            <SelectTrigger size="sm" id="boat-sandbox-autostop" aria-label="Auto-stop window">
              <SelectValue>{autoStopLabel}</SelectValue>
            </SelectTrigger>
            <SelectPopup align="end" alignItemWithTrigger={false}>
              {AUTO_STOP_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectPopup>
          </Select>
        }
      />
      <SettingsRow
        title={<label htmlFor="boat-monthly-budget">Monthly budget</label>}
        description="New sandboxes are refused once running costs reach this. Leave blank for no limit."
        control={
          <Input
            id="boat-monthly-budget"
            size="sm"
            value={draft.monthlyBudgetUsd}
            onChange={(event) => onChange({ ...draft, monthlyBudgetUsd: event.target.value })}
            placeholder="No limit"
            inputMode="decimal"
            disabled={disabled}
            spellCheck={false}
          />
        }
      />
      <SettingsRow
        title={<label htmlFor="boat-max-concurrent">Concurrent sandboxes</label>}
        description="The most this server runs at once."
        control={
          <Input
            id="boat-max-concurrent"
            size="sm"
            type="number"
            min={1}
            max={100}
            step={1}
            value={Number.isNaN(draft.maxConcurrentSandboxes) ? "" : draft.maxConcurrentSandboxes}
            onChange={(event) =>
              onChange({ ...draft, maxConcurrentSandboxes: event.target.valueAsNumber })
            }
            disabled={disabled}
          />
        }
      />
      <SettingsRow
        title={<label htmlFor="boat-template">Template snapshot</label>}
        description="A saved snapshot to boot from, for example one with extra tools or provider CLIs. Leave blank to start from a clean machine."
        control={
          <Input
            id="boat-template"
            size="sm"
            value={draft.template}
            onChange={(event) => onChange({ ...draft, template: event.target.value })}
            placeholder="None"
            disabled={disabled}
            spellCheck={false}
          />
        }
      />
    </SettingsGroup>
  );
}
