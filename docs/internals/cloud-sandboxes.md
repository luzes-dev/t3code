# Managed cloud sandboxes

Managed cloud sandboxes are provider VMs controlled by one environment (the
controller). This note records the ownership model, how persistent
(user-managed) sandboxes differ from followers, what persists across restarts,
and the compatibility rules for record payloads.

## The model

Three independent properties, never inferred from each other:

1. **Compute resource.** The provider VM: `resourceId`, driver kind, profile,
   lifecycle (`allocating`/`active`/`deleting`/`deleted`/`failed`), power,
   intent, spend. Controlled only by `CloudSandboxManager` on its controller.
2. **Owner.** Who may bind, control, and release the resource. Explicit in
   `ManagedCloudSandboxOwner`: `user`, `thread`
   (`hostEnvironmentId` + `threadId`), or `task`
   (`hostEnvironmentId` + `sourceThreadId` + `taskId`). Ownership is never
   derived from power, intent, connection route, or peer-link presence.
3. **Retention.** What happens when the owner finishes: `manual`,
   `stop-and-retain`, or `delete-on-completion`.

Presentation (`selectable` environment vs `follower`) is derived from the
owner: only `user`-owned resources are selectable. Followers keep their own
`environmentId`, `t3 serve`, proxy route, and peer link — registration and
visibility differ, the identity does not.

## Degraded-follower reporting

A follower that cannot do its job (its controller-side bind failed, its
provider token was rejected, the VM or peer link is unreachable, or
allocation failed) carries an optional `issue` on its
`ManagedCloudSandboxResource`: `cause`
(`provider-auth`/`connectivity`/`bind`/`allocation`), the responsible
`driverKind`/`profileId` when known, a `message`, and `firstSeenAt`.
Presence is state, not an event: the field is overwritten on a new failure
class and cleared as soon as a successful connect or rebind replaces it, so
nothing stale accumulates anywhere.

Reporting piggybacks on existing surfaces. The controller's 30-second issue
ticker dispatches `delegated_task.remote.issue` for follower resources whose
owning task is still running; the orchestrator projects it into
`remoteChild.sandboxIssue` on the parent thread through the usual
`subagent.updated` event, so task cards render a degraded state and its
clearance with no new transport, no synthetic turn items, and no waking of the
delegated child. Settings collapse per-controller issues by
`driverKind:profileId:cause` into one "Follower issues" banner per dead cloud
profile (one fix: re-login on that profile), while per-resource rows keep the
individual detail.

The follow loop itself is bounded: an unreachable linked environment is
retried with back-off for 10 minutes (or 40 attempts, so stubbed schedules
exhaust the same way); past that its task completes failed here with the
unreachable wording instead of following forever. Until then the card shows
the durably stamped issue.

## Behavior matrix

| Behavior                                             | Persistent (`user`)                  | Follower (`thread`/`task`)                                                                                                                                                                                                                                                                                                         |
| ---------------------------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Environment picker, Settings scope, provider notices | Normal environment                   | Hidden; status/cost surface through the owning chat/task and the controller's managed-resource administration. A follower's degrade/issue state surfaces on its owning task card and as a collapsed banner in the controller's Settings; followers contribute no global provider warnings                                          |
| Internal routing (proxy, SSH, `t3 serve`)            | Normal                               | Identical; `manager.resolve`/`CloudSandboxDirectory` still locate the environment by id                                                                                                                                                                                                                                            |
| Creation                                             | User provisioning                    | `forkFromController`/`delegateSandboxTask`; bound to the owner after the delegated child exists                                                                                                                                                                                                                                    |
| Owner binding                                        | `user` from creation                 | User-initiated forks inherit the source owner; child-initiated forks start unbound and bind once the task id exists                                                                                                                                                                                                                |
| Release                                              | Manual delete from the UI            | Owner thread's journal (`remove`) or source child. Task followers bound to `delete-on-completion` (opt-in on `delegate_task`) also release when their delegated task settles (terminal `subagent.updated` status) and the VM has stopped; the orphan sweep reclaims the rest when the source thread is gone and the VM has stopped |
| Persistence                                          | Survives restarts, always selectable | A stopped/recovered follower stays a follower; retention never promotes it                                                                                                                                                                                                                                                         |

## Storage and compatibility

Records persist whole as JSON payloads (`managed_cloud_sandboxes.payload_json`,
migration `081_CloudSandboxes`); `owner`, `retention`, and `issue` ride in that
payload. No new migration was required, and none should reuse `081`.

- Old payloads without `owner`/`retention` decode as `user`/`manual`
  (`withDecodingDefault` in the contract), so upgraded records remain visible
  and selectable in Settings. Payloads without `issue` decode with no issue.
- Delegated workspace journals remain the recovery source for child state;
  owner binding after delegation is a no-fail side effect (a failed bind keeps
  the resource user-owned and sweep-recoverable rather than guessing).

Rollback: decoding ignores the new fields when absent, so records written by
this build are read by older builds as ownerless user resources; binds are lost
on rollback, never records.

## Ownership authorization

- `manager.bindOwner` derives the host id server-side: the controller's own
  environment id on the route path, the authenticating source record's
  environment id on the source path. Callers cannot assert host ids.
- Rebinding to a _different_ owner is refused (`ownership` error); retrying
  with the same owner is idempotent.
- Cross-controller lookups fail structurally: records exist only on the
  controller that created them, and source-route children may act only on
  their own `parentResourceId` descendants.
- Client visibility is not authorization: follower routes require the same
  authenticated sessions as before; hiding followers from pickers changes
  presentation only.

## Live verification (2026-11-12/13)

The degraded-reporting life cycle was exercised end to end against a scratch
controller and a real Boat.dev account (small VMs, 30-minute auto-stop):

- **Task-owned bind** — a forked VM bound to a task owner carries the owner on
  its record. A refused rebind to a different task (HTTP 403) stays user-visible
  as `issue {cause: "bind"}` while the sandbox keeps running and stays owned by
  its original task.
- **Clear on recovery** — disconnecting and reconnecting that VM with valid
  credentials erased the issue on the next successful connect.
- **Dead token** — provisioning with a corrupted profile key produced a
  `failed` record with `issue {cause: "provider-auth"}` and `sandboxId: null`:
  no VM allocated, no billing, one visible diagnostic instead of a silent hang.
- **Lost connectivity** — killing the VM's reverse-proxy tunnel and blocking the
  controller's SSH identity stamped `issue {cause: "connectivity"}` on a still
  running, provider-healthy VM; restoring access reconnected within one
  reconciliation tick and cleared the issue.
- **Render coverage** — the Settings roll-up banner (cause label, plural
  affected count, per-row issue lines), the relationships row's degraded pill
  and tooltip, and the mobile task-row issue suffix are covered by web browser
  and react tests (`910654cf17`).

Operational note from the run: follower deletes hold the record lock for
60-70 seconds (reverse-proxy teardown), so scripted delete calls need generous
timeouts.

## Verified against drivers (not CI)

Paid Boat.dev provisioning, real SSH transport, and the degradation life cycle
above ran live and are repeatable only by hand; the unit suites still cover
allocation, binding, decode compatibility, and sweep behavior against driver
fixtures. Desktop rendering of an owned workspace and live reconciliation
against a second provider remain unexercised, as does physical-device mobile
behavior (the management component has automated coverage).
