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

## Behavior matrix

| Behavior                                             | Persistent (`user`)                  | Follower (`thread`/`task`)                                                                                                                                                                                                                                                                                                         |
| ---------------------------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Environment picker, Settings scope, provider notices | Normal environment                   | Hidden; status/cost surface through the owning chat/task and the controller's managed-resource administration                                                                                                                                                                                                                      |
| Internal routing (proxy, SSH, `t3 serve`)            | Normal                               | Identical; `manager.resolve`/`CloudSandboxDirectory` still locate the environment by id                                                                                                                                                                                                                                            |
| Creation                                             | User provisioning                    | `forkFromController`/`delegateSandboxTask`; bound to the owner after the delegated child exists                                                                                                                                                                                                                                    |
| Owner binding                                        | `user` from creation                 | User-initiated forks inherit the source owner; child-initiated forks start unbound and bind once the task id exists                                                                                                                                                                                                                |
| Release                                              | Manual delete from the UI            | Owner thread's journal (`remove`) or source child. Task followers bound to `delete-on-completion` (opt-in on `delegate_task`) also release when their delegated task settles (terminal `subagent.updated` status) and the VM has stopped; the orphan sweep reclaims the rest when the source thread is gone and the VM has stopped |
| Persistence                                          | Survives restarts, always selectable | A stopped/recovered follower stays a follower; retention never promotes it                                                                                                                                                                                                                                                         |

## Storage and compatibility

Records persist whole as JSON payloads (`managed_cloud_sandboxes.payload_json`,
migration `081_CloudSandboxes`); `owner` and `retention` ride in that payload.
No new migration was required, and none should reuse `081`.

- Old payloads without `owner`/`retention` decode as `user`/`manual`
  (`withDecodingDefault` in the contract), so upgraded records remain visible
  and selectable in Settings.
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

## Unverified in CI

Paid Boat.dev provisioning, real SSH transport, desktop/mobile rendering of
an owned workspace, and live reconciliation against a second provider are
exercised neither locally nor in CI here; the unit suites cover allocation,
binding, decode compatibility, and sweep behavior against driver fixtures.
