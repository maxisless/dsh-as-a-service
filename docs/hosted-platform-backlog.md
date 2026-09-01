# Hosted Platform Backlog

[中文版](hosted-platform-backlog.zh-CN.md) · [Target architecture](tenant-session-runtime-design.md)

> This is the implementation backlog from the current trusted, single-node
> deployment to a hosted multi-tenant platform. It does not claim that any
> unchecked item is already available.

## Current foundation

- The control plane persists tenant, principal, Agent version, session, Run,
  lease, input manifest, action, artifact, delivery, usage, and audit metadata.
- Feishu conversations resolve to server-issued sessions. Attachments enter
  `inbox/<run-id>/` and are hash-verified before a Run becomes executable.
- Text delivery has a stable replay ID. Failed media delivery remains `RETRY`
  and never causes a new media generation.
- Private runners inherit only their declared environment capabilities and the
  active session workspace.

The foundation still uses single-node SQLite, one Worker container, and
server-managed provider credentials. It is not the final isolation guarantee.

## P0 — before broad real-user rollout

### [ ] Complete media delivery replay

Persist each delivery stage: artifact download, Feishu upload, card/message
update, and confirmation. Every stage needs a stable idempotency key.

**Done when:** a crash after Feishu accepts an upload never duplicates media,
never regenerates it, and eventually completes user-visible delivery.

### [ ] Unify provider jobs with Run actions

Move asynchronous provider job state into the control-plane action model:
provider, external task ID, poll cursor, cancelability, retries, artifacts, and
delivery links.

**Done when:** recovery always resumes the existing provider task rather than
submitting a second billable task.

### [ ] Production identity, membership, and revocation

Replace development bearer mapping with production identity, tenant membership,
roles, session authorization, disablement, and revocation.

**Done when:** a revoked user or changed group membership cannot access an old
session, Run, artifact, or create new work.

### [ ] Tenant policy, quotas, and cost reservations

Publish immutable tenant policy versions for models, Skills, concurrency, rate,
Token, storage, media, and cost. Reserve estimated cost before dispatch and
reconcile actual cost afterwards.

**Done when:** one tenant cannot exhaust shared capacity or exceed its budget.

## P1 — hosted multi-tenant execution

### [ ] PostgreSQL, durable queue, and object storage

Replace SQLite/process-local scheduling/Docker volumes with shared systems of
record while preserving fencing and idempotency.

### [ ] Session-isolated executor pool

Run active sessions in short-lived containers or sandboxes that mount only the
session workspace, DSH state, and approved read-only Skill bundle.

### [ ] Vault and Model/Tool Gateway

Keep raw credentials in Vault. Issue short-lived scoped capabilities through
model/tool gateways; support rotation, revocation, audit, platform credentials,
and BYOK.

### [ ] Network egress and Tool Policy Enforcement

Apply destination allowlists, SSRF/DNS-rebinding protection, request limits,
risk classification, approval, and audit at dispatch and network boundaries.

### [ ] Runtime lifecycle and capacity pool

Add executor/runtime leases, cold-start rules, idle TTL, LRU eviction, drain,
and maximum process limits. Share a per-model runtime only after upstream DSH
is proven safe for session-scoped workspace/state.

## P2 — shared knowledge and operations

### [ ] Tenant memory and knowledge publication

Build `source → index → review → publish`; Runs consume only authorized
retrieval results. Promotion from session/Run output is explicit and audited.

### [ ] Retention, deletion, backup, and disaster recovery

Define lifecycle for workspace, state, inputs, artifacts, indices, logs,
queues, and backups. Include purge propagation, legal hold, RPO/RTO, and
recovery drills.

### [ ] Observability, alerting, and SLOs

Measure queue wait, latency, failure, delivery retry, provider errors, cost,
cancel rate, and executor saturation by tenant, Agent, model, session, and Run.

### [ ] Agent and Skill release governance

Freeze Agent prompt, Skill manifest/hash, tool policy, runner image, and model
route in a release package. Require security checks, evaluation, canary, and
rollback before publishing.

## Recommended order

1. Complete media delivery replay and provider-job unification.
2. Add production identity, tenant policy, cost reservation, and quotas.
3. Move to PostgreSQL, durable queue, and object storage before multi-Worker.
4. Add isolated executors, Vault, and network policy enforcement.
5. Finish knowledge publication, data governance, observability, and release
   governance.

Before starting an item, update the architecture document's current-state
section and add single-node, recovery, and authorization acceptance tests.
