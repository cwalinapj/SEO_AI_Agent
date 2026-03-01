# Web Secrets Wallet Human-Verified Task Marketplace Contract

This contract defines the backend API for creating, claiming, verifying, and authorizing payouts for human-verified account-creation tasks.

## Security and signatures

- Task spec outputs are signed with `TASK_AGENT_SIGNING_SECRET` (falls back to `WP_PLUGIN_SHARED_SECRET` if unset).
- Verification and payout authorization payloads are also signed.
- Signature algorithm: `HMAC_SHA256_HEX(secret, payload)`.

## Idempotency

Mutating endpoints require `Idempotency-Key` header (or `idempotency_key` in body).

- If key + endpoint + request payload match a prior request, the server replays the previous response.
- If key is reused with different payload, server returns `409 idempotency_key_conflict`.

## Audit trail

All lifecycle mutations write immutable task audit events in `task_audit_log` with:

- `event_type`
- `actor_wallet`
- canonical `payload_json`
- `payload_hash`

## Endpoints

## Task specs

- `POST /v1/tasks`
  - Creates task template/spec.
  - Returns `taskSpecHash` and `agentSignature`.
- `GET /v1/tasks/open`
  - Lists open (unclaimed, non-expired) tasks.
- `GET /v1/tasks/{task_id}`
  - Fetches full spec and current lifecycle state.

## Claim lifecycle

- `POST /v1/tasks/{task_id}/claim`
  - Locks task to `worker_wallet`.
  - Enforces one active claim per task.
- `POST /v1/tasks/{task_id}/release`
  - Manual release by claim owner or timeout release.

## Evidence ingestion

- `POST /v1/tasks/{task_id}/evidence`
  - Accepts URLs, structured fields, screenshots, optional trace ref.
  - Returns immutable `evidence_hash`.

## Deterministic verification

- `POST /v1/tasks/{task_id}/verify`
  - Evaluates evidence against task spec rules.
  - Returns machine-readable diffs and `passed` boolean.
  - Returns `verificationResultHash` and server signature.

## Payout authorization

- `POST /v1/tasks/{task_id}/payout-authorize`
  - Succeeds only when latest verification passed and claim owner matches wallet.
  - Returns signed payout authorization payload for on-chain execution.

## JSON Schemas

- [TaskSpec](/Users/root1/loc-count/_repos/SEO_AI_Agent/docs/schemas/task-spec.schema.json)
- [EvidenceBundle](/Users/root1/loc-count/_repos/SEO_AI_Agent/docs/schemas/evidence-bundle.schema.json)
- [VerificationResult](/Users/root1/loc-count/_repos/SEO_AI_Agent/docs/schemas/verification-result.schema.json)
- [PayoutAuthorization](/Users/root1/loc-count/_repos/SEO_AI_Agent/docs/schemas/payout-authorization.schema.json)

## OpenAPI

- [openapi.yaml](/Users/root1/loc-count/_repos/SEO_AI_Agent/docs/openapi.yaml)
