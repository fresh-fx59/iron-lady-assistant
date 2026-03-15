# Gmail Gateway Migration Execution Status

Last updated: 2026-03-15

## Completed Slices

1. Phase 0 governance artifacts created:
   - `docs/gmail-gateway/phase-0-scope-matrix.md`
   - `docs/gmail-gateway/policy-gate-checklist.md`
2. Phase 1 contract created:
   - `specs/gmail-gateway/openapi.v1.yaml`
3. Phase 1 canonical schema created:
   - `specs/gmail-gateway/schema.v1.sql`
4. Phase 1/2 executable foundations:
   - `src/gmail_gateway/schema.py`
   - `src/gmail_gateway/models.py`
   - `src/gmail_gateway/auth_store.py`
5. Phase 2/3 service implementation (partial):
   - `src/gmail_gateway/http.py`
   - `src/gmail_gateway/message_store.py`
   - Implemented endpoints: `/health`, `/v1/accounts/{account_id}`, `/v1/messages/send`, `/v1/messages/search`, `/v1/messages/{message_id}`, `/v1/messages/{message_id}/trash`, `DELETE /v1/messages/{message_id}`
   - Idempotency semantics for `send` with conflict detection on payload mismatch.
   - Sync endpoints now implemented in service (`bootstrap`, `delta`, `watch renew`) with cursor persistence.
6. Phase 2 auth core (incremental):
   - Implemented connect session persistence and callback completion in `AuthStore`.
   - Implemented HTTP endpoints: `/v1/accounts/{account_id}/connect`, `/v1/oauth/callback`, `/v1/accounts/{account_id}/disconnect`.
7. Assistant-side gateway client abstraction:
   - Added `src/gmail_gateway_client.py` async client with typed error handling.
   - Added gateway runtime flags in `src/config.py`:
     - `GMAIL_GATEWAY_BASE_URL`
     - `GMAIL_GATEWAY_TIMEOUT_SECONDS`
   - Gateway is now the only supported runtime path.
8. Gmail tool routing replacement:
   - Added `src/gmail_gateway_cli.py` for gateway operations from CLI/tooling.
   - Updated `tools/gmail.yaml` to use gateway-only commands (legacy direct `gog gmail` path removed from primary tool instructions).
9. Step 1 production adapter move:
   - Added `src/gmail_gateway/gmail_api.py` with Gmail API send integration.
   - Wired `/v1/messages/send` to use account token + Gmail API call instead of queue-only fake send.
   - Added typed error mapping (`invalid_grant` -> `reauth_required`, `429` -> `quota_limited`).
10. Step 2 production adapter move:
   - Wired `/v1/messages/search`, `/v1/messages/{message_id}`, `/v1/messages/{message_id}/trash`, `DELETE /v1/messages/{message_id}` to Gmail API client.
   - Removed store-backed behavior from those endpoints and kept typed Gmail error mapping.
11. Step 3 observability slice:
   - Added middleware-based request counters and structured request logs in gateway HTTP service.
   - Added `GET /internal/metrics` for local counter inspection.
12. Step 4 OAuth refresh slice:
   - Added access-token refresh exchange in Gmail API client (`oauth2.googleapis.com/token`).
   - Added automatic single retry on 401 with refresh-token rotation in gateway handlers.
   - Persisted rotated access token in auth store.

## Validations Run

- `pytest -q tests/gmail_gateway` -> 18 passed
- `python -m compileall -q src/gmail_gateway` -> success

## Next Slice (auto-continue target)

1. Wire `GmailGatewayClient` into assistant runtime as the only Gmail path (no legacy fallback).
2. Add persistent metrics export path (Prometheus-style or equivalent) beyond in-process counters.
3. Add integration tests that hit real Gmail test tenant (stage ladder prerequisite).
