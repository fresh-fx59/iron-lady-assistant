# Gmail Gateway Policy Gate Checklist

Status: Open

## Required Before Phase 2

1. Confirm exact end-user features enabled in production v1.
2. Confirm minimal Gmail OAuth scopes and mark optional scopes.
3. Classify scope tier (non-sensitive / sensitive / restricted) with evidence.
4. Define consent-screen mode for each environment (internal vs external).
5. Confirm legal documents are published and versioned:
   - Privacy policy
   - Terms of service
   - Data retention/deletion policy
6. Confirm incident owner and escalation contacts.
7. Document verification lead-time assumptions and critical path.

## Approval Blockers

- Any requested feature requiring additional scopes not in the scope matrix.
- Missing security controls for refresh token storage.
- No rollback owner for migration Phase 6.

## Exit Criteria Mapping (Phase 0)

- Signed scope matrix: `docs/gmail-gateway/phase-0-scope-matrix.md`
- Verification risk log includes owner + date for each item.
- Formal go/no-go decision recorded in this file.

## Decision Log

- 2026-03-15: Gate initialized. Awaiting owner sign-off.
