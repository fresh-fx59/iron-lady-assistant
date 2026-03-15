# Gmail Gateway Phase 0 Scope Matrix

Status: Draft (execution started 2026-03-15)

## Feature to Scope Mapping (v1)

| Feature | Gmail API Surface | Minimal Scope | Notes |
|---|---|---|---|
| Send email | `users.messages.send` | `https://www.googleapis.com/auth/gmail.send` | Required for outbound assistant actions. |
| Search messages metadata | `users.messages.list` | `https://www.googleapis.com/auth/gmail.readonly` | Use query-filtered reads only. |
| Read message content | `users.messages.get` | `https://www.googleapis.com/auth/gmail.readonly` | Needed for summarization and reply context. |
| Trash message | `users.messages.trash` | `https://www.googleapis.com/auth/gmail.modify` | Required for soft-delete behavior. |
| Permanent delete (optional in v1) | `users.messages.delete` | `https://www.googleapis.com/auth/gmail.modify` | Keep disabled by default for safer rollout. |
| Watch mailbox | `users.watch` | `https://www.googleapis.com/auth/gmail.readonly` | Required for delta sync trigger path. |
| History delta sync | `users.history.list` | `https://www.googleapis.com/auth/gmail.readonly` | Required for near-real-time convergence. |

## Product Scope (v1)

In scope:
- Account-level OAuth connect/disconnect.
- Send, search, read, trash.
- Watch + history delta sync.
- Idempotency and delivery receipt tracking.

Out of scope:
- Thread mutation APIs beyond read/search.
- Label creation/management.
- Bulk destructive operations.
- Multi-provider mailbox abstraction.

## Verification Risk Register

| Risk | Severity | Owner | Mitigation | Target Date |
|---|---|---|---|---|
| Sensitive/restricted scope verification lead time | High | OAuth/Security owner | Keep v1 scopes minimal; avoid restricted scopes in v1; pre-submit verification artifacts. | 2026-03-18 |
| Consent-screen rejection due to missing policy docs | Medium | Gateway API owner | Ship privacy, data-use, deletion policy docs before external testing. | 2026-03-19 |
| Delayed cutover due to non-compliant token handling | High | OAuth/Security owner | Enforce encrypted at-rest token storage with key version metadata in Phase 2. | 2026-03-22 |

## Gate Approval

- Scope matrix approved by: _pending_
- Verification timeline approved by: _pending_
- Compliance owner assigned: _pending_
- Gate decision date: _pending_
