"""Gmail gateway foundational models and schema helpers."""

from .auth_store import AccountAuthState, AuthStore, ConnectSession
from .http import create_app
from .message_store import MessageStore
from .models import (
    ErrorCode,
    ErrorEnvelope,
    GatewayError,
    IdempotencyOperation,
)
from .schema import REQUIRED_TABLES, ensure_gateway_schema, verify_gateway_schema
from .sync_store import SyncCursor, SyncStore

__all__ = [
    "ErrorCode",
    "ErrorEnvelope",
    "GatewayError",
    "IdempotencyOperation",
    "AccountAuthState",
    "AuthStore",
    "ConnectSession",
    "create_app",
    "MessageStore",
    "SyncCursor",
    "SyncStore",
    "REQUIRED_TABLES",
    "ensure_gateway_schema",
    "verify_gateway_schema",
]
