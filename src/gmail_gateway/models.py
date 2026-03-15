from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    RETRYABLE = "retryable"
    REAUTH_REQUIRED = "reauth_required"
    POLICY_BLOCKED = "policy_blocked"
    QUOTA_LIMITED = "quota_limited"
    INVALID_REQUEST = "invalid_request"
    NOT_FOUND = "not_found"
    INTERNAL_ERROR = "internal_error"


class IdempotencyOperation(str, Enum):
    SEND_MESSAGE = "send_message"
    TRASH_MESSAGE = "trash_message"
    DELETE_MESSAGE = "delete_message"


@dataclass(frozen=True)
class ErrorEnvelope:
    code: ErrorCode
    error_class: str
    message: str
    retryable: bool
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code.value,
                "class": self.error_class,
                "message": self.message,
                "retryable": self.retryable,
                "details": self.details,
            }
        }


class GatewayError(RuntimeError):
    def __init__(
        self,
        *,
        code: ErrorCode,
        error_class: str,
        message: str,
        retryable: bool,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.envelope = ErrorEnvelope(
            code=code,
            error_class=error_class,
            message=message,
            retryable=retryable,
            details=details or {},
        )

    def to_dict(self) -> dict[str, Any]:
        return self.envelope.to_dict()
