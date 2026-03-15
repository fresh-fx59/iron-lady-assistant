from src.gmail_gateway.models import ErrorCode, GatewayError


def test_gateway_error_serializes_with_expected_taxonomy() -> None:
    err = GatewayError(
        code=ErrorCode.REAUTH_REQUIRED,
        error_class="oauth.invalid_grant",
        message="Google token refresh failed with invalid_grant",
        retryable=False,
        details={"account_id": "acc-1"},
    )

    payload = err.to_dict()

    assert payload["error"]["code"] == "reauth_required"
    assert payload["error"]["class"] == "oauth.invalid_grant"
    assert payload["error"]["retryable"] is False
    assert payload["error"]["details"]["account_id"] == "acc-1"
