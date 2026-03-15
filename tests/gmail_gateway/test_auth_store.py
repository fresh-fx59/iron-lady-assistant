from pathlib import Path

from src.gmail_gateway.auth_store import AuthStore


def test_mark_invalid_grant_sets_reauth_required(tmp_path: Path) -> None:
    store = AuthStore(tmp_path / "gateway.db")
    store.upsert_account(account_id="acc-1", gmail_email="alex@example.com")
    store.upsert_token(
        token_id="tok-1",
        account_id="acc-1",
        access_token_ciphertext=b"enc-access",
        refresh_token_ciphertext=b"enc-refresh",
        scopes="gmail.readonly",
        kms_key_version="kms-v1",
        expires_at=None,
    )

    store.mark_invalid_grant(account_id="acc-1")

    state = store.get_account_auth_state(account_id="acc-1")
    assert state is not None
    assert state.status == "reauth_required"
    assert state.auth_state == "expired"
    assert state.invalid_grant_count == 1


def test_upsert_account_resets_reauth_required_after_reconnect(tmp_path: Path) -> None:
    store = AuthStore(tmp_path / "gateway.db")
    store.upsert_account(account_id="acc-1")
    store.upsert_token(
        token_id="tok-1",
        account_id="acc-1",
        access_token_ciphertext=b"enc-access",
        refresh_token_ciphertext=b"enc-refresh",
        scopes="gmail.readonly",
        kms_key_version="kms-v1",
        expires_at=None,
    )
    store.mark_invalid_grant(account_id="acc-1")

    store.upsert_account(account_id="acc-1", gmail_email="alex@example.com")

    state = store.get_account_auth_state(account_id="acc-1")
    assert state is not None
    assert state.status == "active"
    assert state.auth_state == "connected"


def test_connect_session_completion_and_disconnect(tmp_path: Path) -> None:
    store = AuthStore(tmp_path / "gateway.db")
    session = store.start_connect_session(account_id="acc-1", redirect_url="https://app.example.com/done")

    completed = store.complete_connect_session(
        session_id=session.session_id,
        gmail_email="alex@example.com",
        access_token="access-token",
        refresh_token="refresh-token",
        scopes="gmail.readonly gmail.send",
        expires_at=None,
    )
    assert completed is not None
    assert completed.auth_state == "connected"
    assert completed.gmail_email == "alex@example.com"

    disconnected = store.disconnect_account(account_id="acc-1")
    assert disconnected is True
    after = store.get_account_auth_state(account_id="acc-1")
    assert after is not None
    assert after.status == "disabled"
    assert after.auth_state == "revoked"
