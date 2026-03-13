from __future__ import annotations

from src.features.gmail_bootstrap_state import GmailBootstrapStateStore


def test_start_session_persists_browser_first_bootstrap_state(tmp_path) -> None:
    store = GmailBootstrapStateStore(tmp_path / "gmail_bootstrap_sessions.json")

    session = store.start_session(
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
    )

    assert session.phase == "cloud_auth_pending"
    assert session.project_id == "ila-demo-project"
    assert store.get(session.session_id) is not None


def test_bootstrap_state_progresses_through_key_phases(tmp_path) -> None:
    store = GmailBootstrapStateStore(tmp_path / "gmail_bootstrap_sessions.json")
    session = store.start_session(
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
    )

    cloud = store.record_cloud_auth(session_id=session.session_id, account_email="alex@example.com")
    assert cloud is not None
    assert cloud.phase == "cloud_auth_granted"
    assert cloud.gcloud_account_email == "alex@example.com"

    project = store.record_project_bootstrap(
        session_id=session.session_id,
        project_number="1234567890",
        manual_console_url="https://console.cloud.google.com/apis/credentials",
    )
    assert project is not None
    assert project.phase == "oauth_manual_pending"

    uploaded = store.record_credentials_uploaded(
        session_id=session.session_id,
        credentials_path="/tmp/client_secret.json",
    )
    assert uploaded is not None
    assert uploaded.phase == "credentials_uploaded"

    started = store.record_gmail_auth_started(session_id=session.session_id)
    assert started is not None
    assert started.phase == "gmail_auth_pending"

    completed = store.record_completed(
        session_id=session.session_id,
        gmail_account_email="alex@gmail.com",
    )
    assert completed is not None
    assert completed.phase == "completed"
    assert completed.gmail_account_email == "alex@gmail.com"


def test_failed_state_is_persisted(tmp_path) -> None:
    store = GmailBootstrapStateStore(tmp_path / "gmail_bootstrap_sessions.json")
    session = store.start_session(
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
    )

    failed = store.record_failed(session_id=session.session_id, reason="consent_screen_blocked")

    assert failed is not None
    assert failed.phase == "failed"
    assert failed.failure_reason == "consent_screen_blocked"
