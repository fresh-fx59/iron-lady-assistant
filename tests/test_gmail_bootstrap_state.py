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
        telegram_chat_id=123,
        telegram_thread_id=456,
    )

    assert session.phase == "cloud_auth_pending"
    assert session.project_id == "ila-demo-project"
    assert session.telegram_chat_id == 123
    assert session.telegram_thread_id == 456
    assert store.get(session.session_id) is not None


def test_bootstrap_state_progresses_through_key_phases(tmp_path) -> None:
    store = GmailBootstrapStateStore(tmp_path / "gmail_bootstrap_sessions.json")
    session = store.start_session(
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        telegram_chat_id=123,
        telegram_thread_id=None,
    )

    cloud = store.record_cloud_auth(session_id=session.session_id, account_email="alex@example.com")
    assert cloud is not None
    assert cloud.phase == "cloud_auth_granted"
    assert cloud.gcloud_account_email == "alex@example.com"

    project = store.record_project_bootstrap(
        session_id=session.session_id,
        project_number="1234567890",
        manual_console_url="https://console.cloud.google.com/apis/credentials",
        manual_checklist_path="/tmp/MANUAL_CHECKLIST.md",
    )
    assert project is not None
    assert project.phase == "oauth_manual_pending"
    assert project.manual_checklist_path == "/tmp/MANUAL_CHECKLIST.md"

    uploaded = store.record_credentials_uploaded(
        session_id=session.session_id,
        credentials_path="/tmp/client_secret.json",
    )
    assert uploaded is not None
    assert uploaded.phase == "credentials_uploaded"

    started = store.record_gmail_auth_started_for_account(
        session_id=session.session_id,
        gmail_account_email="alex@gmail.com",
    )
    assert started is not None
    assert started.phase == "gmail_auth_pending"
    assert started.gmail_account_email == "alex@gmail.com"

    completed = store.record_completed(
        session_id=session.session_id,
        gmail_account_email="alex@gmail.com",
    )
    assert completed is not None
    assert completed.phase == "completed"
    assert completed.gmail_account_email == "alex@gmail.com"
    assert completed.connected_at is not None

    completed_sessions = store.list_completed()
    assert [item.session_id for item in completed_sessions] == [session.session_id]
    assert store.latest_for_scope(telegram_chat_id=123, telegram_thread_id=None) is not None


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


def test_record_telegram_notification_persists_last_notification_key(tmp_path) -> None:
    store = GmailBootstrapStateStore(tmp_path / "gmail_bootstrap_sessions.json")
    session = store.start_session(
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        telegram_chat_id=123,
        telegram_thread_id=None,
    )

    updated = store.record_telegram_notification(
        session_id=session.session_id,
        notification_key="oauth_manual_pending",
    )

    assert updated is not None
    assert updated.last_telegram_notification_key == "oauth_manual_pending"
