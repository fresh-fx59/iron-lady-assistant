from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

import pytest

from src.features.gmail_bootstrap_state import GmailBootstrapSession
from src.gmail_bootstrap_web import (
    _notify_telegram_for_session,
    _oauth_ready,
    _extract_first_url,
    _gcp_failure_guidance,
    _render_session_html,
    _save_bootstrap_oauth_credentials,
    _validate_credentials_json,
    build_google_auth_url,
    build_session_urls,
)


def test_build_session_urls_trims_trailing_slash() -> None:
    urls = build_session_urls(base_url="https://bot.example.com/", session_id="sess-1")

    assert urls == {
        "session_page_url": "https://bot.example.com/gmail/bootstrap/session/sess-1",
        "status_url": "https://bot.example.com/gmail/bootstrap/api/session/sess-1",
    }


def test_session_payload_shape_is_derived_from_session() -> None:
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="cloud_auth_pending",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/oauth/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
    )

    from src.gmail_bootstrap_web import _session_payload

    payload = _session_payload("https://bot.example.com", session)

    assert payload["session_id"] == "sess-1"
    assert payload["phase"] == "cloud_auth_pending"
    assert payload["phase_label"] == "Google sign-in needed"
    assert payload["connected"] is False
    assert payload["urls"]["status_url"].endswith("/gmail/bootstrap/api/session/sess-1")


def test_build_google_auth_url_contains_expected_redirect_and_state(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.config.GMAIL_BOOTSTRAP_GOOGLE_SCOPES",
        ("openid", "email", "https://www.googleapis.com/auth/cloud-platform"),
    )
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="cloud_auth_pending",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/oauth/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
    )

    url = build_google_auth_url(session=session, client_id="client-123")
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    assert parsed.netloc == "accounts.google.com"
    assert query["client_id"] == ["client-123"]
    assert query["state"] == ["sess-1"]
    assert query["redirect_uri"] == ["https://bot.example.com/gmail/bootstrap/google/callback"]


def test_extract_first_url_returns_first_http_match() -> None:
    text = 'open https://accounts.google.com/o/oauth2/v2/auth?x=1 and ignore the rest'
    assert _extract_first_url(text) == "https://accounts.google.com/o/oauth2/v2/auth?x=1"


def test_validate_credentials_json_accepts_installed_payload() -> None:
    payload = _validate_credentials_json('{"installed":{"client_id":"abc"}}')
    assert payload["installed"]["client_id"] == "abc"


def test_render_session_html_shows_bootstrap_upload_form_when_oauth_missing(tmp_path, monkeypatch) -> None:
    creds_path = tmp_path / "bootstrap-client.json"
    monkeypatch.setattr("src.config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID", "")
    monkeypatch.setattr("src.config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET", "")
    monkeypatch.setattr("src.gmail_bootstrap_web._bootstrap_oauth_credentials_path", lambda: creds_path)
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="cloud_auth_pending",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="http://127.0.0.1:8781/gmail/oauth/callback",
        callback_base_url="http://127.0.0.1:8781",
        oauth_client_name="ILA Gmail OAuth",
    )

    html = _render_session_html("http://127.0.0.1:8781", session)

    assert "One-time Bootstrap Prerequisite" in html
    assert "Save Bootstrap Credentials" in html
    assert "/gmail/bootstrap/google/callback" in html


def test_save_bootstrap_oauth_credentials_enables_oauth_ready(tmp_path, monkeypatch) -> None:
    creds_path = tmp_path / "bootstrap-client.json"
    monkeypatch.setattr("src.config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_ID", "")
    monkeypatch.setattr("src.config.GMAIL_BOOTSTRAP_GOOGLE_CLIENT_SECRET", "")
    monkeypatch.setattr("src.gmail_bootstrap_web._bootstrap_oauth_credentials_path", lambda: creds_path)

    _save_bootstrap_oauth_credentials(
        '{"installed":{"client_id":"bootstrap-client-id","client_secret":"bootstrap-client-secret"}}'
    )

    assert _oauth_ready() is True
    saved = json.loads(creds_path.read_text(encoding="utf-8"))
    assert saved["client_id"] == "bootstrap-client-id"
    assert saved["client_secret"] == "bootstrap-client-secret"


def test_render_session_html_includes_manual_checklist_and_upload_form(tmp_path) -> None:
    checklist_path = tmp_path / "MANUAL_CHECKLIST.md"
    checklist_path.write_text(
        "# Gmail self-hosted setup checklist for `ila-demo-project`\n\n"
        "## Manual Google Cloud Console checkpoint\n"
        "1. Open Google Cloud Console for the prepared project.\n",
        encoding="utf-8",
    )
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="oauth_manual_pending",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/oauth/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        manual_checklist_path=str(checklist_path),
    )

    html = _render_session_html("https://bot.example.com", session)

    assert "Manual Checklist" in html
    assert "Upload Credentials and Continue" in html
    assert "Manual Google Cloud Console checkpoint" in html


def test_render_session_html_manual_console_link_opens_new_tab() -> None:
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="oauth_manual_pending",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/bootstrap/gog/callback/sess-1",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        manual_console_url="https://console.cloud.google.com/apis/credentials?project=ila-demo-project",
    )

    html = _render_session_html("https://bot.example.com", session)

    assert "target='_blank'" in html
    assert "Google Cloud Console" in html


def test_render_session_html_shows_retry_button_when_credentials_and_email_exist() -> None:
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="failed",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/bootstrap/gog/callback/sess-1",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        gmail_account_email="alex@gmail.com",
        credentials_path="/tmp/client_secret.json",
    )

    html = _render_session_html("https://bot.example.com", session)

    assert "Retry Gmail Authorization" in html
    assert "/gmail/bootstrap/session/sess-1/gmail-auth/restart" in html


def test_render_session_html_for_google_auth_failure_omits_upload_form() -> None:
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="failed",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/oauth/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        failure_reason="google_auth_error:access_denied",
    )

    html = _render_session_html("https://bot.example.com", session)

    assert "Google sign-in was cancelled or denied" in html
    assert "Upload Credentials and Continue" not in html


def test_render_session_html_prefers_persisted_failure_guidance() -> None:
    session = GmailBootstrapSession(
        session_id="sess-1",
        created_at="2026-03-13T10:00:00+00:00",
        updated_at="2026-03-13T10:00:00+00:00",
        phase="failed",
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/oauth/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        failure_reason="gcp_bootstrap_failed:billing disabled",
        failure_guidance="Enable billing for the project, then retry Gmail setup.",
    )

    html = _render_session_html("https://bot.example.com", session)

    assert "Enable billing for the project, then retry Gmail setup." in html
    assert "Recovery hint" in html


def test_gcp_failure_guidance_mentions_billing_when_detected() -> None:
    guidance = _gcp_failure_guidance("serviceusage API failed because billing is disabled")
    assert "billing" in guidance.lower()


@pytest.mark.asyncio
async def test_notify_telegram_for_session_deduplicates_by_notification_key(tmp_path, monkeypatch) -> None:
    from src.features.gmail_bootstrap_state import GmailBootstrapStateStore

    store = GmailBootstrapStateStore(tmp_path / "gmail_bootstrap_sessions.json")
    session = store.start_session(
        project_id="ila-demo-project",
        project_name="ILA Demo Project",
        redirect_uri="https://bot.example.com/gmail/oauth/callback",
        callback_base_url="https://bot.example.com",
        oauth_client_name="ILA Gmail OAuth",
        telegram_chat_id=123,
        telegram_thread_id=456,
    )
    session = store.record_project_bootstrap(
        session_id=session.session_id,
        project_number="1234567890",
        manual_console_url="https://console.cloud.google.com/apis/credentials",
    )
    sent: list[str] = []

    async def fake_send_telegram_update(session_obj, text: str) -> None:
        sent.append(text)

    monkeypatch.setattr("src.gmail_bootstrap_web._send_telegram_update", fake_send_telegram_update)

    await _notify_telegram_for_session(store, session)
    refreshed = store.get(session.session_id)
    assert refreshed is not None
    assert refreshed.last_telegram_notification_key == "oauth_manual_pending"

    await _notify_telegram_for_session(store, refreshed)
    assert len(sent) == 1
