from __future__ import annotations

import json
from pathlib import Path

from src import gmail_setup_tool


def test_find_shared_credentials_prefers_explicit_path(tmp_path: Path) -> None:
    creds = tmp_path / "client.json"
    creds.write_text('{"installed":{}}', encoding="utf-8")

    source = gmail_setup_tool.find_shared_credentials(
        {
            "GOG_OAUTH_CREDENTIALS_PATH": str(creds),
            "GOG_OAUTH_CREDENTIALS_JSON": '{"ignored":true}',
        }
    )

    assert source == gmail_setup_tool.CredentialSource(
        kind="path",
        value=str(creds),
        source="env:GOG_OAUTH_CREDENTIALS_PATH",
    )


def test_find_shared_credentials_uses_json_env_when_no_path() -> None:
    source = gmail_setup_tool.find_shared_credentials(
        {"GOG_OAUTH_CREDENTIALS_JSON": '{"installed":{"client_id":"abc"}}'}
    )

    assert source == gmail_setup_tool.CredentialSource(
        kind="json",
        value='{"installed":{"client_id":"abc"}}',
        source="env:GOG_OAUTH_CREDENTIALS_JSON",
    )


def test_build_authorize_command_adds_remote_and_gmail_scope() -> None:
    command = gmail_setup_tool.build_authorize_command(
        gog_path="/tmp/gog",
        account="alex@example.com",
        services="gmail,calendar",
        gmail_scope="readonly",
        remote=True,
    )

    assert command == [
        "/tmp/gog",
        "auth",
        "add",
        "alex@example.com",
        "--services",
        "gmail,calendar",
        "--gmail-scope",
        "readonly",
        "--remote",
    ]


def test_build_authorize_command_skips_gmail_scope_for_other_services() -> None:
    command = gmail_setup_tool.build_authorize_command(
        gog_path="/tmp/gog",
        account="alex@example.com",
        services="calendar",
        gmail_scope="readonly",
        remote=False,
    )

    assert command == [
        "/tmp/gog",
        "auth",
        "add",
        "alex@example.com",
        "--services",
        "calendar",
    ]


def test_doctor_json_reports_missing_setup(monkeypatch, capsys) -> None:
    monkeypatch.setattr(gmail_setup_tool, "_find_gog_binary", lambda: None)

    exit_code = gmail_setup_tool.main(["doctor", "--format", "json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["gog_path"] is None
    assert payload["stored_credentials_configured"] is False


def test_has_stored_credentials_checks_stderr(monkeypatch) -> None:
    class _Result:
        returncode = 0
        stdout = ""
        stderr = "No OAuth client credentials stored\n"

    monkeypatch.setattr(gmail_setup_tool.subprocess, "run", lambda *args, **kwargs: _Result())

    assert gmail_setup_tool._has_stored_credentials("/tmp/gog") is False


def test_build_self_hosted_bootstrap_commands_includes_project_and_api_steps() -> None:
    commands = gmail_setup_tool.build_self_hosted_bootstrap_commands(
        gcloud_path="/tmp/gcloud",
        project_id="ila-gmail-demo",
        project_name="ILA Gmail Demo",
        create_project=True,
        enable_apis=True,
    )

    assert commands == [
        ["/tmp/gcloud", "projects", "create", "ila-gmail-demo", "--name=ILA Gmail Demo"],
        ["/tmp/gcloud", "config", "set", "project", "ila-gmail-demo"],
        [
            "/tmp/gcloud",
            "services",
            "enable",
            "gmail.googleapis.com",
            "people.googleapis.com",
        ],
    ]


def test_write_self_hosted_bundle_writes_config_and_checklist(tmp_path: Path) -> None:
    config_path, checklist_path = gmail_setup_tool.write_self_hosted_bundle(
        bootstrap_dir=tmp_path,
        project_id="ila-gmail-demo",
        project_name="ILA Gmail Demo",
        redirect_uri="http://localhost:8080/oauth2/callback",
        oauth_client_name="ILA Gmail OAuth",
    )

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    checklist = checklist_path.read_text(encoding="utf-8")

    assert payload["project_id"] == "ila-gmail-demo"
    assert payload["oauth_client_name"] == "ILA Gmail OAuth"
    assert "Google Cloud Console checkpoint" in checklist
    assert "http://localhost:8080/oauth2/callback" in checklist


def test_self_hosted_doctor_json_reports_missing_gcloud(monkeypatch, capsys, tmp_path: Path) -> None:
    monkeypatch.setattr(gmail_setup_tool, "_find_gcloud_binary", lambda: None)
    monkeypatch.setattr(gmail_setup_tool, "_find_gog_binary", lambda: "/tmp/gog")
    monkeypatch.setattr(gmail_setup_tool, "DEFAULT_SELF_HOSTED_DIR", tmp_path)

    exit_code = gmail_setup_tool.main(["self-hosted-doctor", "--format", "json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["gcloud_path"] is None
    assert payload["gcloud_account"] is None
