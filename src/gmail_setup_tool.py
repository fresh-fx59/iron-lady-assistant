"""Helpers for Gmail setup via shared credentials or self-hosted user-owned bootstrap."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence

GOG_ENV_NAMES = ("GOG_OAUTH_CREDENTIALS_PATH", "GMAIL_OAUTH_CREDENTIALS_PATH")
GOG_JSON_ENV_NAMES = ("GOG_OAUTH_CREDENTIALS_JSON", "GMAIL_OAUTH_CREDENTIALS_JSON")
DEFAULT_SELF_HOSTED_DIR = Path.home() / ".config" / "iron-lady-assistant" / "gmail-self-hosted"
DEFAULT_SHARED_CREDENTIAL_PATHS = (
    Path.home() / ".config" / "iron-lady-assistant" / "google-oauth-client.json",
    Path.home() / ".config" / "gogcli" / "client_credentials.json",
)
DEFAULT_REQUIRED_APIS = (
    "gmail.googleapis.com",
    "people.googleapis.com",
)


@dataclass
class CredentialSource:
    kind: str
    value: str
    source: str


@dataclass
class SetupStatus:
    gog_path: str | None
    stored_credentials_configured: bool
    shared_credentials: CredentialSource | None


@dataclass
class SelfHostedStatus:
    gog_path: str | None
    gcloud_path: str | None
    gcloud_account: str | None
    bootstrap_dir: str
    bootstrap_ready: bool


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.gmail_setup_tool")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="Inspect local gog/Gmail setup prerequisites.")
    doctor.add_argument("--format", choices=("json", "text"), default="text")

    self_hosted_doctor = sub.add_parser(
        "self-hosted-doctor",
        help="Inspect local readiness for a per-user self-hosted Gmail API bootstrap.",
    )
    self_hosted_doctor.add_argument("--format", choices=("json", "text"), default="text")

    authorize = sub.add_parser(
        "authorize",
        help="Configure shared OAuth credentials if needed, then start gog auth for an account.",
    )
    authorize.add_argument("--account", required=True, help="Google account email to authorize.")
    authorize.add_argument("--services", default="gmail", help="Comma-separated gog services.")
    authorize.add_argument(
        "--gmail-scope",
        choices=("full", "readonly"),
        default="full",
        help="Gmail scope mode passed to gog auth add.",
    )
    authorize.add_argument(
        "--remote",
        action="store_true",
        help="Use gog remote auth flow for server/headless environments.",
    )
    authorize.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands that would run instead of executing them.",
    )

    bootstrap = sub.add_parser(
        "self-hosted-bootstrap",
        help="Prepare local bootstrap files and optional gcloud commands for isolated Gmail setup.",
    )
    bootstrap.add_argument("--project-id", required=True, help="Google Cloud project id to create/use.")
    bootstrap.add_argument(
        "--project-name",
        default="Iron Lady Assistant Gmail",
        help="Human-readable Google Cloud project name.",
    )
    bootstrap.add_argument(
        "--redirect-uri",
        default="http://localhost:8080/oauth2/callback",
        help="Redirect URI that will be used for the OAuth client.",
    )
    bootstrap.add_argument(
        "--oauth-client-name",
        default="Iron Lady Assistant Gmail",
        help="Suggested name for the OAuth client shown in the generated checklist.",
    )
    bootstrap.add_argument(
        "--bootstrap-dir",
        default=str(DEFAULT_SELF_HOSTED_DIR),
        help="Directory where bootstrap config/checklist files will be written.",
    )
    bootstrap.add_argument(
        "--skip-project-create",
        action="store_true",
        help="Do not create the Google Cloud project automatically.",
    )
    bootstrap.add_argument(
        "--skip-enable-apis",
        action="store_true",
        help="Do not enable Gmail/People APIs automatically.",
    )
    bootstrap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the commands and output paths without executing or writing files.",
    )
    return parser


def _find_binary(name: str, *, fallback: Path | None = None) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    if fallback and fallback.exists():
        return str(fallback)
    return None


def _find_gog_binary() -> str | None:
    return _find_binary("gog", fallback=Path.home() / ".local" / "bin" / "gog")


def _find_gcloud_binary() -> str | None:
    return _find_binary("gcloud")


def _has_stored_credentials(gog_path: str) -> bool:
    result = subprocess.run(
        [gog_path, "auth", "credentials", "list"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return False
    output = "\n".join(part for part in (result.stdout, result.stderr) if part)
    return "No OAuth client credentials stored" not in output


def find_shared_credentials(env: Mapping[str, str] | None = None) -> CredentialSource | None:
    env_map = env or os.environ
    for name in GOG_ENV_NAMES:
        raw = env_map.get(name, "").strip()
        if raw:
            path = Path(raw).expanduser()
            if path.exists():
                return CredentialSource(kind="path", value=str(path), source=f"env:{name}")
    for path in DEFAULT_SHARED_CREDENTIAL_PATHS:
        if path.exists():
            return CredentialSource(kind="path", value=str(path), source=f"path:{path}")
    for name in GOG_JSON_ENV_NAMES:
        raw = env_map.get(name, "").strip()
        if raw:
            return CredentialSource(kind="json", value=raw, source=f"env:{name}")
    return None


def inspect_setup(env: Mapping[str, str] | None = None) -> SetupStatus:
    gog_path = _find_gog_binary()
    if not gog_path:
        return SetupStatus(
            gog_path=None,
            stored_credentials_configured=False,
            shared_credentials=find_shared_credentials(env),
        )
    return SetupStatus(
        gog_path=gog_path,
        stored_credentials_configured=_has_stored_credentials(gog_path),
        shared_credentials=find_shared_credentials(env),
    )


def _detect_active_gcloud_account(gcloud_path: str) -> str | None:
    result = subprocess.run(
        [
            gcloud_path,
            "auth",
            "list",
            "--filter=status:ACTIVE",
            "--format=value(account)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    account = result.stdout.strip()
    return account or None


def inspect_self_hosted_setup(bootstrap_dir: Path | None = None) -> SelfHostedStatus:
    target_dir = (bootstrap_dir or DEFAULT_SELF_HOSTED_DIR).expanduser()
    gog_path = _find_gog_binary()
    gcloud_path = _find_gcloud_binary()
    account = _detect_active_gcloud_account(gcloud_path) if gcloud_path else None
    return SelfHostedStatus(
        gog_path=gog_path,
        gcloud_path=gcloud_path,
        gcloud_account=account,
        bootstrap_dir=str(target_dir),
        bootstrap_ready=target_dir.exists(),
    )


def _run_command(command: Sequence[str], *, input_text: str | None = None, dry_run: bool = False) -> int:
    rendered = " ".join(command)
    if dry_run:
        print(rendered)
        return 0
    result = subprocess.run(command, input=input_text, text=True, check=False)
    return result.returncode


def ensure_shared_credentials(
    status: SetupStatus,
    *,
    dry_run: bool = False,
) -> int:
    if not status.gog_path:
        print("gog binary not found; install gogcli first.", file=sys.stderr)
        return 1
    if status.stored_credentials_configured:
        return 0
    if status.shared_credentials is None:
        print(
            "No shared Google OAuth client configured. "
            "Set GOG_OAUTH_CREDENTIALS_PATH or GOG_OAUTH_CREDENTIALS_JSON once on the host.",
            file=sys.stderr,
        )
        return 2

    if status.shared_credentials.kind == "path":
        return _run_command(
            [status.gog_path, "auth", "credentials", "set", status.shared_credentials.value],
            dry_run=dry_run,
        )
    return _run_command(
        [status.gog_path, "auth", "credentials", "set", "-"],
        input_text=status.shared_credentials.value,
        dry_run=dry_run,
    )


def build_authorize_command(
    *,
    gog_path: str,
    account: str,
    services: str,
    gmail_scope: str,
    remote: bool,
) -> list[str]:
    command = [gog_path, "auth", "add", account, "--services", services]
    if "gmail" in {item.strip() for item in services.split(",") if item.strip()}:
        command.extend(["--gmail-scope", gmail_scope])
    if remote:
        command.append("--remote")
    return command


def build_self_hosted_bootstrap_commands(
    *,
    gcloud_path: str,
    project_id: str,
    project_name: str,
    create_project: bool,
    enable_apis: bool,
    required_apis: Sequence[str] = DEFAULT_REQUIRED_APIS,
) -> list[list[str]]:
    commands: list[list[str]] = []
    if create_project:
        commands.append([gcloud_path, "projects", "create", project_id, f"--name={project_name}"])
    commands.append([gcloud_path, "config", "set", "project", project_id])
    if enable_apis:
        commands.append([gcloud_path, "services", "enable", *required_apis])
    return commands


def _bootstrap_paths(bootstrap_dir: Path, project_id: str) -> tuple[Path, Path]:
    base_dir = bootstrap_dir / project_id
    return base_dir / "bootstrap.json", base_dir / "MANUAL_CHECKLIST.md"


def build_manual_checklist(
    *,
    project_id: str,
    project_name: str,
    redirect_uri: str,
    oauth_client_name: str,
) -> str:
    return "\n".join(
        [
            f"# Gmail self-hosted setup checklist for `{project_id}`",
            "",
            "Use this checklist after bootstrap has prepared the project locally.",
            "",
            "## Automated part already prepared",
            f"- Project id: `{project_id}`",
            f"- Project name: `{project_name}`",
            f"- Redirect URI: `{redirect_uri}`",
            "",
            "## Manual Google Cloud Console checkpoint",
            "1. Open Google Cloud Console for the prepared project.",
            "2. Configure OAuth consent screen.",
            f"3. Create an OAuth client named `{oauth_client_name}`.",
            f"4. Add redirect URI `{redirect_uri}`.",
            "5. Download the credentials JSON.",
            "",
            "## Finish locally",
            "1. Save the file as `client_secret.json` in the same bootstrap directory.",
            "2. Return to the Gmail Connect session page and use the UI flow (`Upload Credentials and Continue`, then `Retry Gmail Authorization` if needed).",
            "",
            "Note: this manual checkpoint exists because Google does not expose full generic OAuth client creation for Gmail via public automation APIs.",
        ]
    )


def write_self_hosted_bundle(
    *,
    bootstrap_dir: Path,
    project_id: str,
    project_name: str,
    redirect_uri: str,
    oauth_client_name: str,
) -> tuple[Path, Path]:
    config_path, checklist_path = _bootstrap_paths(bootstrap_dir, project_id)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "project_id": project_id,
        "project_name": project_name,
        "redirect_uri": redirect_uri,
        "oauth_client_name": oauth_client_name,
        "credentials_import_path": str(config_path.parent / "client_secret.json"),
        "checklist_path": str(checklist_path),
    }
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    checklist_path.write_text(
        build_manual_checklist(
            project_id=project_id,
            project_name=project_name,
            redirect_uri=redirect_uri,
            oauth_client_name=oauth_client_name,
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path, checklist_path


def _format_text(status: SetupStatus) -> str:
    lines = [
        f"gog_path: {status.gog_path or 'missing'}",
        f"stored_credentials_configured: {status.stored_credentials_configured}",
        f"shared_credentials: {status.shared_credentials.source if status.shared_credentials else 'missing'}",
    ]
    if not status.gog_path:
        lines.append("next_step: install gogcli")
    elif status.stored_credentials_configured:
        lines.append("next_step: run authorize for target account")
    elif status.shared_credentials:
        lines.append("next_step: shared OAuth client is available and can be installed automatically")
    else:
        lines.append(
            "next_step: operator must provision shared OAuth client once via "
            "GOG_OAUTH_CREDENTIALS_PATH or GOG_OAUTH_CREDENTIALS_JSON"
        )
    return "\n".join(lines)


def _format_self_hosted_text(status: SelfHostedStatus) -> str:
    lines = [
        f"gog_path: {status.gog_path or 'missing'}",
        f"gcloud_path: {status.gcloud_path or 'missing'}",
        f"gcloud_account: {status.gcloud_account or 'missing'}",
        f"bootstrap_dir: {status.bootstrap_dir}",
        f"bootstrap_ready: {status.bootstrap_ready}",
    ]
    if not status.gcloud_path:
        lines.append("next_step: install Google Cloud SDK only if you want the CLI fallback path")
    elif not status.gcloud_account:
        lines.append(
            "next_step: for the CLI fallback path run `gcloud auth login`; "
            "browser-first setup should obtain Google auth through the web flow instead"
        )
    else:
        lines.append("next_step: CLI fallback is ready; browser-first flow can reuse the same local host")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "doctor":
        status = inspect_setup()
        if args.format == "json":
            print(json.dumps(asdict(status), ensure_ascii=False, indent=2))
        else:
            print(_format_text(status))
        return 0

    if args.command == "self-hosted-doctor":
        status = inspect_self_hosted_setup()
        if args.format == "json":
            print(json.dumps(asdict(status), ensure_ascii=False, indent=2))
        else:
            print(_format_self_hosted_text(status))
        return 0

    if args.command == "self-hosted-bootstrap":
        status = inspect_self_hosted_setup(Path(args.bootstrap_dir))
        if not status.gcloud_path:
            print("gcloud binary not found; install Google Cloud SDK first.", file=sys.stderr)
            return 1
        commands = build_self_hosted_bootstrap_commands(
            gcloud_path=status.gcloud_path,
            project_id=args.project_id,
            project_name=args.project_name,
            create_project=not args.skip_project_create,
            enable_apis=not args.skip_enable_apis,
        )
        config_path, checklist_path = _bootstrap_paths(Path(args.bootstrap_dir).expanduser(), args.project_id)
        if args.dry_run:
            for command in commands:
                print(" ".join(command))
            print(f"would_write_config: {config_path}")
            print(f"would_write_checklist: {checklist_path}")
            return 0
        for command in commands:
            exit_code = _run_command(command)
            if exit_code != 0:
                return exit_code
        written_config, written_checklist = write_self_hosted_bundle(
            bootstrap_dir=Path(args.bootstrap_dir).expanduser(),
            project_id=args.project_id,
            project_name=args.project_name,
            redirect_uri=args.redirect_uri,
            oauth_client_name=args.oauth_client_name,
        )
        print(f"bootstrap_config: {written_config}")
        print(f"manual_checklist: {written_checklist}")
        return 0

    status = inspect_setup()
    configured = ensure_shared_credentials(status, dry_run=args.dry_run)
    if configured != 0:
        return configured
    if not status.gog_path:
        return 1
    command = build_authorize_command(
        gog_path=status.gog_path,
        account=args.account,
        services=args.services,
        gmail_scope=args.gmail_scope,
        remote=args.remote,
    )
    return _run_command(command, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
