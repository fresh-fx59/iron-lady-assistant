import subprocess
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from src.memory import MemoryManager
from src.memory_tool import main as memory_tool_main
from src.worklog_tool import main as worklog_tool_main


def _write_profile(path: Path, payload: dict) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_legacy_yaml_migrates_to_sql_and_is_removed(tmp_path: Path) -> None:
    memory_dir = tmp_path / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    profile_path = memory_dir / "user_profile.yaml"
    _write_profile(
        profile_path,
        {
            "name": "Alex",
            "preferences": {"timezone": "UTC+3", "languages": ["Russian", "English"]},
            "facts": [
                {
                    "key": "commit_versioning_rule",
                    "value": "Always bump version on every commit",
                    "confidence": 1.0,
                    "source": "explicit",
                    "updated": "2026-03-07",
                }
            ],
        },
    )

    manager = MemoryManager(memory_dir)
    facts = manager.list_facts(fact_type="workflow", include_deleted=True)

    assert profile_path.exists() is False
    assert any(f["key"] == "commit_versioning_rule" for f in facts)


def test_build_context_groups_relevant_facts_by_type(tmp_path: Path) -> None:
    manager = MemoryManager(tmp_path / "memory")
    manager.upsert_fact(
        key="location",
        value="Ryazan, Russia",
        fact_type="identity",
        confidence=1.0,
        source="explicit",
        updated="2026-03-07",
        mode="append",
    )
    manager.upsert_fact(
        key="feature_apply_commit_push_verify_preference",
        value="After applying a feature, commit and push, then verify",
        fact_type="workflow",
        confidence=1.0,
        source="explicit",
        updated="2026-03-07",
        mode="append",
    )
    manager.upsert_fact(
        key="monitoring_server_connection",
        value="ssh user1@45.151.30.146",
        fact_type="infrastructure",
        confidence=1.0,
        source="explicit",
        updated="2026-03-07",
        mode="append",
    )

    context = manager.build_context("continue commit push workflow")

    assert "<relevant_facts>" in context
    assert "[workflow]" in context
    assert "feature_apply_commit_push_verify_preference" in context


def test_search_episodes_scoped_fallback_avoids_unrelated_recent_topic(tmp_path: Path) -> None:
    manager = MemoryManager(tmp_path / "memory")
    manager.add_episode(
        chat_id=42,
        message_thread_id=7,
        scope_key="42:7",
        summary="Fix browser takeover context bleed",
        topics=["context"],
    )
    manager.add_episode(
        chat_id=42,
        message_thread_id=8,
        scope_key="42:8",
        summary="Plan a weekend trip to Prague",
        topics=["travel"],
    )

    episodes = manager.search_episodes(
        "totally unrelated query",
        chat_id=42,
        message_thread_id=7,
        scope_key="42:7",
    )

    assert len(episodes) == 1
    assert episodes[0]["summary"] == "Fix browser takeover context bleed"


def test_search_episodes_same_scope_prefers_matching_topic_label(tmp_path: Path) -> None:
    manager = MemoryManager(tmp_path / "memory")
    manager.add_episode(
        chat_id=42,
        message_thread_id=7,
        scope_key="42:7",
        session_type="codex",
        session_id="sess-alpha",
        topic_label="Alpha topic",
        summary="Implement alpha context isolation",
        topics=["context"],
    )
    manager.add_episode(
        chat_id=42,
        message_thread_id=7,
        scope_key="42:7",
        session_type="codex",
        session_id="sess-beta",
        topic_label="Beta topic",
        summary="Implement beta notifications",
        topics=["notifications"],
    )

    episodes = manager.search_episodes(
        "unmatched query",
        chat_id=42,
        message_thread_id=7,
        scope_key="42:7",
        topic_label="Alpha topic",
    )

    assert len(episodes) == 1
    assert episodes[0]["summary"] == "Implement alpha context isolation"


def test_build_instructions_require_sql_memory_manager() -> None:
    manager = MemoryManager(Path("memory"))
    instructions = manager.build_instructions()

    assert "memory-manager tool" in instructions
    assert "no YAML profile file" in instructions
    assert "Allowed fact types:" in instructions


def test_memory_tool_upsert_and_delete(tmp_path: Path, capsys) -> None:
    memory_dir = tmp_path / "memory"
    MemoryManager(memory_dir)

    upsert_rc = memory_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "upsert",
            "--key",
            "test_fact",
            "--value",
            "value",
            "--type",
            "workflow",
        ]
    )
    assert upsert_rc == 0
    capsys.readouterr()

    list_rc = memory_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "list",
            "--type",
            "workflow",
        ]
    )
    assert list_rc == 0
    listed = capsys.readouterr().out
    assert "test_fact" in listed

    delete_rc = memory_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "delete",
            "--key",
            "test_fact",
        ]
    )
    assert delete_rc == 0
    deleted = capsys.readouterr().out
    assert '"removed": true' in deleted

    list_after_delete_rc = memory_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "list",
            "--type",
            "workflow",
        ]
    )
    assert list_after_delete_rc == 0
    listed_after_delete = capsys.readouterr().out
    assert "test_fact" not in listed_after_delete

    list_deleted_rc = memory_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "list",
            "--type",
            "workflow",
            "--include-deleted",
        ]
    )
    assert list_deleted_rc == 0
    listed_with_deleted = capsys.readouterr().out
    assert "test_fact" in listed_with_deleted


def test_upsert_append_and_replace_modes(tmp_path: Path) -> None:
    manager = MemoryManager(tmp_path / "memory")
    manager.upsert_fact(
        key="environment",
        value="staging",
        fact_type="operation",
        mode="append",
    )
    manager.upsert_fact(
        key="environment",
        value="prod",
        fact_type="operation",
        mode="append",
    )
    active_before_replace = manager.list_facts(fact_type="operation")
    env_values_before = sorted(f["value"] for f in active_before_replace if f["key"] == "environment")
    assert env_values_before == ["prod", "staging"]

    manager.upsert_fact(
        key="environment",
        value="production",
        fact_type="operation",
        mode="replace",
    )
    active_after_replace = manager.list_facts(fact_type="operation")
    env_values_after = [f["value"] for f in active_after_replace if f["key"] == "environment"]
    assert env_values_after == ["production"]

    with_deleted = manager.list_facts(fact_type="operation", include_deleted=True)
    deleted_versions = [f for f in with_deleted if f["key"] == "environment" and f["status"] == "deleted"]
    assert len(deleted_versions) >= 1


def test_memory_facts_hard_delete_is_blocked(tmp_path: Path) -> None:
    manager = MemoryManager(tmp_path / "memory")
    manager.upsert_fact(
        key="sql_guard_check",
        value="must_soft_delete_only",
        fact_type="workflow",
        mode="replace",
    )

    con = manager._connect()  # noqa: SLF001
    try:
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("DELETE FROM memory_facts")
    finally:
        con.close()


def test_worklog_links_summary_to_commit_and_files(tmp_path: Path) -> None:
    manager = MemoryManager(tmp_path / "memory")
    episode_id = manager.add_episode(
        chat_id=42,
        message_thread_id=7,
        scope_key="42:7",
        provider="codex",
        session_type="codex",
        session_id="sess-42",
        repo_path="/repo",
        branch="main",
        summary="Fixed summary linkage",
        topics=["memory"],
        decisions=["store commits in sqlite"],
        entities=["episodes.db"],
    )

    result = manager.record_commit_link(
        chat_id=42,
        message_thread_id=7,
        scope_key="42:7",
        provider="codex",
        session_type="codex",
        session_id="sess-42",
        repo_path="/repo",
        branch="main",
        commit_sha="abcdef1234567890",
        short_sha="abcdef1",
        subject="Persist worklog metadata",
        authored_at="2026-03-07T10:00:00+00:00",
        committed_at="2026-03-07T10:05:00+00:00",
        files=[
            {"path": "src/memory.py", "additions": 10, "deletions": 2},
            {"path": "src/worklog_tool.py", "additions": 50, "deletions": 0},
        ],
    )

    links = manager.list_worklog_links(chat_id=42, limit=3)

    assert episode_id > 0
    assert result["file_count"] == 2
    assert len(links) == 1
    assert links[0]["summary"] == "Fixed summary linkage"
    assert links[0]["session_id"] == "sess-42"
    assert links[0]["commits"][0]["commit_sha"] == "abcdef1234567890"
    assert {item["path"] for item in links[0]["files"]} == {"src/memory.py", "src/worklog_tool.py"}


def test_worklog_tool_records_summary_and_commit(tmp_path: Path, capsys) -> None:
    memory_dir = tmp_path / "memory"
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    subprocess.run(["git", "-C", str(repo_dir), "init"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.name", "Test User"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.email", "test@example.com"], check=True)
    tracked = repo_dir / "tracked.txt"
    tracked.write_text("hello\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_dir), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", "Initial tracked commit"], check=True)

    summary_rc = worklog_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "record-summary",
            "--chat-id",
            "123",
            "--message-thread-id",
            "9",
            "--provider",
            "codex",
            "--session-type",
            "codex",
            "--session-id",
            "sess-123",
            "--summary",
            "Stored summary in sqlite",
            "--topics-json",
            '["memory"]',
            "--decisions-json",
            '["track commits"]',
            "--entities-json",
            '["episodes.db"]',
            "--repo-path",
            str(repo_dir),
            "--branch",
            "master",
        ]
    )
    assert summary_rc == 0
    capsys.readouterr()

    tracked.write_text("hello\nworld\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_dir), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", "Update tracked file"], check=True)

    commit_rc = worklog_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "record-commit",
            "--chat-id",
            "123",
            "--message-thread-id",
            "9",
            "--provider",
            "codex",
            "--session-type",
            "codex",
            "--session-id",
            "sess-123",
            "--repo-path",
            str(repo_dir),
            "--commit",
            "HEAD",
        ]
    )
    assert commit_rc == 0
    capsys.readouterr()

    list_rc = worklog_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "list",
            "--chat-id",
            "123",
            "--limit",
            "2",
        ]
    )
    assert list_rc == 0
    listed = capsys.readouterr().out
    assert "Stored summary in sqlite" in listed
    assert "Update tracked file" in listed
    assert "tracked.txt" in listed


def test_worklog_tool_auto_records_latest_active_session(capsys, monkeypatch) -> None:
    work_dir = Path.cwd()
    memory_dir = work_dir / "memory"
    repo_dir = work_dir / "repo"
    for key in (
        "ILA_WORKLOG_SCOPE_KEY",
        "ILA_WORKLOG_CHAT_ID",
        "ILA_WORKLOG_SESSION_ID",
        "ILA_WORKLOG_SESSION_TYPE",
        "ILA_WORKLOG_PROVIDER",
        "ILA_WORKLOG_LAST_ACTIVITY_AT",
        "ILA_WORKLOG_MESSAGE_THREAD_ID",
        "ILA_WORKLOG_TOPIC_LABEL",
        "ILA_WORKLOG_TOPIC_STARTED_AT",
    ):
        monkeypatch.delenv(key, raising=False)
    repo_dir.mkdir()
    subprocess.run(["git", "-C", str(repo_dir), "init"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.name", "Test User"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.email", "test@example.com"], check=True)
    tracked = repo_dir / "tracked.txt"
    tracked.write_text("one\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_dir), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", "Initial commit"], check=True)
    tracked.write_text("one\ntwo\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_dir), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", "Auto recorded commit"], check=True)

    sessions_file = work_dir / "sessions.json"
    sessions_file.write_text(
        json.dumps(
            {
                "123:main": {
                    "claude_session_id": None,
                    "codex_session_id": "codex-sess-1",
                    "model": "sonnet",
                    "codex_model": "gpt-5.4",
                    "provider": "codex",
                    "chat_id": 123,
                    "message_thread_id": None,
                    "topic_label": "Worklog auto capture",
                    "topic_started_at": "2026-03-07T10:00:00+00:00",
                    "last_activity_at": datetime.now(timezone.utc).isoformat(),
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    commit_rc = worklog_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "auto-record-commit",
            "--repo-path",
            str(repo_dir),
            "--commit",
            "HEAD",
        ]
    )
    assert commit_rc == 0
    output = capsys.readouterr().out
    assert '"status": "ok"' in output
    assert '"resolved_scope": "123:main"' in output

    list_rc = worklog_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "list",
            "--chat-id",
            "123",
            "--limit",
            "2",
        ]
    )
    assert list_rc == 0
    listed = capsys.readouterr().out
    assert "Auto recorded commit" in listed
    assert "tracked.txt" in listed


def test_worklog_tool_auto_record_uses_env_scope_over_global_sessions(tmp_path: Path, capsys, monkeypatch) -> None:
    memory_dir = tmp_path / "memory"
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    subprocess.run(["git", "-C", str(repo_dir), "init"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.name", "Test User"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.email", "test@example.com"], check=True)
    tracked = repo_dir / "tracked.txt"
    tracked.write_text("alpha\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_dir), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", "Initial commit"], check=True)
    tracked.write_text("alpha\nbeta\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo_dir), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "-m", "Env scoped commit"], check=True)

    monkeypatch.setenv("ILA_WORKLOG_CHAT_ID", "555")
    monkeypatch.setenv("ILA_WORKLOG_SCOPE_KEY", "555:77")
    monkeypatch.setenv("ILA_WORKLOG_MESSAGE_THREAD_ID", "77")
    monkeypatch.setenv("ILA_WORKLOG_PROVIDER", "codex")
    monkeypatch.setenv("ILA_WORKLOG_SESSION_TYPE", "codex")
    monkeypatch.setenv("ILA_WORKLOG_SESSION_ID", "env-sess-1")
    monkeypatch.setenv("ILA_WORKLOG_TOPIC_LABEL", "Parallel topic")
    monkeypatch.setenv("ILA_WORKLOG_TOPIC_STARTED_AT", "2026-03-07T12:00:00+00:00")

    commit_rc = worklog_tool_main(
        [
            "--memory-dir",
            str(memory_dir),
            "auto-record-commit",
            "--repo-path",
            str(repo_dir),
            "--commit",
            "HEAD",
        ]
    )
    assert commit_rc == 0
    output = capsys.readouterr().out
    assert '"resolved_scope": "555:77"' in output

    manager = MemoryManager(memory_dir)
    items = manager.list_worklog_links(chat_id=555, limit=3)
    assert len(items) == 1
    assert items[0]["scope_key"] == "555:77"
    assert items[0]["session_id"] == "env-sess-1"
    assert items[0]["commits"][0]["subject"] == "Env scoped commit"
