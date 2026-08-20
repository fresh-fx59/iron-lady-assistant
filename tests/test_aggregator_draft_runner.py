from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest


SOURCE_RUNNER = Path(__file__).resolve().parents[1] / "scripts" / "aggregator_draft_runner.sh"


class AggregatorDraftRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.tmp = Path(self.tempdir.name)
        self.repo = self.tmp / "repo"
        self.state = self.tmp / "state"
        self.fakebin = self.tmp / "bin"
        (self.repo / "scripts").mkdir(parents=True)
        (self.repo / "venv" / "bin").mkdir(parents=True)
        self.fakebin.mkdir()
        shutil.copy2(SOURCE_RUNNER, self.repo / "scripts" / SOURCE_RUNNER.name)
        self._write_executable(
            self.fakebin / "git",
            """#!/bin/sh
if [ "${FAKE_GIT_FAIL_COMMAND:-}" = "${3:-}" ]; then
  exit 73
fi
exit 0
""",
        )
        self._write_executable(self.fakebin / "flock", "#!/bin/sh\nexit 0\n")
        self._write_executable(
            self.fakebin / "claude",
            """#!/usr/bin/env python3
import os, pathlib, sys
pathlib.Path(os.environ["FAKE_CLAUDE_ARGV"]).write_text("\\n".join(sys.argv[1:]))
print("BLOCKED — usage limit at 98%")
raise SystemExit(0)
""",
        )
        self._write_executable(
            self.repo / "venv" / "bin" / "python",
            """#!/usr/bin/env python3
import json, os
from pathlib import Path
import sys

args = sys.argv[1:]
if args[:3] == ["-m", "src.telegram_aggregator_tool", "render-input"]:
    target = Path(args[args.index("--out") + 1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text('{"items": []}\\n')
elif args[:3] == ["-m", "src.telegram_aggregator_tool", "gate"]:
    count_path = Path(os.environ["FAKE_GATE_COUNT"])
    count = int(count_path.read_text()) + 1 if count_path.exists() else 1
    count_path.write_text(str(count))
    draft = Path(args[args.index("--draft") + 1])
    print(json.dumps({"gate": count, "draft_exists": draft.is_file()}))
    sequence = os.environ.get("FAKE_GATE_SEQUENCE", "pass").split(",")
    mode = sequence[min(count - 1, len(sequence) - 1)]
    raise SystemExit(0 if mode == "pass" and draft.is_file() and draft.stat().st_size else 1)
raise SystemExit(0)
""",
        )
        self.shared_runner = self.fakebin / "headless-runner"
        self._write_executable(
            self.shared_runner,
            """#!/usr/bin/env python3
import json, os
from pathlib import Path
import sys

args = sys.argv[1:]
count_path = Path(os.environ["FAKE_RUNNER_COUNT"])
count = int(count_path.read_text()) + 1 if count_path.exists() else 1
count_path.write_text(str(count))
with Path(os.environ["FAKE_RUNNER_ARGV"]).open("a", encoding="utf-8") as fh:
    fh.write(json.dumps(args) + "\\n")
with Path(os.environ["FAKE_RUNNER_STDIN"]).open("a", encoding="utf-8") as fh:
    fh.write(sys.stdin.read() + "\\n---PROMPT---\\n")
sequence = os.environ.get("FAKE_RUNNER_SEQUENCE", "valid").split(",")
mode = sequence[min(count - 1, len(sequence) - 1)]
artifact = Path(args[args.index("--require-artifact") + 1])
if mode == "valid":
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text('{"title": "fresh"}\\n')
elif mode == "empty":
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("")
elif mode == "blocked":
    print("BLOCKED — usage limit at 98%")
raise SystemExit(0)
""",
        )
        self.runner_argv = self.tmp / "runner-argv.jsonl"
        self.runner_stdin = self.tmp / "runner-stdin.txt"
        self.runner_count = self.tmp / "runner-count"
        self.gate_count = self.tmp / "gate-count"
        self.claude_argv = self.tmp / "claude-argv.txt"

    @staticmethod
    def _write_executable(path: Path, body: str) -> None:
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        path.chmod(0o755)

    def run_script(
        self,
        sequence: str = "valid",
        *,
        gate_sequence: str = "pass",
        provider: str = "codex",
        deploy_ref: str = "",
        git_fail_command: str = "",
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.fakebin}:{env['PATH']}",
                "AGGREGATOR_STATE_DIR": str(self.state),
                "AGGREGATOR_DEPLOY_REF": deploy_ref,
                "HEADLESS_AGENT_PROVIDER": provider,
                "HEADLESS_AGENT_RUNNER": str(self.shared_runner),
                "HEADLESS_AGENT_MODEL": "gpt-5.6-terra",
                "HEADLESS_AGENT_EFFORT": "medium",
                "HEADLESS_AGENT_SANDBOX": "workspace-write",
                "HEADLESS_AGENT_CODEX_BIN": "codex-test",
                "HEADLESS_AGENT_CLAUDE_BIN": "claude-test",
                "FAKE_RUNNER_SEQUENCE": sequence,
                "FAKE_GATE_SEQUENCE": gate_sequence,
                "FAKE_RUNNER_ARGV": str(self.runner_argv),
                "FAKE_RUNNER_STDIN": str(self.runner_stdin),
                "FAKE_RUNNER_COUNT": str(self.runner_count),
                "FAKE_GATE_COUNT": str(self.gate_count),
                "FAKE_CLAUDE_ARGV": str(self.claude_argv),
                "FAKE_GIT_FAIL_COMMAND": git_fail_command,
            }
        )
        return subprocess.run(
            ["bash", str(self.repo / "scripts" / SOURCE_RUNNER.name)],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

    def canonical_draft(self) -> Path:
        matches = list((self.state / "drafts").glob("*-draft.json"))
        self.assertLessEqual(len(matches), 1)
        return matches[0] if matches else self.state / "drafts" / "absent-draft.json"

    def test_zero_exit_blocked_text_without_file_fails_and_never_gates(self) -> None:
        proc = self.run_script("blocked,blocked")
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(self.gate_count.exists())
        self.assertFalse(self.canonical_draft().exists())

    def test_pinned_checkout_failure_exits_without_running_agent_or_gate(self) -> None:
        proc = self.run_script(deploy_ref="reviewed-sha", git_fail_command="checkout")
        self.assertEqual(proc.returncode, 73)
        self.assertFalse(self.runner_count.exists())
        self.assertFalse(self.gate_count.exists())

    def test_preexisting_canonical_draft_cannot_satisfy_attempt(self) -> None:
        draft_dir = self.state / "drafts"
        draft_dir.mkdir(parents=True)
        old = draft_dir / f"{subprocess.check_output(['date', '-u', '+%F'], text=True).strip()}-draft.json"
        old.write_text('{"title": "stale"}\n', encoding="utf-8")
        proc = self.run_script("blocked,blocked")
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(self.gate_count.exists())
        self.assertEqual(json.loads(old.read_text(encoding="utf-8"))["title"], "stale")

    def test_first_missing_artifact_then_valid_artifact_promotes_and_gates(self) -> None:
        proc = self.run_script("blocked,valid")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.runner_count.read_text(encoding="utf-8"), "2")
        self.assertEqual(self.gate_count.read_text(encoding="utf-8"), "1")
        self.assertEqual(json.loads(self.canonical_draft().read_text(encoding="utf-8"))["title"], "fresh")

    def test_transport_retry_does_not_consume_gate_feedback_regeneration(self) -> None:
        proc = self.run_script("blocked,valid,valid", gate_sequence="fail,pass")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.runner_count.read_text(encoding="utf-8"), "3")
        self.assertEqual(self.gate_count.read_text(encoding="utf-8"), "2")
        prompts = self.runner_stdin.read_text(encoding="utf-8")
        self.assertIn("-gate-errors.json", prompts.split("---PROMPT---")[-2])

    def test_valid_first_attempt_promotes_once_and_gates_once(self) -> None:
        proc = self.run_script("valid")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.runner_count.read_text(encoding="utf-8"), "1")
        self.assertEqual(self.gate_count.read_text(encoding="utf-8"), "1")

    def test_prompt_is_stdin_not_process_argument_or_log(self) -> None:
        proc = self.run_script("valid")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        argv_text = self.runner_argv.read_text(encoding="utf-8")
        prompt = self.runner_stdin.read_text(encoding="utf-8")
        self.assertNotIn("Use $aggregator-digest", argv_text)
        self.assertIn("Use $aggregator-digest", prompt)
        log_text = "".join(path.read_text(errors="replace") for path in (self.state / "logs").glob("*"))
        self.assertNotIn("Use $aggregator-digest", log_text)

    def test_claude_rollback_uses_claude_skill_syntax_on_stdin(self) -> None:
        proc = self.run_script("valid", provider="claude")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        argv = json.loads(self.runner_argv.read_text(encoding="utf-8").splitlines()[0])
        prompt = self.runner_stdin.read_text(encoding="utf-8")
        self.assertEqual(argv[argv.index("--provider") + 1], "claude")
        self.assertIn("/aggregator-digest ", prompt)
        self.assertNotIn("Use $aggregator-digest", prompt)

    def test_provider_model_effort_sandbox_and_state_add_dir_are_explicit(self) -> None:
        proc = self.run_script("valid")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        argv = json.loads(self.runner_argv.read_text(encoding="utf-8").splitlines()[0])
        expected_pairs = {
            "--provider": "codex",
            "--cwd": str(self.repo),
            "--model": "gpt-5.6-terra",
            "--effort": "medium",
            "--sandbox": "workspace-write",
            "--add-dir": str(self.state),
            "--codex-bin": "codex-test",
            "--claude-bin": "claude-test",
        }
        for option, value in expected_pairs.items():
            self.assertEqual(argv[argv.index(option) + 1], value)
        self.assertIn("--require-artifact", argv)


if __name__ == "__main__":
    unittest.main()
