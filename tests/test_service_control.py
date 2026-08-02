"""A restart that did not happen must never read as one that did."""

from __future__ import annotations

import pytest

from src import service_control


class _FakeProc:
    def __init__(self, returncode: int, stderr: bytes = b"") -> None:
        self.returncode = returncode
        self._stderr = stderr

    async def communicate(self):
        return b"", self._stderr


@pytest.mark.asyncio
async def test_successful_restart_reports_ok(monkeypatch) -> None:
    async def fake_exec(*args, **kwargs):
        return _FakeProc(0)

    monkeypatch.setattr(service_control.asyncio, "create_subprocess_exec", fake_exec)
    ok, detail = await service_control.restart_unit("iron-lady-bot.service")
    assert ok is True
    assert detail == ""


@pytest.mark.asyncio
async def test_unknown_unit_is_a_failure_naming_the_unit(monkeypatch) -> None:
    """The exact production bug: restarting a unit that does not exist."""

    async def fake_exec(*args, **kwargs):
        return _FakeProc(5, b"Failed to restart telegram-bot.service: Unit not found.")

    monkeypatch.setattr(service_control.asyncio, "create_subprocess_exec", fake_exec)
    ok, detail = await service_control.restart_unit("telegram-bot.service")
    assert ok is False
    assert "telegram-bot.service" in detail
    assert "Unit not found" in detail


@pytest.mark.asyncio
async def test_failure_without_stderr_still_names_the_unit(monkeypatch) -> None:
    async def fake_exec(*args, **kwargs):
        return _FakeProc(1, b"")

    monkeypatch.setattr(service_control.asyncio, "create_subprocess_exec", fake_exec)
    ok, detail = await service_control.restart_unit("my-bot.service")
    assert ok is False
    assert "my-bot.service" in detail
    assert "exit code 1" in detail
