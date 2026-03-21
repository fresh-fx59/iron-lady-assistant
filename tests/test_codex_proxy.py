from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from src import config
from src.codex_proxy import CodexRunResult, CodexTimeoutError, RUNNER_KEY, SEMAPHORE_KEY, create_app


async def _client(monkeypatch, tmp_path: Path, *, runner=None):
    monkeypatch.setattr(config, "CODEX_PROXY_WORKDIR", str(tmp_path / "proxy-work"))
    monkeypatch.setattr(config, "CODEX_PROXY_API_KEY", "proxy-token")
    monkeypatch.setattr(config, "CODEX_PROXY_MODEL_ALIAS", "codex-cli")
    monkeypatch.setattr(config, "CODEX_PROXY_CLI_NAME", "codex")
    monkeypatch.setattr(config, "CODEX_PROXY_TIMEOUT_SECONDS", 5.0)
    monkeypatch.setattr(config, "CODEX_PROXY_MAX_OUTPUT_BYTES", 1024 * 1024)
    monkeypatch.setattr(config, "CODEX_PROXY_MAX_INFLIGHT", 2)

    app = create_app()
    if runner is not None:
        app[RUNNER_KEY] = runner
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    return app, server, client


async def test_health_and_ready(monkeypatch, tmp_path: Path) -> None:
    _, server, client = await _client(monkeypatch, tmp_path)
    try:
        health = await client.get("/health")
        ready = await client.get("/ready")
        assert health.status == 200
        assert ready.status == 200
        assert (await health.json())["ok"] is True
        assert (await ready.json())["ok"] is True
    finally:
        await client.close()
        await server.close()


async def test_models_list(monkeypatch, tmp_path: Path) -> None:
    _, server, client = await _client(monkeypatch, tmp_path)
    try:
        resp = await client.get("/v1/models")
        payload = await resp.json()
        assert resp.status == 200
        model_ids = {m["id"] for m in payload["data"]}
        assert "codex-cli" in model_ids
        assert "openai:codex-cli" in model_ids
    finally:
        await client.close()
        await server.close()


async def test_chat_rejects_invalid_auth(monkeypatch, tmp_path: Path) -> None:
    _, server, client = await _client(monkeypatch, tmp_path)
    try:
        resp = await client.post(
            "/v1/chat/completions",
            json={"model": "codex-cli", "messages": [{"role": "user", "content": "hello"}]},
        )
        payload = await resp.json()
        assert resp.status == 401
        assert payload["error"]["code"] == "invalid_api_key"
    finally:
        await client.close()
        await server.close()


async def test_chat_rejects_stream_true(monkeypatch, tmp_path: Path) -> None:
    _, server, client = await _client(monkeypatch, tmp_path)
    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token"},
            json={
                "model": "codex-cli",
                "stream": True,
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        payload = await resp.json()
        assert resp.status == 400
        assert payload["error"]["code"] == "stream_not_supported"
    finally:
        await client.close()
        await server.close()


async def test_chat_rejects_model_mismatch(monkeypatch, tmp_path: Path) -> None:
    _, server, client = await _client(monkeypatch, tmp_path)
    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token"},
            json={"model": "gpt-4o", "messages": [{"role": "user", "content": "hello"}]},
        )
        payload = await resp.json()
        assert resp.status == 404
        assert payload["error"]["code"] == "model_not_found"
    finally:
        await client.close()
        await server.close()


async def test_chat_success_response_shape(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []

    async def _fake_runner(**kwargs):
        calls.append(kwargs)
        return CodexRunResult(text="Hello from Codex", duration_ms=321.0)
    _, server, client = await _client(monkeypatch, tmp_path, runner=_fake_runner)

    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token", "X-Request-ID": "abc-123"},
            json={
                "model": "codex-cli",
                "messages": [
                    {"role": "system", "content": "be concise"},
                    {"role": "user", "content": "say hi"},
                ],
                "temperature": 0.2,
                "max_tokens": 64,
            },
        )
        payload = await resp.json()

        assert resp.status == 200
        assert payload["object"] == "chat.completion"
        assert payload["model"] == "codex-cli"
        assert payload["choices"][0]["message"]["role"] == "assistant"
        assert payload["choices"][0]["message"]["content"] == "Hello from Codex"
        assert resp.headers["X-Request-ID"] == "abc-123"
        assert payload["usage"]["total_tokens"] >= 2

        assert len(calls) == 1
        assert calls[0]["working_dir"] == str(tmp_path / "proxy-work")
        assert calls[0]["cli_name"] == "codex"
    finally:
        await client.close()
        await server.close()


async def test_chat_accepts_block_content_and_extra_fields(monkeypatch, tmp_path: Path) -> None:
    async def _fake_runner(**kwargs):  # noqa: ARG001
        return CodexRunResult(text="OK", duration_ms=50.0)

    _, server, client = await _client(monkeypatch, tmp_path, runner=_fake_runner)
    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token"},
            json={
                "model": "codex-cli",
                "messages": [
                    {
                        "role": "developer",
                        "content": [{"type": "text", "text": "You are concise."}],
                    },
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "Reply with OK"}],
                    },
                ],
                # Typical extra fields from OpenAI-compatible clients.
                "n": 1,
                "metadata": {"client": "deepagents"},
                "tools": [],
            },
        )
        payload = await resp.json()
        assert resp.status == 200
        assert payload["choices"][0]["message"]["content"] == "OK"
    finally:
        await client.close()
        await server.close()


async def test_chat_maps_timeout_to_504(monkeypatch, tmp_path: Path) -> None:
    async def _timeout_runner(**kwargs):  # noqa: ARG001
        raise CodexTimeoutError("Codex execution timed out.")
    _, server, client = await _client(monkeypatch, tmp_path, runner=_timeout_runner)

    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token"},
            json={"model": "codex-cli", "messages": [{"role": "user", "content": "hello"}]},
        )
        payload = await resp.json()
        assert resp.status == 504
        assert payload["error"]["code"] == "gateway_timeout"
    finally:
        await client.close()
        await server.close()


async def test_chat_returns_429_when_capacity_exhausted(monkeypatch, tmp_path: Path) -> None:
    app, server, client = await _client(monkeypatch, tmp_path)

    semaphore = app[SEMAPHORE_KEY]
    await semaphore.acquire()
    await semaphore.acquire()

    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token"},
            json={"model": "codex-cli", "messages": [{"role": "user", "content": "hello"}]},
        )
        payload = await resp.json()
        assert resp.status == 429
        assert payload["error"]["code"] == "rate_limit_exceeded"
    finally:
        semaphore.release()
        semaphore.release()
        await client.close()
        await server.close()


async def test_responses_success_response_shape(monkeypatch, tmp_path: Path) -> None:
    async def _fake_runner(**kwargs):  # noqa: ARG001
        return CodexRunResult(text="Hello from responses", duration_ms=123.0)

    _, server, client = await _client(monkeypatch, tmp_path, runner=_fake_runner)
    try:
        resp = await client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer proxy-token"},
            json={
                "model": "codex-cli",
                "input": "Say hello",
                "instructions": "Be concise",
                "max_output_tokens": 32,
            },
        )
        payload = await resp.json()
        assert resp.status == 200
        assert payload["object"] == "response"
        assert payload["status"] == "completed"
        assert payload["model"] == "codex-cli"
        assert payload["output"][0]["type"] == "message"
        assert payload["output"][0]["role"] == "assistant"
        assert payload["output"][0]["content"][0]["type"] == "output_text"
        assert payload["output_text"] == "Hello from responses"
        assert payload["usage"]["total_tokens"] >= 2
    finally:
        await client.close()
        await server.close()


async def test_responses_rejects_stream_true(monkeypatch, tmp_path: Path) -> None:
    _, server, client = await _client(monkeypatch, tmp_path)
    try:
        resp = await client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer proxy-token"},
            json={
                "model": "codex-cli",
                "input": "hello",
                "stream": True,
            },
        )
        payload = await resp.json()
        assert resp.status == 400
        assert payload["error"]["code"] == "stream_not_supported"
    finally:
        await client.close()
        await server.close()


async def test_responses_rejects_invalid_input(monkeypatch, tmp_path: Path) -> None:
    _, server, client = await _client(monkeypatch, tmp_path)
    try:
        resp = await client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer proxy-token"},
            json={"model": "codex-cli", "input": []},
        )
        payload = await resp.json()
        assert resp.status == 400
        assert payload["error"]["code"] == "invalid_input"
    finally:
        await client.close()
        await server.close()


async def test_responses_accepts_extra_fields(monkeypatch, tmp_path: Path) -> None:
    async def _fake_runner(**kwargs):  # noqa: ARG001
        return CodexRunResult(text="OK", duration_ms=20.0)

    _, server, client = await _client(monkeypatch, tmp_path, runner=_fake_runner)
    try:
        resp = await client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer proxy-token"},
            json={
                "model": "codex-cli",
                "input": [{"role": "user", "content": [{"type": "input_text", "text": "ping"}]}],
                "metadata": {"client": "deepagents"},
                "store": False,
                "top_p": 1,
            },
        )
        payload = await resp.json()
        assert resp.status == 200
        assert payload["output_text"] == "OK"
    finally:
        await client.close()
        await server.close()


async def test_accepts_provider_prefixed_model_name(monkeypatch, tmp_path: Path) -> None:
    async def _fake_runner(**kwargs):  # noqa: ARG001
        return CodexRunResult(text="OK", duration_ms=10.0)

    _, server, client = await _client(monkeypatch, tmp_path, runner=_fake_runner)
    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token"},
            json={
                "model": "openai:codex-cli",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        payload = await resp.json()
        assert resp.status == 200
        assert payload["choices"][0]["message"]["content"] == "OK"
    finally:
        await client.close()
        await server.close()


async def test_chat_skips_non_text_assistant_toolcall_messages(monkeypatch, tmp_path: Path) -> None:
    async def _fake_runner(**kwargs):  # noqa: ARG001
        return CodexRunResult(text="OK", duration_ms=10.0)

    _, server, client = await _client(monkeypatch, tmp_path, runner=_fake_runner)
    try:
        resp = await client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer proxy-token"},
            json={
                "model": "openai:codex-cli",
                "messages": [
                    {"role": "assistant", "content": ""},
                    {"role": "user", "content": "Return OK"},
                ],
            },
        )
        payload = await resp.json()
        assert resp.status == 200
        assert payload["choices"][0]["message"]["content"] == "OK"
    finally:
        await client.close()
        await server.close()
