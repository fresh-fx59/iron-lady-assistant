from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from aiohttp import web

from . import config

logger = logging.getLogger(__name__)

ALLOWED_ROLES = {"system", "user", "assistant", "developer", "tool"}

SEMAPHORE_KEY: web.AppKey[asyncio.Semaphore] = web.AppKey("codex_proxy_semaphore", asyncio.Semaphore)
RUNNER_KEY: web.AppKey[Callable[..., Awaitable["CodexRunResult"]]] = web.AppKey("codex_proxy_runner", object)


@dataclass(frozen=True)
class ChatRequest:
    model: str
    messages: list[dict[str, str]]
    temperature: float | None
    max_tokens: int | None


@dataclass(frozen=True)
class CodexRunResult:
    text: str
    duration_ms: float


class ProxyHttpError(Exception):
    def __init__(self, *, status: int, err_type: str, err_code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.err_type = err_type
        self.err_code = err_code
        self.message = message


class CodexTimeoutError(Exception):
    pass


class CodexExecutionError(Exception):
    pass


def _error_response(*, status: int, err_type: str, err_code: str, message: str) -> web.Response:
    return web.json_response(
        {
            "error": {
                "message": message,
                "type": err_type,
                "code": err_code,
            }
        },
        status=status,
    )


def _request_id(request: web.Request) -> str:
    incoming = request.headers.get("X-Request-ID", "").strip()
    if incoming:
        return incoming[:128]
    return f"req_{uuid.uuid4().hex}"


def _require_auth(request: web.Request) -> None:
    expected = config.CODEX_PROXY_API_KEY
    if not expected:
        raise ProxyHttpError(
            status=500,
            err_type="server_error",
            err_code="proxy_not_configured",
            message="CODEX_PROXY_API_KEY is not configured.",
        )
    provided = request.headers.get("Authorization", "").strip()
    if provided != f"Bearer {expected}":
        raise ProxyHttpError(
            status=401,
            err_type="invalid_api_key",
            err_code="invalid_api_key",
            message="Invalid API key.",
        )


def _parse_messages(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_messages",
            message="messages must be a non-empty array.",
        )

    parsed: list[dict[str, str]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise ProxyHttpError(
                status=400,
                err_type="invalid_request_error",
                err_code="invalid_messages",
                message=f"messages[{idx}] must be an object.",
            )

        role_raw = item.get("role")
        if role_raw not in ALLOWED_ROLES:
            raise ProxyHttpError(
                status=400,
                err_type="invalid_request_error",
                err_code="invalid_role",
                message=f"messages[{idx}].role must be one of {sorted(ALLOWED_ROLES)}.",
            )
        role = "system" if role_raw == "developer" else ("user" if role_raw == "tool" else role_raw)

        content = item.get("content")
        text_content: str | None = None
        if isinstance(content, str) and content.strip():
            text_content = content.strip()
        elif isinstance(content, list):
            # OpenAI clients may send block content: [{"type":"text","text":"..."}].
            parts: list[str] = []
            for block in content:
                if not isinstance(block, dict):
                    continue
                maybe_text = block.get("text")
                if isinstance(maybe_text, str) and maybe_text.strip():
                    parts.append(maybe_text.strip())
            if parts:
                text_content = "\n".join(parts)

        if not text_content:
            raise ProxyHttpError(
                status=400,
                err_type="invalid_request_error",
                err_code="invalid_content",
                message=f"messages[{idx}].content must contain non-empty text.",
            )

        parsed.append({"role": role, "content": text_content})

    return parsed


def _parse_chat_request(payload: Any) -> ChatRequest:
    if not isinstance(payload, dict):
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_json",
            message="Request body must be a JSON object.",
        )

    stream = payload.get("stream")
    if stream is True:
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="stream_not_supported",
            message="stream=true is not supported in MVP.",
        )
    if stream not in (None, False):
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_stream",
            message="stream must be boolean when provided.",
        )

    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_model",
            message="model must be a non-empty string.",
        )
    model = model.strip()
    if model != config.CODEX_PROXY_MODEL_ALIAS:
        raise ProxyHttpError(
            status=404,
            err_type="invalid_request_error",
            err_code="model_not_found",
            message=f"Model '{model}' not found.",
        )

    temperature: float | None = None
    if "temperature" in payload:
        value = payload["temperature"]
        if not isinstance(value, (float, int)):
            raise ProxyHttpError(
                status=400,
                err_type="invalid_request_error",
                err_code="invalid_temperature",
                message="temperature must be a number.",
            )
        temperature = float(value)

    max_tokens: int | None = None
    if "max_tokens" in payload:
        value = payload["max_tokens"]
        if not isinstance(value, int) or value <= 0:
            raise ProxyHttpError(
                status=400,
                err_type="invalid_request_error",
                err_code="invalid_max_tokens",
                message="max_tokens must be a positive integer.",
            )
        max_tokens = value

    messages = _parse_messages(payload.get("messages"))
    return ChatRequest(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )


def _extract_response_input_messages(value: Any) -> list[dict[str, str]]:
    if isinstance(value, str) and value.strip():
        return [{"role": "user", "content": value.strip()}]

    if not isinstance(value, list) or not value:
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_input",
            message="input must be a non-empty string or array.",
        )

    messages: list[dict[str, str]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "user").strip().lower()
        if role not in ALLOWED_ROLES:
            role = "user"

        content = item.get("content")
        if isinstance(content, str) and content.strip():
            messages.append({"role": role, "content": content.strip()})
            continue

        if isinstance(content, list):
            parts: list[str] = []
            for chunk in content:
                if not isinstance(chunk, dict):
                    continue
                text = chunk.get("text")
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
            if parts:
                messages.append({"role": role, "content": "\n".join(parts)})
                continue

        item_type = str(item.get("type") or "").strip().lower()
        if item_type == "message":
            alt_content = item.get("content")
            if isinstance(alt_content, str) and alt_content.strip():
                messages.append({"role": role, "content": alt_content.strip()})

    if not messages:
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_input",
            message="input did not contain any usable text messages.",
        )
    return messages


def _parse_responses_request(payload: Any) -> ChatRequest:
    if not isinstance(payload, dict):
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_json",
            message="Request body must be a JSON object.",
        )

    stream = payload.get("stream")
    if stream is True:
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="stream_not_supported",
            message="stream=true is not supported in MVP.",
        )
    if stream not in (None, False):
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_stream",
            message="stream must be boolean when provided.",
        )

    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_model",
            message="model must be a non-empty string.",
        )
    model = model.strip()
    if model != config.CODEX_PROXY_MODEL_ALIAS:
        raise ProxyHttpError(
            status=404,
            err_type="invalid_request_error",
            err_code="model_not_found",
            message=f"Model '{model}' not found.",
        )

    temperature: float | None = None
    if "temperature" in payload:
        value = payload["temperature"]
        if not isinstance(value, (float, int)):
            raise ProxyHttpError(
                status=400,
                err_type="invalid_request_error",
                err_code="invalid_temperature",
                message="temperature must be a number.",
            )
        temperature = float(value)

    max_tokens: int | None = None
    if "max_output_tokens" in payload:
        value = payload["max_output_tokens"]
        if not isinstance(value, int) or value <= 0:
            raise ProxyHttpError(
                status=400,
                err_type="invalid_request_error",
                err_code="invalid_max_output_tokens",
                message="max_output_tokens must be a positive integer.",
            )
        max_tokens = value

    messages = _extract_response_input_messages(payload.get("input"))
    instructions = payload.get("instructions")
    if isinstance(instructions, str) and instructions.strip():
        messages = [{"role": "system", "content": instructions.strip()}, *messages]

    return ChatRequest(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )


def _build_prompt(messages: list[dict[str, str]], *, temperature: float | None, max_tokens: int | None) -> str:
    system_lines: list[str] = []
    transcript_lines: list[str] = []

    for message in messages:
        role = message["role"]
        content = message["content"]
        if role == "system":
            system_lines.append(content)
        else:
            transcript_lines.append(f"{role.upper()}:\n{content}")

    system_block = "\n\n".join(system_lines) if system_lines else "(none)"
    transcript_block = "\n\n---\n\n".join(transcript_lines)
    controls = {
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    controls_block = json.dumps(controls, separators=(",", ":"), ensure_ascii=False)

    return (
        "[SYSTEM INSTRUCTIONS]\n"
        f"{system_block}\n\n"
        "[CONVERSATION]\n"
        f"{transcript_block}\n\n"
        "[GENERATION CONTROLS]\n"
        f"{controls_block}"
    )


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    return env


async def _run_codex(
    *,
    prompt: str,
    cli_name: str,
    working_dir: str,
    timeout_seconds: float,
    max_output_bytes: int,
) -> CodexRunResult:
    cmd = [
        cli_name,
        "exec",
        "--json",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        prompt,
    ]

    start = time.monotonic()
    proc: asyncio.subprocess.Process | None = None

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=working_dir,
            env=_subprocess_env(),
            limit=max_output_bytes,
        )
    except FileNotFoundError as exc:
        raise CodexExecutionError(f"Provider CLI '{cli_name}' is not installed or not in PATH.") from exc

    async def _collect_output() -> CodexRunResult:
        assert proc is not None  # nosec B101
        if proc.stdout is None or proc.stderr is None:  # pragma: no cover
            raise CodexExecutionError("Codex subprocess streams are unavailable.")

        total_output_bytes = 0
        last_message: str | None = None
        error_text: str | None = None

        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            total_output_bytes += len(line)
            if total_output_bytes > max_output_bytes:
                raise CodexExecutionError("Codex output exceeded max size limit.")

            line_str = line.decode("utf-8", errors="replace").strip()
            if not line_str:
                continue

            try:
                payload = json.loads(line_str)
            except json.JSONDecodeError:
                continue

            event_type = payload.get("type")
            item = payload.get("item", {})

            if event_type in {"item.started", "item.completed"}:
                item_type = item.get("type")
                if item_type in {"assistant_message", "agent_message"}:
                    text = item.get("text") or item.get("message")
                    if isinstance(text, str) and text.strip():
                        last_message = text.strip()
            elif event_type == "error":
                maybe_error = payload.get("message") or payload.get("error", {}).get("message")
                if isinstance(maybe_error, str) and maybe_error.strip():
                    error_text = maybe_error.strip()

        stderr_bytes = await proc.stderr.read()
        total_output_bytes += len(stderr_bytes)
        stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()

        await proc.wait()
        elapsed_ms = (time.monotonic() - start) * 1000

        if error_text:
            raise CodexExecutionError(error_text)

        if proc.returncode != 0:
            raise CodexExecutionError(stderr_text or f"Codex exited with code {proc.returncode}.")

        if not last_message:
            raise CodexExecutionError("Codex exited without an assistant response.")

        return CodexRunResult(text=last_message, duration_ms=elapsed_ms)

    try:
        return await asyncio.wait_for(_collect_output(), timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        if proc is not None and proc.returncode is None:
            proc.kill()
            await proc.wait()
        raise CodexTimeoutError("Codex execution timed out.") from exc
    except CodexExecutionError:
        if proc is not None and proc.returncode is None:
            proc.kill()
            await proc.wait()
        raise


def _estimate_usage(prompt: str, completion: str) -> dict[str, int]:
    prompt_tokens = max(1, len(prompt) // 4)
    completion_tokens = max(1, len(completion) // 4)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
    }


async def _health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _ready(_: web.Request) -> web.Response:
    workdir = Path(config.CODEX_PROXY_WORKDIR)
    if not config.CODEX_PROXY_WORKDIR:
        return _error_response(
            status=500,
            err_type="server_error",
            err_code="proxy_not_configured",
            message="CODEX_PROXY_WORKDIR is not configured.",
        )
    if not workdir.is_absolute():
        return _error_response(
            status=500,
            err_type="server_error",
            err_code="proxy_not_configured",
            message="CODEX_PROXY_WORKDIR must be an absolute path.",
        )
    if not workdir.exists() or not workdir.is_dir():
        return _error_response(
            status=500,
            err_type="server_error",
            err_code="workdir_missing",
            message="CODEX_PROXY_WORKDIR does not exist or is not a directory.",
        )
    if not os.access(workdir, os.W_OK):
        return _error_response(
            status=500,
            err_type="server_error",
            err_code="workdir_not_writable",
            message="CODEX_PROXY_WORKDIR is not writable.",
        )
    return web.json_response({"ok": True})


async def _chat_completions(request: web.Request) -> web.Response:
    req_id = _request_id(request)
    _require_auth(request)

    try:
        payload = await request.json()
    except json.JSONDecodeError as exc:
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_json",
            message="Request body must be valid JSON.",
        ) from exc

    parsed = _parse_chat_request(payload)

    semaphore = request.app[SEMAPHORE_KEY]
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=0.001)
    except asyncio.TimeoutError as exc:
        raise ProxyHttpError(
            status=429,
            err_type="rate_limit_exceeded",
            err_code="rate_limit_exceeded",
            message="Too many in-flight requests.",
        ) from exc

    try:
        prompt = _build_prompt(
            parsed.messages,
            temperature=parsed.temperature,
            max_tokens=parsed.max_tokens,
        )

        runner = request.app[RUNNER_KEY]
        result = await runner(
            prompt=prompt,
            cli_name=config.CODEX_PROXY_CLI_NAME,
            working_dir=config.CODEX_PROXY_WORKDIR,
            timeout_seconds=config.CODEX_PROXY_TIMEOUT_SECONDS,
            max_output_bytes=config.CODEX_PROXY_MAX_OUTPUT_BYTES,
        )

        completion_id = f"chatcmpl_{uuid.uuid4().hex}"
        created = int(time.time())
        usage = _estimate_usage(prompt, result.text)

        response = {
            "id": completion_id,
            "object": "chat.completion",
            "created": created,
            "model": parsed.model,
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {
                        "role": "assistant",
                        "content": result.text,
                    },
                }
            ],
            "usage": usage,
        }
        return web.json_response(response, headers={"X-Request-ID": req_id})
    finally:
        semaphore.release()


def _responses_success_payload(*, parsed: ChatRequest, prompt: str, result: CodexRunResult) -> dict[str, Any]:
    response_id = f"resp_{uuid.uuid4().hex}"
    output_id = f"msg_{uuid.uuid4().hex}"
    usage = _estimate_usage(prompt, result.text)
    return {
        "id": response_id,
        "object": "response",
        "created_at": int(time.time()),
        "status": "completed",
        "model": parsed.model,
        "output": [
            {
                "id": output_id,
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": result.text,
                        "annotations": [],
                    }
                ],
            }
        ],
        "output_text": result.text,
        "usage": {
            "input_tokens": usage["prompt_tokens"],
            "output_tokens": usage["completion_tokens"],
            "total_tokens": usage["total_tokens"],
        },
    }


async def _responses(request: web.Request) -> web.Response:
    req_id = _request_id(request)
    _require_auth(request)

    try:
        payload = await request.json()
    except json.JSONDecodeError as exc:
        raise ProxyHttpError(
            status=400,
            err_type="invalid_request_error",
            err_code="invalid_json",
            message="Request body must be valid JSON.",
        ) from exc

    parsed = _parse_responses_request(payload)
    semaphore = request.app[SEMAPHORE_KEY]
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=0.001)
    except asyncio.TimeoutError as exc:
        raise ProxyHttpError(
            status=429,
            err_type="rate_limit_exceeded",
            err_code="rate_limit_exceeded",
            message="Too many in-flight requests.",
        ) from exc

    try:
        prompt = _build_prompt(
            parsed.messages,
            temperature=parsed.temperature,
            max_tokens=parsed.max_tokens,
        )
        runner = request.app[RUNNER_KEY]
        result = await runner(
            prompt=prompt,
            cli_name=config.CODEX_PROXY_CLI_NAME,
            working_dir=config.CODEX_PROXY_WORKDIR,
            timeout_seconds=config.CODEX_PROXY_TIMEOUT_SECONDS,
            max_output_bytes=config.CODEX_PROXY_MAX_OUTPUT_BYTES,
        )
        response = _responses_success_payload(parsed=parsed, prompt=prompt, result=result)
        return web.json_response(response, headers={"X-Request-ID": req_id})
    finally:
        semaphore.release()


@web.middleware
async def _error_middleware(request: web.Request, handler):
    try:
        return await handler(request)
    except ProxyHttpError as exc:
        return _error_response(
            status=exc.status,
            err_type=exc.err_type,
            err_code=exc.err_code,
            message=exc.message,
        )
    except CodexTimeoutError as exc:
        return _error_response(
            status=504,
            err_type="timeout_error",
            err_code="gateway_timeout",
            message=str(exc),
        )
    except CodexExecutionError as exc:
        return _error_response(
            status=500,
            err_type="server_error",
            err_code="server_error",
            message=str(exc),
        )
    except Exception:
        logger.exception("Unhandled codex proxy error")
        return _error_response(
            status=500,
            err_type="server_error",
            err_code="server_error",
            message="Unexpected server error.",
        )


def _validate_startup_config() -> None:
    workdir = config.CODEX_PROXY_WORKDIR
    if not workdir:
        raise RuntimeError("CODEX_PROXY_WORKDIR is required.")
    path = Path(workdir)
    if not path.is_absolute():
        raise RuntimeError("CODEX_PROXY_WORKDIR must be an absolute path.")
    path.mkdir(parents=True, exist_ok=True)


def create_app() -> web.Application:
    _validate_startup_config()
    app = web.Application(middlewares=[_error_middleware])
    app[SEMAPHORE_KEY] = asyncio.Semaphore(config.CODEX_PROXY_MAX_INFLIGHT)
    app[RUNNER_KEY] = _run_codex

    app.router.add_get("/health", _health)
    app.router.add_get("/ready", _ready)
    app.router.add_post("/v1/chat/completions", _chat_completions)
    app.router.add_post("/v1/responses", _responses)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    web.run_app(
        create_app(),
        host=config.CODEX_PROXY_BIND_HOST,
        port=config.CODEX_PROXY_BIND_PORT,
    )


if __name__ == "__main__":
    main()
