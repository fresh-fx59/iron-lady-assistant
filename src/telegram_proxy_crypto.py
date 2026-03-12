from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from . import config


class TelegramProxyCryptoError(RuntimeError):
    pass


@dataclass(frozen=True)
class TelegramProxyCredentials:
    api_id: int
    api_hash: str
    session_string: str | None
    session_path: str | None


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def load_decryption_key() -> str:
    credentials_dir = os.getenv("CREDENTIALS_DIRECTORY", "").strip()
    if credentials_dir:
        candidate = Path(credentials_dir) / config.TELEGRAM_PROXY_KEY_CREDENTIAL_NAME
        if candidate.is_file():
            return _read_text(candidate)
    fallback = config.TELEGRAM_PROXY_KEY_FALLBACK_PATH
    if fallback and fallback.is_file():
        return _read_text(fallback)
    raise TelegramProxyCryptoError(
        "Telegram proxy decryption key was not found in CREDENTIALS_DIRECTORY or fallback path."
    )


def decrypt_credentials(encrypted_blob: str, key: str) -> TelegramProxyCredentials:
    if not encrypted_blob:
        raise TelegramProxyCryptoError("TELEGRAM_PROXY_ENCRYPTED_CREDENTIALS is empty.")
    try:
        payload = Fernet(key.encode("utf-8")).decrypt(encrypted_blob.encode("utf-8"))
    except (ValueError, InvalidToken) as exc:
        raise TelegramProxyCryptoError("Failed to decrypt Telegram proxy credentials.") from exc
    try:
        data = json.loads(payload.decode("utf-8"))
        api_id = int(data["api_id"])
        api_hash = str(data["api_hash"]).strip()
        session_string = str(data["session_string"]).strip() if data.get("session_string") else None
        session_path = str(data["session_path"]).strip() if data.get("session_path") else None
    except Exception as exc:  # pragma: no cover - malformed payload
        raise TelegramProxyCryptoError("Telegram proxy credential payload is invalid.") from exc
    if not api_id or not api_hash:
        raise TelegramProxyCryptoError("Telegram proxy credential payload is missing api_id/api_hash.")
    return TelegramProxyCredentials(
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        session_path=session_path,
    )

