from cryptography.fernet import Fernet

from src.telegram_proxy_crypto import decrypt_credentials


def test_decrypt_credentials_returns_payload_fields() -> None:
    key = Fernet.generate_key().decode("utf-8")
    token = Fernet(key.encode("utf-8")).encrypt(
        b'{"api_id":12345,"api_hash":"hash123","session_string":"sessionabc"}'
    ).decode("utf-8")

    creds = decrypt_credentials(token, key)

    assert creds.api_id == 12345
    assert creds.api_hash == "hash123"
    assert creds.session_string == "sessionabc"
    assert creds.session_path is None
