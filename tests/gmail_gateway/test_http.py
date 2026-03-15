from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from src.gmail_gateway.http import AUTH_STORE_KEY, GMAIL_API_KEY, MESSAGE_STORE_KEY, create_app


async def _client(tmp_path: Path, *, gmail_api=None):
    app = create_app(db_path=tmp_path / "gateway.db")
    if gmail_api is not None:
        app[GMAIL_API_KEY] = gmail_api
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    return app, server, client


class _FakeGmailApi:
    def __init__(self) -> None:
        self.calls = 0
        self.messages: dict[str, dict] = {
            "msg-1": {
                "message_id": "msg-1",
                "thread_id": "thr-1",
                "subject": "Invoice April",
                "from": "billing@example.com",
                "snippet": "Invoice attached",
                "internal_date": "1710000000",
                "body_text": "Full invoice body",
                "body_html": None,
                "labels": ["INBOX"],
            }
        }

    async def send_message(self, *, access_token: str, to: list[str], subject: str, body_text: str) -> str:
        self.calls += 1
        assert access_token
        assert to
        assert subject
        assert body_text
        return "gmail-msg-1"

    async def search_messages(self, *, access_token: str, query: str, max_results: int) -> list[dict]:
        assert access_token
        return [
            msg
            for msg in self.messages.values()
            if query.lower() in msg["subject"].lower() or query.lower() in msg["snippet"].lower()
        ][:max_results]

    async def read_message(self, *, access_token: str, message_id: str) -> dict:
        assert access_token
        if message_id not in self.messages:
            from src.gmail_gateway.gmail_api import GmailApiError

            raise GmailApiError(status=404, reason="notFound", message="Message not found", retryable=False)
        return self.messages[message_id]

    async def trash_message(self, *, access_token: str, message_id: str) -> None:
        if message_id not in self.messages:
            from src.gmail_gateway.gmail_api import GmailApiError

            raise GmailApiError(status=404, reason="notFound", message="Message not found", retryable=False)
        labels = self.messages[message_id]["labels"]
        if "TRASH" not in labels:
            labels.append("TRASH")

    async def delete_message(self, *, access_token: str, message_id: str) -> None:
        if message_id not in self.messages:
            from src.gmail_gateway.gmail_api import GmailApiError

            raise GmailApiError(status=404, reason="notFound", message="Message not found", retryable=False)
        labels = self.messages[message_id]["labels"]
        if "DELETED" not in labels:
            labels.append("DELETED")


def _connect_account_for_send(app) -> None:
    store = app[AUTH_STORE_KEY]
    store.upsert_account(account_id="acc-1", gmail_email="alex@example.com")
    store.upsert_token(
        token_id="tok-1",
        account_id="acc-1",
        access_token_ciphertext=b"access-token",
        refresh_token_ciphertext=b"refresh-token",
        scopes="gmail.send",
        kms_key_version="kms-v1",
        expires_at=None,
    )


async def test_health_endpoint_returns_ok(tmp_path: Path) -> None:
    _, server, client = await _client(tmp_path)
    try:
        resp = await client.get("/health")
        payload = await resp.json()
        assert resp.status == 200
        assert payload["ok"] is True
    finally:
        await client.close()
        await server.close()


async def test_get_account_returns_not_found_for_unknown_account(tmp_path: Path) -> None:
    _, server, client = await _client(tmp_path)
    try:
        resp = await client.get("/v1/accounts/missing")
        payload = await resp.json()
        assert resp.status == 404
        assert payload["error"]["code"] == "not_found"
    finally:
        await client.close()
        await server.close()


async def test_connect_callback_and_disconnect_flow(tmp_path: Path) -> None:
    _, server, client = await _client(tmp_path)
    try:
        connect = await client.post(
            "/v1/accounts/acc-1/connect",
            json={"redirect_url": "https://app.example.com/connected"},
        )
        connect_payload = await connect.json()
        assert connect.status == 202
        assert "session_id=" in connect_payload["connect_url"]
        session_id = connect_payload["connect_url"].split("session_id=")[-1]

        callback = await client.post(
            "/v1/oauth/callback",
            json={
                "session_id": session_id,
                "gmail_email": "alex@example.com",
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "scopes": "gmail.readonly gmail.send",
            },
        )
        callback_payload = await callback.json()
        assert callback.status == 200
        assert callback_payload["auth_state"] == "connected"
        assert callback_payload["email"] == "alex@example.com"

        account = await client.get("/v1/accounts/acc-1")
        account_payload = await account.json()
        assert account.status == 200
        assert account_payload["status"] == "active"

        disconnect = await client.post("/v1/accounts/acc-1/disconnect")
        assert disconnect.status == 204

        account_after = await client.get("/v1/accounts/acc-1")
        payload_after = await account_after.json()
        assert payload_after["status"] == "disabled"
        assert payload_after["auth_state"] == "revoked"
    finally:
        await client.close()
        await server.close()


async def test_send_is_idempotent_for_same_payload(tmp_path: Path) -> None:
    fake_api = _FakeGmailApi()
    app, server, client = await _client(tmp_path, gmail_api=fake_api)
    _connect_account_for_send(app)
    payload = {
        "account_id": "acc-1",
        "to": ["bob@example.com"],
        "subject": "Hello",
        "body_text": "Test body",
    }
    headers = {"Idempotency-Key": "idem-12345"}
    try:
        first = await client.post("/v1/messages/send", json=payload, headers=headers)
        first_payload = await first.json()
        second = await client.post("/v1/messages/send", json=payload, headers=headers)
        second_payload = await second.json()

        assert first.status == 202
        assert second.status == 202
        assert first_payload["receipt_id"] == second_payload["receipt_id"]
        assert first_payload["status"] == "sent"
        assert fake_api.calls == 1
    finally:
        await client.close()
        await server.close()


async def test_send_rejects_same_idempotency_key_with_different_payload(tmp_path: Path) -> None:
    app, server, client = await _client(tmp_path, gmail_api=_FakeGmailApi())
    _connect_account_for_send(app)
    headers = {"Idempotency-Key": "idem-12345"}
    try:
        first = await client.post(
            "/v1/messages/send",
            json={
                "account_id": "acc-1",
                "to": ["bob@example.com"],
                "subject": "Hello",
                "body_text": "Body A",
            },
            headers=headers,
        )
        conflict = await client.post(
            "/v1/messages/send",
            json={
                "account_id": "acc-1",
                "to": ["bob@example.com"],
                "subject": "Hello",
                "body_text": "Body B",
            },
            headers=headers,
        )
        payload = await conflict.json()
        assert first.status == 202
        assert conflict.status == 409
        assert payload["error"]["class"] == "idempotency.conflict"
    finally:
        await client.close()
        await server.close()


async def test_send_refreshes_access_token_on_401_and_retries(tmp_path: Path, monkeypatch) -> None:
    from src import config
    from src.gmail_gateway.gmail_api import GmailApiError

    monkeypatch.setattr(config, "GMAIL_GATEWAY_GOOGLE_CLIENT_ID", "client-id")
    monkeypatch.setattr(config, "GMAIL_GATEWAY_GOOGLE_CLIENT_SECRET", "client-secret")

    class _RefreshingApi(_FakeGmailApi):
        def __init__(self) -> None:
            super().__init__()
            self.send_attempts = 0

        async def send_message(self, *, access_token: str, to: list[str], subject: str, body_text: str) -> str:
            self.send_attempts += 1
            if self.send_attempts == 1:
                raise GmailApiError(
                    status=401,
                    reason="invalidCredentials",
                    message="Expired access token",
                    retryable=False,
                )
            assert access_token == "new-access-token"
            return "gmail-msg-refreshed"

        async def refresh_access_token(
            self,
            *,
            refresh_token: str,
            client_id: str,
            client_secret: str,
        ) -> tuple[str, str | None]:
            assert refresh_token == "refresh-token"
            assert client_id == "client-id"
            assert client_secret == "client-secret"
            return "new-access-token", None

    api = _RefreshingApi()
    app, server, client = await _client(tmp_path, gmail_api=api)
    _connect_account_for_send(app)
    try:
        resp = await client.post(
            "/v1/messages/send",
            json={
                "account_id": "acc-1",
                "to": ["bob@example.com"],
                "subject": "Hello",
                "body_text": "Refresh me",
            },
            headers={"Idempotency-Key": "idem-refresh-1"},
        )
        payload = await resp.json()
        assert resp.status == 202
        assert payload["status"] == "sent"
        assert api.send_attempts == 2
        assert app[AUTH_STORE_KEY].get_active_access_token(account_id="acc-1") == "new-access-token"
    finally:
        await client.close()
        await server.close()


async def test_read_search_and_trash_message_flow(tmp_path: Path) -> None:
    fake_api = _FakeGmailApi()
    app, server, client = await _client(tmp_path, gmail_api=fake_api)
    _connect_account_for_send(app)
    try:
        search = await client.post(
            "/v1/messages/search",
            json={"account_id": "acc-1", "query": "Invoice"},
        )
        search_payload = await search.json()
        assert search.status == 200
        assert len(search_payload["messages"]) == 1
        assert search_payload["messages"][0]["message_id"] == "msg-1"

        read = await client.get("/v1/messages/msg-1", headers={"X-Account-Id": "acc-1"})
        read_payload = await read.json()
        assert read.status == 200
        assert read_payload["body_text"] == "Full invoice body"

        trash = await client.post("/v1/messages/msg-1/trash", headers={"X-Account-Id": "acc-1"})
        assert trash.status == 202

        read_after = await client.get("/v1/messages/msg-1", headers={"X-Account-Id": "acc-1"})
        after_payload = await read_after.json()
        assert "TRASH" in after_payload["labels"]
    finally:
        await client.close()
        await server.close()


async def test_sync_bootstrap_delta_and_watch_renew(tmp_path: Path) -> None:
    app, server, client = await _client(tmp_path)
    app[AUTH_STORE_KEY].upsert_account(account_id="acc-sync", gmail_email="sync@example.com")
    try:
        bootstrap = await client.post("/v1/sync/acc-sync/bootstrap")
        bootstrap_payload = await bootstrap.json()
        assert bootstrap.status == 202
        assert bootstrap_payload["sync_state"] == "bootstrap_running"

        delta = await client.post(
            "/v1/sync/acc-sync/delta",
            json={"history_id": "1234567890"},
        )
        delta_payload = await delta.json()
        assert delta.status == 202
        assert delta_payload["sync_state"] == "idle"
        assert delta_payload["last_history_id"] == "1234567890"

        renew = await client.post(
            "/v1/watch/acc-sync/renew",
            json={"watch_expiration_ts": "2026-03-20T00:00:00Z"},
        )
        renew_payload = await renew.json()
        assert renew.status == 202
        assert renew_payload["watch_expiration_ts"] == "2026-03-20T00:00:00Z"
    finally:
        await client.close()
        await server.close()


async def test_internal_metrics_collects_route_status_counters(tmp_path: Path) -> None:
    _, server, client = await _client(tmp_path)
    try:
        ok = await client.get("/health")
        assert ok.status == 200
        missing = await client.get("/v1/accounts/missing")
        assert missing.status == 404

        metrics = await client.get("/internal/metrics")
        payload = await metrics.json()
        counters = payload["counters"]

        assert counters["GET /health 2xx"] >= 1
        assert counters["GET /v1/accounts/missing 4xx"] >= 1
    finally:
        await client.close()
        await server.close()
