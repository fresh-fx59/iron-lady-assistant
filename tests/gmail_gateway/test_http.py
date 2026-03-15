from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from src.gmail_gateway.http import AUTH_STORE_KEY, MESSAGE_STORE_KEY, create_app


async def _client(tmp_path: Path):
    app = create_app(db_path=tmp_path / "gateway.db")
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    return app, server, client


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
    _, server, client = await _client(tmp_path)
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
    finally:
        await client.close()
        await server.close()


async def test_send_rejects_same_idempotency_key_with_different_payload(tmp_path: Path) -> None:
    _, server, client = await _client(tmp_path)
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


async def test_read_search_and_trash_message_flow(tmp_path: Path) -> None:
    app, server, client = await _client(tmp_path)
    app[MESSAGE_STORE_KEY].upsert_message(
        account_id="acc-1",
        message_id="msg-1",
        thread_id="thr-1",
        subject="Invoice April",
        from_email="billing@example.com",
        snippet="Invoice attached",
        body_text="Full invoice body",
        body_html=None,
        labels=["INBOX"],
    )
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
