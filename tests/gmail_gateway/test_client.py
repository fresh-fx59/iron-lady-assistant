from pathlib import Path

import pytest
from aiohttp.test_utils import TestServer

from src.gmail_gateway.http import MESSAGE_STORE_KEY, create_app
from src.gmail_gateway_client import GatewayClientError, GmailGatewayClient


@pytest.mark.asyncio
async def test_client_connect_callback_send_and_disconnect(tmp_path: Path) -> None:
    app = create_app(db_path=tmp_path / "gateway.db")
    server = TestServer(app)
    await server.start_server()
    try:
        client = GmailGatewayClient(base_url=str(server.make_url("/")).rstrip("/"))

        connect = await client.connect_account(
            account_id="acc-1",
            redirect_url="https://app.example.com/connected",
        )
        session_id = connect["connect_url"].split("session_id=")[-1]

        account = await client.oauth_callback(
            session_id=session_id,
            gmail_email="alex@example.com",
            access_token="access-token",
            refresh_token="refresh-token",
            scopes="gmail.readonly gmail.send",
        )
        assert account["auth_state"] == "connected"

        receipt = await client.send_message(
            account_id="acc-1",
            to=["bob@example.com"],
            subject="Hello",
            body_text="Body",
            idempotency_key="idem-client-1",
        )
        assert receipt["status"] == "queued"

        await client.disconnect_account(account_id="acc-1")

        disconnected = await client.get_account(account_id="acc-1")
        assert disconnected["status"] == "disabled"
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_client_search_read_trash_delete(tmp_path: Path) -> None:
    app = create_app(db_path=tmp_path / "gateway.db")
    app[MESSAGE_STORE_KEY].upsert_message(
        account_id="acc-1",
        message_id="msg-1",
        thread_id="thr-1",
        subject="Project update",
        from_email="alice@example.com",
        snippet="latest update",
        body_text="full body",
        body_html=None,
        labels=["INBOX"],
    )
    server = TestServer(app)
    await server.start_server()
    try:
        client = GmailGatewayClient(base_url=str(server.make_url("/")).rstrip("/"))
        search = await client.search_messages(account_id="acc-1", query="Project", page_size=10)
        assert search["messages"][0]["message_id"] == "msg-1"

        read = await client.read_message(account_id="acc-1", message_id="msg-1")
        assert read["body_text"] == "full body"

        await client.trash_message(account_id="acc-1", message_id="msg-1")
        after_trash = await client.read_message(account_id="acc-1", message_id="msg-1")
        assert "TRASH" in after_trash["labels"]

        await client.delete_message(account_id="acc-1", message_id="msg-1")
        after_delete = await client.read_message(account_id="acc-1", message_id="msg-1")
        assert "DELETED" in after_delete["labels"]
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_client_raises_typed_error(tmp_path: Path) -> None:
    app = create_app(db_path=tmp_path / "gateway.db")
    server = TestServer(app)
    await server.start_server()
    try:
        client = GmailGatewayClient(base_url=str(server.make_url("/")).rstrip("/"))
        with pytest.raises(GatewayClientError) as exc:
            await client.get_account(account_id="missing")

        assert exc.value.status == 404
        assert exc.value.code == "not_found"
    finally:
        await server.close()
