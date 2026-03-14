import pytest

from src.gmail_gcp_bootstrap import bootstrap_gcp_project, build_manual_console_url


def test_build_manual_console_url_points_to_project_credentials_page() -> None:
    assert (
        build_manual_console_url("ila-demo-project")
        == "https://console.cloud.google.com/apis/credentials?project=ila-demo-project"
    )


@pytest.mark.asyncio
async def test_bootstrap_gcp_project_reuses_existing_project(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    async def fake_request_json(*, method: str, url: str, access_token: str, json_body=None):
        calls.append((method, url))
        if method == "POST" and url.endswith("/v3/projects"):
            raise RuntimeError("POST failed: {'message': 'Project already exists'}")
        if method == "GET" and url.endswith("/v3/projects/ila-demo"):
            return {"name": "projects/1234567890"}
        if method == "POST" and "services:batchEnable" in url:
            return {"name": "operations/service-enable"}
        if method == "GET" and url.endswith("/v1/operations/service-enable"):
            return {"done": True}
        raise AssertionError(f"Unexpected call: {method} {url}")

    monkeypatch.setattr("src.gmail_gcp_bootstrap._request_json", fake_request_json)

    result = await bootstrap_gcp_project(
        access_token="token",
        project_id="ila-demo",
        project_name="ILA Demo",
    )

    assert result["project_number"] == "1234567890"
    assert ("GET", "https://cloudresourcemanager.googleapis.com/v3/projects/ila-demo") in calls
