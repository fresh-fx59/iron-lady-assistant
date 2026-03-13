from __future__ import annotations

import asyncio
from typing import Any, Sequence

from aiohttp import ClientSession

DEFAULT_REQUIRED_SERVICES: tuple[str, ...] = (
    "gmail.googleapis.com",
    "people.googleapis.com",
)


def build_manual_console_url(project_id: str) -> str:
    return f"https://console.cloud.google.com/apis/credentials?project={project_id}"


async def _request_json(
    *,
    method: str,
    url: str,
    access_token: str,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with ClientSession() as session:
        async with session.request(
            method,
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=json_body,
        ) as response:
            payload = await response.json()
            if response.status >= 400:
                raise RuntimeError(f"{method} {url} failed: {payload}")
            return payload


async def _poll_operation(
    *,
    access_token: str,
    operation_url: str,
    max_attempts: int = 30,
    sleep_seconds: float = 1.0,
) -> dict[str, Any]:
    for _ in range(max_attempts):
        payload = await _request_json(method="GET", url=operation_url, access_token=access_token)
        if payload.get("done") is True:
            if "error" in payload:
                raise RuntimeError(f"Operation failed: {payload['error']}")
            return payload
        await asyncio.sleep(sleep_seconds)
    raise RuntimeError(f"Operation did not complete in time: {operation_url}")


async def bootstrap_gcp_project(
    *,
    access_token: str,
    project_id: str,
    project_name: str,
    required_services: Sequence[str] = DEFAULT_REQUIRED_SERVICES,
) -> dict[str, str]:
    create_payload = await _request_json(
        method="POST",
        url="https://cloudresourcemanager.googleapis.com/v3/projects",
        access_token=access_token,
        json_body={"projectId": project_id, "displayName": project_name},
    )
    op_name = str(create_payload.get("name", "")).strip()
    if not op_name:
        raise RuntimeError("Project creation did not return an operation name.")
    project_operation = await _poll_operation(
        access_token=access_token,
        operation_url=f"https://cloudresourcemanager.googleapis.com/v3/{op_name}",
    )
    project = dict(project_operation.get("response") or {})
    project_resource_name = str(project.get("name", "")).strip()
    if not project_resource_name.startswith("projects/"):
        raise RuntimeError(f"Unexpected project response: {project}")
    project_number = project_resource_name.split("/", 1)[1]

    enable_payload = await _request_json(
        method="POST",
        url=f"https://serviceusage.googleapis.com/v1/projects/{project_number}/services:batchEnable",
        access_token=access_token,
        json_body={"serviceIds": list(required_services)},
    )
    enable_op_name = str(enable_payload.get("name", "")).strip()
    if not enable_op_name:
        raise RuntimeError("Service enablement did not return an operation name.")
    await _poll_operation(
        access_token=access_token,
        operation_url=f"https://serviceusage.googleapis.com/v1/{enable_op_name}",
    )
    return {
        "project_id": project_id,
        "project_number": project_number,
        "manual_console_url": build_manual_console_url(project_id),
    }
