#!/usr/bin/env python3
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


def load_env(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip("'").strip('"')
            os.environ.setdefault(k.strip(), v)


def api_request(method: str, base_url: str, token: str, path: str, payload=None):
    url = base_url.rstrip("/") + path
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body
    except urllib.error.URLError as e:
        return 0, str(e)


def parse_json(text: str):
    try:
        return json.loads(text) if text else None
    except json.JSONDecodeError:
        return None


def print_json(data):
    print(json.dumps(data, ensure_ascii=True, indent=2))


def require_config():
    load_env(os.path.expanduser("~/.config/grafana/env"))
    base_url = os.environ.get("GRAFANA_URL")
    token = os.environ.get("GRAFANA_TOKEN")
    if not base_url or not token:
        print("Missing GRAFANA_URL or GRAFANA_TOKEN in env or ~/.config/grafana/env", file=sys.stderr)
        sys.exit(2)
    return base_url, token


def cmd_me(args):
    base_url, token = require_config()
    status, body = api_request("GET", base_url, token, "/api/user")
    data = parse_json(body)
    if status != 200 or not isinstance(data, dict):
        print(f"ERROR {status}: {body}", file=sys.stderr)
        sys.exit(1)
    print(
        f"id={data.get('id')} login={data.get('login')} name={data.get('name')} "
        f"orgId={data.get('orgId')} grafanaAdmin={data.get('isGrafanaAdmin')}"
    )


def cmd_search(args):
    base_url, token = require_config()
    q = urllib.parse.quote(args.query or "")
    status, body = api_request("GET", base_url, token, f"/api/search?query={q}")
    data = parse_json(body)
    if status != 200 or not isinstance(data, list):
        print(f"ERROR {status}: {body}", file=sys.stderr)
        sys.exit(1)
    if not data:
        print("No results")
        return
    for item in data:
        print(
            f"{item.get('type','?'):10} uid={item.get('uid','-'):20} "
            f"title={item.get('title','')} folder={item.get('folderTitle','')}"
        )


def cmd_dashboard_summary(args):
    base_url, token = require_config()
    uid = urllib.parse.quote(args.uid)
    status, body = api_request("GET", base_url, token, f"/api/dashboards/uid/{uid}")
    data = parse_json(body)
    if status != 200 or not isinstance(data, dict):
        print(f"ERROR {status}: {body}", file=sys.stderr)
        sys.exit(1)
    dash = data.get("dashboard", {})
    meta = data.get("meta", {})
    panels = dash.get("panels", []) or []
    print(
        f"uid={dash.get('uid')} title={dash.get('title')} version={dash.get('version')} "
        f"folder={meta.get('folderTitle','')}"
    )
    print(f"panels={len(panels)} tags={','.join(dash.get('tags', []) or [])}")
    for p in panels:
        print(
            f"panel id={p.get('id')} type={p.get('type')} title={p.get('title','')}"
        )


def cmd_dashboard_get(args):
    base_url, token = require_config()
    uid = urllib.parse.quote(args.uid)
    status, body = api_request("GET", base_url, token, f"/api/dashboards/uid/{uid}")
    if status != 200:
        print(f"ERROR {status}: {body}", file=sys.stderr)
        sys.exit(1)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(body)
        print(args.out)
    else:
        parsed = parse_json(body)
        print_json(parsed if parsed is not None else {"raw": body})


def cmd_dashboard_upsert(args):
    base_url, token = require_config()
    with open(args.file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    status, body = api_request("POST", base_url, token, "/api/dashboards/db", payload=payload)
    data = parse_json(body)
    if status not in (200, 202):
        print(f"ERROR {status}: {body}", file=sys.stderr)
        sys.exit(1)
    print_json(data if data is not None else {"raw": body})


def cmd_dashboard_delete(args):
    base_url, token = require_config()
    uid = urllib.parse.quote(args.uid)
    status, body = api_request("DELETE", base_url, token, f"/api/dashboards/uid/{uid}")
    data = parse_json(body)
    if status != 200:
        print(f"ERROR {status}: {body}", file=sys.stderr)
        sys.exit(1)
    print_json(data if data is not None else {"raw": body})


def build_parser():
    p = argparse.ArgumentParser(description="Low-token Grafana API helper")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("me", help="Validate credentials and show principal")
    sp.set_defaults(func=cmd_me)

    sp = sub.add_parser("search", help="Search dashboards/folders")
    sp.add_argument("--query", default="", help="search query text")
    sp.set_defaults(func=cmd_search)

    sp = sub.add_parser("dashboard-summary", help="Summarize a dashboard by UID")
    sp.add_argument("--uid", required=True)
    sp.set_defaults(func=cmd_dashboard_summary)

    sp = sub.add_parser("dashboard-get", help="Get full dashboard JSON by UID")
    sp.add_argument("--uid", required=True)
    sp.add_argument("--out", help="optional output file path")
    sp.set_defaults(func=cmd_dashboard_get)

    sp = sub.add_parser("dashboard-upsert", help="Create/update dashboard from payload JSON")
    sp.add_argument("--file", required=True, help="payload JSON file with dashboard/folderId/overwrite")
    sp.set_defaults(func=cmd_dashboard_upsert)

    sp = sub.add_parser("dashboard-delete", help="Delete dashboard by UID")
    sp.add_argument("--uid", required=True)
    sp.set_defaults(func=cmd_dashboard_delete)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
