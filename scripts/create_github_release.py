#!/usr/bin/env python3
"""Create a GitHub Release for the current tag using only the standard library.

This keeps the release workflow independent of third-party GitHub Actions.
Required environment variables are provided by GitHub Actions:
GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_REF_NAME, and GITHUB_SHA.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"missing required environment variable: {name}", file=sys.stderr)
        sys.exit(2)
    return value


def request(method: str, url: str, token: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if payload is not None:
        req.add_header("Content-Type", "application/json; charset=utf-8")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError:
            parsed = {"message": body}
        return exc.code, parsed


def main() -> int:
    token = require_env("GITHUB_TOKEN")
    repo = require_env("GITHUB_REPOSITORY")
    tag = require_env("GITHUB_REF_NAME")
    sha = require_env("GITHUB_SHA")

    api = f"https://api.github.com/repos/{repo}"
    status, existing = request("GET", f"{api}/releases/tags/{tag}", token)
    if status == 200:
        print(f"release already exists: {existing.get('html_url', tag)}")
        return 0
    if status != 404:
        print(f"failed to check existing release: HTTP {status} {existing}", file=sys.stderr)
        return 1

    repo_lower = repo.lower()
    body = "\n".join(
        [
            f"Automated release for `{tag}`.",
            "",
            "Docker images:",
            f"- `ghcr.io/{repo_lower}:{tag}`",
            f"- `ghcr.io/{repo_lower}:latest`",
            f"- `ghcr.io/{repo_lower}:sha-{sha[:7]}`",
            "",
            "The BridgeOps admin panel uses this GitHub Release as the source for update checks.",
        ]
    )
    payload = {
        "tag_name": tag,
        "target_commitish": sha,
        "name": f"BridgeOps {tag}",
        "body": body,
        "draft": False,
        "prerelease": False,
    }
    status, created = request("POST", f"{api}/releases", token, payload)
    if status not in (200, 201):
        print(f"failed to create release: HTTP {status} {created}", file=sys.stderr)
        return 1
    print(f"created release: {created.get('html_url', tag)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
