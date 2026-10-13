"""End-to-end smoke test against a *running* service, over HTTP.

    python scripts/smoke.py https://your-service.onrender.com

This exists because the unit tests prove the store, not the deployment: they use a
fake channel and never touch HTTP. This walks the exact surface the admin panel and
the app use — create, read, patch, the stale-revision conflict, a `where` query, a
file upload and a ranged download, then a cleanup — and prints a line per step.

It leaves nothing behind: everything is written to a throwaway collection and a
file that is deleted at the end. Run it against preview before pointing the panel
at a new base URL.

Exit status is the verdict: 0 means every step matched what the clients expect.
"""

from __future__ import annotations

import json
import os
import sys

# Windows terminals default to cp1252, which cannot encode every arrow and dash
# this script prints. Losing a diagnostic line to a UnicodeEncodeError would be a
# silly way to hide a real failure, so the stream is made forgiving up front.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):  # pragma: no cover - older interpreters
    pass
import urllib.error
import urllib.request
import uuid

COLLECTION = "smoke"
FAILURES: list[str] = []


def call(
    base: str,
    key: str,
    method: str,
    path: str,
    *,
    body: dict | None = None,
    raw: bytes | None = None,
    content_type: str | None = None,
    headers: dict[str, str] | None = None,
    expect: int | tuple[int, ...] = (200, 201),
) -> tuple[int, dict | bytes, dict[str, str]]:
    url = f"{base.rstrip('/')}{path}"
    data = raw
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        content_type = "application/json"
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Authorization", f"Bearer {key}")
    if content_type:
        request.add_header("Content-Type", content_type)
    for name, value in (headers or {}).items():
        request.add_header(name, value)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status = response.status
            payload = response.read()
            response_headers = dict(response.headers)
    except urllib.error.HTTPError as exc:
        status = exc.code
        payload = exc.read()
        response_headers = dict(exc.headers)

    wanted = (expect,) if isinstance(expect, int) else expect
    if status not in wanted:
        FAILURES.append(
            f"{method} {path} -> {status}, expected {wanted}: {payload[:300]!r}"
        )
    try:
        parsed: dict | bytes = json.loads(payload) if payload else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        parsed = payload
    # Header names are case-insensitive; a plain dict is not.
    response_headers = {name.lower(): value for name, value in response_headers.items()}
    return status, parsed, response_headers


def step(label: str, ok: bool, detail: str = "") -> None:
    mark = "ok  " if ok else "FAIL"
    print(f"  {mark} {label}{(' — ' + detail) if detail else ''}")
    if not ok:
        FAILURES.append(f"{label}: {detail}")


def multipart(field: str, filename: str, payload: bytes, mime: str) -> tuple[bytes, str]:
    boundary = f"----vega{uuid.uuid4().hex}"
    head = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{field}"; filename="{filename}"\r\n'
        f"Content-Type: {mime}\r\n\r\n"
    ).encode("utf-8")
    tail = f"\r\n--{boundary}--\r\n".encode("utf-8")
    return head + payload + tail, f"multipart/form-data; boundary={boundary}"


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    base = sys.argv[1]
    key = os.environ.get("ZENTRAGRID_API_KEY") or os.environ.get("STORAGE_API_KEY") or ""
    if not key:
        print("Set ZENTRAGRID_API_KEY in the environment first.", file=sys.stderr)
        return 2

    doc_id = f"smoke_{uuid.uuid4().hex[:12]}"
    print(f"smoke test against {base} (collection={COLLECTION!r}, doc={doc_id!r})\n")

    # -- health -------------------------------------------------------- #
    status, health, _ = call(base, key, "GET", "/healthz", expect=(200, 200))
    step("healthz answers", status == 200, str(health)[:120])
    if status != 200:
        return 1

    # -- status -------------------------------------------------------- #
    status, health_status, _ = call(base, key, "GET", "/v1/status")
    channels = health_status.get("channels", {}) if isinstance(health_status, dict) else {}
    resolved = [k for k, v in channels.items() if v.get("resolved")]
    step("status lists the five channels", len(channels) == 5, f"{len(channels)} configured")
    step("every channel resolved", len(resolved) == 5, f"{len(resolved)} resolved")
    index = health_status.get("index", {}) if isinstance(health_status, dict) else {}
    step("index is ready", bool(index.get("ready")), f"rebuilt_at={index.get('rebuilt_at')}")

    # -- auth ---------------------------------------------------------- #
    status, _, _ = call(base, "definitely-wrong-key", "GET", "/v1/status", expect=401)
    step("a wrong key is a 401", status == 401, f"got {status}")

    # -- create / read -------------------------------------------------- #
    status, created, _ = call(
        base,
        key,
        "POST",
        f"/v1/data/{COLLECTION}",
        body={"id": doc_id, "data": {"chat_id": "smoke@vega", "text": "hello", "read": False}},
    )
    step("create returns 201 with rev 1", status == 201 and created.get("rev") == 1,
         f"status={status} rev={created.get('rev')}")

    status, fetched, _ = call(base, key, "GET", f"/v1/data/{COLLECTION}/{doc_id}")
    step("read returns the stored body",
         isinstance(fetched, dict) and fetched.get("data", {}).get("text") == "hello")

    # -- patch --------------------------------------------------------- #
    status, patched, _ = call(
        base, key, "PATCH", f"/v1/data/{COLLECTION}/{doc_id}",
        body={"data": {"read": True}, "expected_rev": 1},
    )
    step("patch merges and bumps the revision",
         status == 200 and patched.get("rev") == 2
         and patched.get("data", {}).get("read") is True
         and patched.get("data", {}).get("text") == "hello",
         f"status={status} rev={patched.get('rev')}")

    # -- stale write --------------------------------------------------- #
    # `expected_rev` is the whole point of this step. Without it the patch is a
    # plain merge and *succeeds*, which is exactly how an optimistic-concurrency
    # check ends up quietly testing nothing.
    status, conflict, _ = call(
        base, key, "PATCH", f"/v1/data/{COLLECTION}/{doc_id}",
        body={"data": {"text": "stale"}, "expected_rev": 1}, expect=409,
    )
    code = conflict.get("code") if isinstance(conflict, dict) else None
    step("a stale expected_rev is a 409 REVISION_CONFLICT",
         status == 409 and code == "REVISION_CONFLICT", f"status={status} code={code}")

    # -- query --------------------------------------------------------- #
    status, page, _ = call(
        base, key, "GET",
        f"/v1/data/{COLLECTION}?where=chat_id:eq:smoke@vega&order_by=updated_at:desc&limit=10",
    )
    documents = page.get("documents", []) if isinstance(page, dict) else []
    step("where= finds the document", any(d.get("id") == doc_id for d in documents),
         f"{len(documents)} row(s)")

    status, too_big, _ = call(
        base, key, "GET", f"/v1/data/{COLLECTION}?limit=500", expect=400
    )
    step("limit above the ceiling is a 400", status == 400, f"got {status}")

    # -- files --------------------------------------------------------- #
    payload = bytes(range(256)) * 8
    raw, content_type = multipart("file", "smoke.bin", payload, "application/octet-stream")
    status, uploaded, _ = call(
        base, key, "POST", "/v1/files", raw=raw, content_type=content_type
    )
    file_id = uploaded.get("id") if isinstance(uploaded, dict) else None
    step("upload returns a file record with an id", bool(file_id),
         f"status={status} id={file_id}")
    step("upload reports the right size",
         isinstance(uploaded, dict) and uploaded.get("size") == len(payload),
         f"size={uploaded.get('size') if isinstance(uploaded, dict) else None}")

    if file_id:
        status, body_bytes, headers = call(
            base, key, "GET", f"/v1/files/{file_id}/download"
        )
        step("download returns the exact bytes",
             isinstance(body_bytes, bytes) and body_bytes == payload,
             f"{len(body_bytes) if isinstance(body_bytes, bytes) else '?'} bytes")

        status, chunk, headers = call(
            base, key, "GET", f"/v1/files/{file_id}/download",
            headers={"Range": "bytes=100-199"}, expect=206,
        )
        step("a Range request is a 206 with the right slice",
             status == 206 and chunk == payload[100:200],
             f"status={status} content-range={headers.get('content-range')}")

        call(base, key, "DELETE", f"/v1/files/{file_id}")

    # -- cleanup ------------------------------------------------------- #
    status, deleted, _ = call(base, key, "DELETE", f"/v1/data/{COLLECTION}/{doc_id}")
    step("delete reports the removal",
         status == 200 and isinstance(deleted, dict) and deleted.get("deleted") is True,
         f"status={status}")

    status, _, _ = call(base, key, "GET", f"/v1/data/{COLLECTION}/{doc_id}", expect=404)
    step("a deleted document reads as 404", status == 404, f"got {status}")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} problem(s):")
        for failure in FAILURES:
            print(f"  - {failure}")
        return 1
    print("Every step matched what the panel and the app expect.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
