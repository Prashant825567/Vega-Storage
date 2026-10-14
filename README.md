# Vega storage backend

Telegram channels as storage, behind a **ZentraGrid-compatible API** — so the admin
panel and the app keep working unchanged and only swap a base URL.

This is **MTProto** (a real user account, via Telethon), not the Bot API. The
account is a member of five private channels; each channel is a box, and a document
is one message that is edited in place.

```
storage-backend/
├── storage_backend/
│   ├── config.py      environment → channels, routes, limits
│   ├── wire.py        the ZG1 record codec: one document ⇄ channel messages
│   ├── query.py       where= / order_by= / limit / cursor, as the clients send them
│   ├── ranges.py      HTTP Range arithmetic
│   ├── telegram.py    the only module that speaks MTProto (Telethon)
│   ├── index.py       Layer 1 — in-memory index + cold rebuild
│   ├── documents.py   Layers 2 + 3 — per-document locks, rev + expected_rev, write path
│   ├── hotstore.py    the same contract for flag collections, with no channel
│   ├── files.py       bytes in a channel, a pointer document in the index
│   └── app.py         the HTTP surface (FastAPI)
├── scripts/
│   ├── make_session.py     one-off: produce TELEGRAM_SESSION
│   ├── check_channels.py   prove the account can reach all five channels
│   └── smoke.py            end-to-end test against a running service
└── tests/                  115 tests, no credentials, no network
```

## The five channels

| Key | Channel | Id | Holds |
|---|---|---|---|
| `tg.vault.1` | Vega Vault | `-1003597396903` | Uploaded bytes — one message per file (or per part) |
| `tg.ai.1` | Vega AI | `-1004362809278` | AI chat documents for signed-in users |
| `tg.support.1` | Vega Support | `-1004337129320` | `messages` — one message = one support message |
| `tg.notify.1` | Vega Notify | `-1003755323456` | `chat_broadcast` and notification documents |
| `tg.index.1` | Vega Index | `-1003960423150` | Reserved for encrypted index manifests (recovery) |

The ids default in `config.py`, so a deploy needs no channel configuration. Point
them at `[TST]` channels per environment with `TELEGRAM_*_ID`.

## Quick start

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows; use .venv/bin/activate on macOS/Linux
pip install -r requirements.txt
cp .env.example .env.local           # then fill in the four secrets
python scripts/make_session.py      # prints TELEGRAM_SESSION
python scripts/check_channels.py    # proves all five are reachable
uvicorn storage_backend.app:app --host 0.0.0.0 --port 8000
python scripts/smoke.py http://localhost:8000
```

Use a virtualenv rather than a global Python: this project needs current
`fastapi`/`pydantic`/`httpx`, and installing those globally can pull dependencies
out from under unrelated tools that pin older versions.

## Deploying to Render

1. **Generate the session locally** — `python scripts/make_session.py`. It logs in
   and prints a string. Never run this on Render; it is interactive on purpose.
2. **Create the service** from `render.yaml` (New → Blueprint), or manually with:
   - build: `pip install -r requirements.txt`
   - start: `uvicorn storage_backend.app:app --host 0.0.0.0 --port $PORT --workers 1`
   - health check path: `/healthz`
3. **Set four secrets** in Environment:

   | Variable | What |
   |---|---|
   | `TELEGRAM_API_ID` | from my.telegram.org |
   | `TELEGRAM_API_HASH` | from my.telegram.org |
   | `TELEGRAM_SESSION` | the string from step 1 (**bearer credential**) |
   | `ZENTRAGRID_API_KEY` | the key the panel/app send as `Authorization: Bearer …` |
4. **Verify**: run `python scripts/check_channels.py` locally **before** the
   hosted instance starts (never share its session with a concurrent process),
   then `python scripts/smoke.py https://<service>.onrender.com`. Check `/readyz`
   returns 200 once the connection and index are ready.

### Pointing the clients at it

Nothing in the panel or the app changes except the base URL:

- panel: `lib/zentra.ts` → `ZENTRAGRID_BASE`
- app: the ZentraGrid base constant in `src/lib/zentragrid/zentragridApi.ts`

Keep `ZENTRAGRID_API_KEY` the same value the clients already send.

## API

Identical shapes to ZentraGrid, including two behaviours the clients depend on:
a `limit` above the ceiling is a **400** (not a trimmed page), and every error
carries both `error` spellings plus a `code`.

| Method | Path | Notes |
|---|---|---|
| `POST` | `/v1/data/{collection}` | `{id?, data}` → `201 {id, rev, data, created_at, updated_at}` |
| `GET` | `/v1/data/{collection}` | `where=field:op:value` (repeat = AND), `order_by=field(:desc)`, `limit`, `cursor` |
| `GET` | `/v1/data/{collection}/{id}` | one document, or `404` |
| `PATCH` | `/v1/data/{collection}/{id}` | `{data}` merges; `{expected_rev}` enforces CAS |
| `PUT` | `/v1/data/{collection}/{id}` | replace the whole body |
| `DELETE` | `/v1/data/{collection}/{id}` | `{id, deleted: true}` — tombstone by default |
| `POST` | `/v1/files` | multipart `file` → file record with an opaque `id` |
| `GET` | `/v1/files` | `?owner=`, `?parent_id=` |
| `GET` | `/v1/files/{id}` | metadata |
| `PATCH` | `/v1/files/{id}` | `{name}` — **metadata only**, never a re-upload |
| `DELETE` | `/v1/files/{id}` | removes the record (bytes too when `ZG_HARD_DELETE=true`) |
| `GET` | `/v1/files/{id}/download` | bytes, with `Range` → `206` for seeking |
| `GET` | `/healthz` | Liveness: 200 while the HTTP process is up; includes connection state |
| `GET` | `/readyz` | Readiness: 200 only with connected MTProto and a complete index; otherwise 503 |
| `GET` | `/v1/status` | channels, index stats, hot store stats, routes |
| `POST` | `/v1/admin/rebuild` | re-run the cold rebuild on demand |

Errors: `400 VALIDATION_ERROR`, `401 INVALID_ID_TOKEN`, `404 NOT_FOUND`,
`409 DOCUMENT_ALREADY_EXISTS`, `409 REVISION_CONFLICT`, `416 RANGE_NOT_SATISFIABLE`,
`429 RATE_LIMITED` (with `Retry-After`), `503 CHANNEL_UNAVAILABLE`, `503 NOT_READY`.

## How it works

### The record format

Every stored message is:

```
ZG1|<collection>|<doc_id>|<rev>|<part>/<total>|<chunk>
```

`ZG1` is a format tag, so anything else in the channel is somebody's human message
and is ignored by the index — the channels stay readable and an operator can still
type in them. The revision lives **inside the message**, which is what makes a cold
rebuild possible with no side database: scan the channel, read the headers, and the
whole index is back.

### Layer 1 — every read comes from memory

The index is rebuilt once at startup by scanning the channels, then kept current by
each successful write. A read never touches the network. This is not an optimisation
for its own sake: the panel polls every 10–15 seconds, and a Telegram round trip per
poll would be both slow and a flood-wait magnet.

`/healthz` answers 200 immediately and reports readiness separately, because
Render's health check must pass within five seconds and an MTProto login plus a
rebuild is not that fast. Until the index is real, writes get `503 NOT_READY`.

### MTProto recovery (no manual redeploy for transient outages)

A single background supervisor owns connection attempts. It retries transient
startup/network failures indefinitely using exponential backoff (2 seconds up to
60 seconds, with jitter). Telethon's built-in reconnect remains enabled; the
supervisor also recovers after Telethon exhausts its own retries.

- Every 30 seconds a real, read-only Telegram RPC checks responsiveness, with a
  15-second timeout. Connection setup has a 60-second timeout. A hung socket cannot
  leave the service stuck forever; actual recovery also depends on network and
  Telegram availability and cleanup time.
- `/healthz` is **liveness**, so leave Render's health-check path on `/healthz`.
  `/readyz` returns **503** during startup, disconnection or incomplete rebuilds.
  Do not use readiness as a restart trigger: Telegram outages should not create
  process restart loops. `connected` checks the transport flag as well as the
  last successful handshake/probe; a half-open socket is detected at the next
  bounded probe, not synchronously by HTTP health requests.
- `/healthz`, `/readyz` and authenticated `/v1/status` expose `connection_state`,
  `connect_error`, `reconnect_attempts` (including the initial attempt), and
  `last_successful_probe` (UTC; null until the first periodic probe succeeds).
- A healthy in-memory index survives reconnects: no full history scan on every
  brief network drop. Startup/failed rebuilds are retried. A scan error is no
  longer silently mistaken for end-of-history. All configured channels must scan
  successfully before writes become ready; check `index.rebuild_errors` in
  `/v1/status`. Failed scans retain the previously published index for cached
  reads, which may be stale. New rebuilds are staged before publication.
- Telegram flood waits during recovery are honored in full, even when longer
  than the backoff ceiling. Write flood waits still return `429` + `Retry-After`.
- A revoked/invalid/duplicate session stops retries with
  `connection_state: session_error`. Replace the bad credentials/session and
  restart once; restarting repeatedly cannot repair revoked credentials. Login
  in the server is non-interactive (no phone/code prompts).
- Recovery does **not** replay in-flight writes or uploads. An interrupted write
  can have an unknown outcome; reconcile it before retrying to avoid duplicates.
- Use **one worker and one service instance**. The Render start command explicitly
  sets `--workers 1`, overriding `WEB_CONCURRENCY`. Do not run local checks or
  another deployment concurrently with the same session. Avoid old/new instance
  overlap during deployments; use stop-before-start if your hosting setup permits
  it. The application supervisor is process-local, not a distributed lock.
- A sleeping/stopped Render Free instance cannot run a supervisor. Use an
  always-on plan if continuous availability is required. This also cannot fix
  Telegram bans, persistent network restrictions, missing channel permissions,
  OOM kills, or other host-level failures.

Optional settings (all in seconds; see `.env.example`):

| Variable | Default |
|---|---:|
| `ZG_CONNECT_TIMEOUT_SECONDS` | 60 |
| `ZG_PROBE_INTERVAL_SECONDS` | 30 |
| `ZG_PROBE_TIMEOUT_SECONDS` | 15 |
| `ZG_RECONNECT_INITIAL_SECONDS` | 2 |
| `ZG_RECONNECT_MAX_SECONDS` | 60 |
| `ZG_REBUILD_TIMEOUT_SECONDS` | 600 |

No new secrets are required. Raise the rebuild timeout for histories that take
longer than ten minutes; otherwise each timed-out scan will be retried.

### Layer 2 — one lock per document

Fifty requests aimed at one document queue up instead of racing: the whole
read-modify-write cycle runs inside the lock. Different documents have different
locks, so they still go in parallel. Locks are dropped when idle, so a long-lived
process does not accumulate one per document it has ever seen.

### Layer 3 — `rev` + `expected_rev`

`sent_at`-style timestamps are not a concurrency control. A caller that says
`expected_rev: 3` when the document is at 4 gets `409 REVISION_CONFLICT` and learns
its copy was stale — the same compare-and-swap idea as Firestore. This is what stops
two overlapping edits from silently losing one of them.

### Write order, and what a crash leaves behind

Telegram is written **first**, memory second. If Telegram fails, memory is
untouched and the index still describes what is actually stored. The two write
styles differ by shape:

- **one message in, one message out** — a plain in-place edit: the message id, the
  channel size and the API calls all stay minimal.
- **anything spanning several messages** — append the whole new revision, *then*
  delete the old one. An in-place multipart edit has a torn window (part 1 says
  revision 9 while parts 2–5 still say 1), and a crash inside it would leave the
  old revision unrecoverable. Append-then-swap costs a few extra messages and makes
  "the newest **complete** revision wins" a rule that actually holds.

### Why `presence` never touches a channel

A chat carries roughly **one write per second**; a heartbeat per user every 20
seconds at 10k users is **~500 writes per second**. That ratio cannot be cached
away, because it is the workload itself. So hot collections (`presence`, `typing`,
`read_cursor`) are served by the same API with the same `rev`/`expected_rev`
semantics from process memory, with a best-effort disk snapshot. Their loss is
survivable by design: a heartbeat is stale within 60 seconds anyway.

Point a collection at the hot store with `ZG_HOT_COLLECTIONS`; everything else goes
to its channel, chosen by `ZG_ROUTE_<COLLECTION>` (see `.env.example`).

## Tests

```bash
python -m unittest discover -s tests -t .
```

115 tests, no credentials and no network: the fake channel implements the same
`TelegramLike` protocol as the real gateway, so the write path, the conflict check,
multi-part edits, byte ranges and the cold rebuild are all proven in-process — and
the API tests drive the real FastAPI app through `TestClient`, so the status codes
and error bodies are proven too. Highlights:

- a failed edit leaves the index describing what the channel really holds;
- a stale `expected_rev` is a 409 and the stored text is unchanged;
- a torn multipart write leaves the previous **complete** revision in place;
- a rebuild restores every document, tombstone included, from the channel alone;
- 100 presence beats produce **zero** channel writes;
- parallel writers on one document lose no update (50 increments, 50 accounted for);
- over HTTP: `409 REVISION_CONFLICT` carries the real revision, an oversized
  `limit` is a 400, and a `Range` request is a `206` with a correct `Content-Range`.

Running the HTTP tests needs `httpx` (for `TestClient`); everything else is
stdlib. `pip install httpx` if the API tests skip on import.

## Limits and risks (read before trusting it with data)

- **Not a storage SLA.** One account, one session. A ban or a restricted channel
  takes the store with it — which is why the plan keeps five channels and an index
  channel for recovery, and why the store must not be the only copy of anything
  irreplaceable.
- **Telegram's ToS/rate limits.** Respect `~1 write/second per chat`
  (`ZG_CHANNEL_MIN_INTERVAL_MS` enforces it), honour `429 Retry-After`, and never
  deploy a second instance against the same session.
- **One writer.** The in-memory index means one process. Two workers would each hold
  half an index. Scale up (bigger instance), not out.
- **Documents are last-write-wins.** Telegram keeps no revision history you can
  read back, so an edit replaces the old text for good. That is fine for these
  documents; it is not an archive.
- **Files:** 2 GB per part (4 GB on Premium), split above `MAX_PART_BYTES`. Rename
  never re-uploads. Delete is a tombstone unless `ZG_HARD_DELETE=true`.
- **Free Render instances spin down** after 15 minutes idle: expect a reconnect plus
  rebuild on the next request.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `503 CHANNEL_UNAVAILABLE` | The account is not a member, or the id is wrong. Run `check_channels.py`; a `ValueError` means the account has never seen that channel, so open it once in that account's Telegram app. |
| `429 RATE_LIMITED` with `Retry-After` | Telegram flood control. Raise `ZG_CHANNEL_MIN_INTERVAL_MS`, or check whether something is writing per-message where it should write per-document. |
| `503 NOT_READY` right after deploy | Normal for the first seconds: the handshake and rebuild are still running. `/v1/status` shows `index.rebuilt_at`. |
| `connection_state: recovering` | Automatic recovery is running. Inspect `connect_error`; no manual redeploy is needed for a transient outage. |
| `connection_state: session_error` | Credentials/session need attention. Stop duplicate instances, fix/replace the session, then restart once. |
| `index.rebuild_errors` or repeated rebuild timeouts | Fix channel membership/IDs or increase `ZG_REBUILD_TIMEOUT_SECONDS` for large histories. Writes remain unready rather than using a partial index. |
| Everything vanished after a redeploy | Expected for hot collections (`presence`). Documents live in the channels and come back on the next rebuild. |
| `400 VALIDATION_ERROR` on a big query | The page ceiling is 200, exactly like ZentraGrid. Page with `cursor`. |

## Not built yet

Honest list, so nothing here is mistaken for finished:

- **No per-user auth.** A single project-wide key, same as today. `owner` on files
  is a label the caller sets, not a claim the service verifies.
- **No sharing links or expiry** for files (`shares` in the plan is not implemented).
- **No encryption.** Content is plain inside the channel; the plan's encrypted
  index manifest is not written yet, so `tg.index.1` is reserved but unused.
- **`tg.ai.1` and `tg.notify.1` are routed but not exercised by a feature** — the
  mapping exists, the traffic does not.
- **No multi-writer support** (see above), and no background compaction of
  superseded multipart messages.
