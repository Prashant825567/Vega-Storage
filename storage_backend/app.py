"""The HTTP surface — deliberately ZentraGrid-shaped, so no UI has to change.

The panel (`lib/zentra.ts`) and the app (`src/lib/zentragrid/zentragridApi.ts`)
already speak this dialect, and switching to this backend is meant to be a
one-line change of base URL:

    POST   /v1/data/{collection}          → 201 {id, rev, data, created_at, updated_at}
    GET    /v1/data/{collection}?where=…  → 200 {documents: [...], next_cursor}
    PATCH  /v1/data/{collection}/{id}     → 200 {id, rev, data, …}
    DELETE /v1/data/{collection}/{id}     → 200 {id, deleted: true}
    POST   /v1/files                      → 201 {id, name, size, mime_type, status}
    GET    /v1/files/{id}/download        → 200 bytes (keyed, not public)

Two behaviours are copied on purpose because clients depend on them:

* a `limit` above the ceiling is a **400**, not a trimmed page;
* every failure answers with both `error` spellings and a `code`, so the panel's
  `ZtError` and a plain reader both get something true.

Startup never blocks the port. The MTProto session and the cold rebuild take as
long as they take, so the app binds first, reports readiness on `/healthz`, and
refuses writes with `503 NOT_READY` until the index is real.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import Depends, FastAPI, File, Header, Query, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from .config import Config, get_config
from .documents import DocumentStore
from .errors import StorageError, not_found, not_ready, unauthorized, validation_error
from .files import FileStore
from .hotstore import HotStore
from .index import ChannelIndex
from .query import check_limit, parse_order_by, parse_where
from .ranges import parse_range
from .telegram import TelethonGateway

log = logging.getLogger("storage_backend.app")


class Services:
    """Everything the routes need, in one object hung off `app.state`.

    The gateway is injectable so the HTTP surface can be tested against the fake
    channel: the routes, the status codes and the error bodies are worth proving
    without a Telegram account, and a test that needs credentials is a test that
    does not run.
    """

    def __init__(self, config: Config, gateway: Any | None = None) -> None:
        self.config = config
        self.gateway = gateway if gateway is not None else TelethonGateway(config)
        self.index = ChannelIndex(config)
        self.hot = HotStore(config)
        self.documents = DocumentStore(config, self.gateway, self.index, self.hot)
        self.files = FileStore(config, self.gateway, self.documents)
        self.connected = False
        self.connect_error: str | None = None
        self.snapshot_task: asyncio.Task[None] | None = None
        self.rebuild_task: asyncio.Task[None] | None = None

    # -- lifecycle ----------------------------------------------------- #

    async def bootstrap(self) -> None:
        """Connect, load the hot snapshot, then rebuild the index."""
        try:
            await self.gateway.connect()
            self.connected = True
        except Exception as exc:
            self.connect_error = f"{type(exc).__name__}: {exc}"
            log.error("MTProto connect failed: %s", self.connect_error)
            return
        try:
            self.hot.load()
        except Exception as exc:  # pragma: no cover - best effort
            log.warning("hot snapshot load failed: %s", exc)
        await self.rebuild()

    async def rebuild(self) -> None:
        try:
            await self.index.rebuild(self.gateway)
        except Exception as exc:  # pragma: no cover - defensive
            log.error("cold rebuild crashed: %s", exc)

    async def shutdown(self) -> None:
        for task in (self.rebuild_task, self.snapshot_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        try:
            self.hot.save()
        except Exception:  # pragma: no cover
            pass
        await self.gateway.close()


def build_app(
    config: Config | None = None,
    gateway_factory: Any | None = None,
) -> FastAPI:
    resolved = config

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        actual = resolved or get_config()
        services = Services(
            actual, gateway_factory(actual) if gateway_factory is not None else None
        )
        app.state.services = services
        # The port opens immediately; Telegram and the index catch up behind it.
        services.rebuild_task = asyncio.create_task(services.bootstrap())
        services.snapshot_task = asyncio.create_task(_snapshot_loop(services))
        try:
            yield
        finally:
            await services.shutdown()

    app = FastAPI(
        title="Vega storage backend",
        version="0.1.0",
        description="Telegram (MTProto) channels behind a ZentraGrid-compatible API",
        lifespan=lifespan,
    )

    # ------------------------------------------------------------------ #
    # Wiring: auth, errors, readiness
    # ------------------------------------------------------------------ #

    def services_for(request: Request) -> Services:
        services: Services | None = getattr(request.app.state, "services", None)
        if services is None:  # pragma: no cover - impossible while serving
            raise not_ready("the service is still starting")
        return services

    async def require_key(
        request: Request,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None, alias="X-Api-Key"),
    ) -> Services:
        services = services_for(request)
        presented = ""
        if authorization and authorization.lower().startswith("bearer "):
            presented = authorization[7:].strip()
        elif x_api_key:
            presented = x_api_key.strip()
        expected = services.config.api_key
        # Constant-time compare: an API key check that leaks length is a small
        # hole, but it is free to close.
        import hmac

        if not presented or not hmac.compare_digest(presented, expected):
            raise unauthorized()
        return services

    @app.exception_handler(StorageError)
    async def storage_error_handler(_: Request, exc: StorageError) -> JSONResponse:
        return JSONResponse(exc.body(), status_code=exc.status, headers=exc.headers)

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error: %s", exc)
        error = StorageError(str(exc) or "Unhandled error", 500, "INTERNAL_ERROR")
        return JSONResponse(error.body(), status_code=error.status)

    def guard_ready(services: Services) -> None:
        if not services.connected:
            raise not_ready(
                services.connect_error or "the MTProto session is not connected yet"
            )
        if not services.index.ready:
            raise not_ready("the channel index is still being rebuilt")

    # ------------------------------------------------------------------ #
    # Health — must answer in well under Render's 5-second check limit
    # ------------------------------------------------------------------ #

    @app.get("/healthz")
    async def healthz(request: Request) -> dict[str, Any]:
        services = services_for(request)
        return {
            "ok": True,
            # Reported, never enforced: the check answers 200 as soon as the
            # process is up, because a slow Telegram handshake must not look
            # like a dead service.
            "connected": services.connected,
            "index_ready": services.index.ready,
            "connect_error": services.connect_error,
        }

    @app.get("/")
    async def root() -> dict[str, Any]:
        return {
            "service": "vega-storage-backend",
            "api": "/v1/data/{collection}, /v1/files",
            "health": "/healthz",
        }

    @app.get("/v1/status")
    async def status(_: Services = Depends(require_key)) -> dict[str, Any]:
        return {
            "connected": _.connected,
            "connect_error": _.connect_error,
            "channels": {
                key: {
                    "title": channel.title,
                    "peer_id": channel.peer_id,
                    "resolved": key in _.gateway.resolved,
                }
                for key, channel in _.config.channels.items()
            },
            "index": _.index.stats(),
            "hot": _.hot.stats(),
            "routes": dict(_.config.routes),
            "hot_collections": list(_.config.hot_collections),
        }

    @app.post("/v1/admin/rebuild")
    async def rebuild(_: Services = Depends(require_key)) -> dict[str, Any]:
        await _.rebuild()
        return {"rebuilt": True, "index": _.index.stats()}

    # ------------------------------------------------------------------ #
    # Documents
    # ------------------------------------------------------------------ #

    @app.post("/v1/data/{collection}", status_code=201)
    async def create_document(
        collection: str,
        request: Request,
        services: Services = Depends(require_key),
    ) -> Any:
        body = await _json_body(request)
        guard_ready(services)
        document = await services.documents.create(
            collection,
            body.get("data") if isinstance(body.get("data"), dict) else body.get("data") or {},
            doc_id=body.get("id"),
        )
        return document

    @app.get("/v1/data/{collection}")
    async def query_documents(
        collection: str,
        where: list[str] | None = Query(default=None),
        order_by: str | None = Query(default=None),
        limit: int | None = Query(default=None),
        cursor: str | None = Query(default=None),
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        filters = parse_where(where or [])
        ordering = parse_order_by(order_by)
        page_size = check_limit(limit, services.config.max_page_limit)
        page = services.documents.query(
            collection,
            filters=filters,
            ordering=ordering,
            limit=page_size,
            cursor=cursor,
        )
        return {"documents": page.documents, "next_cursor": page.next_cursor}

    @app.get("/v1/data/{collection}/{doc_id}")
    async def get_document(
        collection: str,
        doc_id: str,
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        document = services.documents.get(collection, doc_id)
        if document is None:
            raise not_found(f"No document {doc_id!r} in collection {collection!r}")
        return document

    @app.patch("/v1/data/{collection}/{doc_id}")
    async def patch_document(
        collection: str,
        doc_id: str,
        request: Request,
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        body = await _json_body(request)
        guard_ready(services)
        return await services.documents.patch(
            collection,
            doc_id,
            body.get("data") or {},
            expected_rev=_rev(body),
        )

    @app.put("/v1/data/{collection}/{doc_id}")
    async def put_document(
        collection: str,
        doc_id: str,
        request: Request,
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        body = await _json_body(request)
        guard_ready(services)
        return await services.documents.replace(
            collection,
            doc_id,
            body.get("data") or {},
            expected_rev=_rev(body),
        )

    @app.delete("/v1/data/{collection}/{doc_id}")
    async def delete_document(
        collection: str,
        doc_id: str,
        request: Request,
        expected_rev: int | None = Query(default=None),
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        guard_ready(services)
        # A body is optional on DELETE; read one only if it is there, so a client
        # that sends `{data: {...}}` out of habit is not punished for it.
        body = await _json_body(request, optional=True)
        if expected_rev is None:
            expected_rev = _rev(body)
        return await services.documents.delete(
            collection, doc_id, expected_rev=expected_rev
        )

    # ------------------------------------------------------------------ #
    # Files
    # ------------------------------------------------------------------ #

    @app.post("/v1/files", status_code=201)
    async def upload_file(
        request: Request,
        file: UploadFile = File(...),
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        guard_ready(services)
        name = file.filename or "upload.bin"
        payload = await file.read()
        record = await services.files.upload(
            data=payload,
            name=name,
            mime=file.content_type or "application/octet-stream",
            owner=request.headers.get("X-Owner"),
            parent_id=request.headers.get("X-Parent-Id"),
        )
        return record.to_document()

    @app.get("/v1/files")
    async def list_files(
        owner: str | None = Query(default=None),
        parent_id: str | None = Query(default=None),
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        return {"files": services.files.list(owner=owner, parent_id=parent_id)}

    @app.get("/v1/files/{file_id}")
    async def get_file(
        file_id: str, services: Services = Depends(require_key)
    ) -> dict[str, Any]:
        return services.files.get(file_id).to_document()

    @app.patch("/v1/files/{file_id}")
    async def rename_file(
        file_id: str,
        request: Request,
        services: Services = Depends(require_key),
    ) -> dict[str, Any]:
        body = await _json_body(request)
        name = body.get("name") or (body.get("data") or {}).get("name")
        if not name:
            raise validation_error("PATCH /v1/files/{id} needs a `name`")
        guard_ready(services)
        return (await services.files.rename(file_id, str(name))).to_document()

    @app.delete("/v1/files/{file_id}")
    async def delete_file(
        file_id: str, services: Services = Depends(require_key)
    ) -> dict[str, Any]:
        guard_ready(services)
        return await services.files.delete(file_id)

    @app.get("/v1/files/{file_id}/download")
    async def download_file(
        file_id: str,
        request: Request,
        services: Services = Depends(require_key),
    ) -> StreamingResponse:
        guard_ready(services)
        record = services.files.get(file_id)
        total = record.size
        range_header = request.headers.get("range") or request.headers.get("Range")
        start, end = parse_range(range_header, total)
        length = None if total == 0 else end - start + 1

        async def body() -> AsyncIterator[bytes]:
            async for chunk in services.files.stream(file_id, offset=start, length=length):
                yield chunk

        headers = {
            "Accept-Ranges": "bytes",
            "Content-Disposition": f'inline; filename="{record.name}"',
            "Content-Type": record.mime or "application/octet-stream",
        }
        if range_header:
            headers["Content-Range"] = f"bytes {start}-{end}/{total}"
            return StreamingResponse(body(), status_code=206, headers=headers)
        if total:
            headers["Content-Length"] = str(total)
        return StreamingResponse(body(), status_code=200, headers=headers)

    return app


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


async def _json_body(request: Request, *, optional: bool = False) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        if optional:
            return {}
        raise validation_error("Request body must be JSON") from None
    if not isinstance(body, dict):
        raise validation_error("Request body must be a JSON object")
    return body


def _rev(body: dict[str, Any]) -> int | None:
    """`expected_rev` from either place a client might put it."""
    for key in ("expected_rev", "expectedRev", "rev_expected"):
        value = body.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            raise validation_error(f"{key} must be an integer") from None
    return None


async def _snapshot_loop(services: Services, interval_seconds: float = 30.0) -> None:
    """Periodically persist the hot store. Its loss is survivable, not free."""
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            services.hot.save()
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            raise
        except Exception as exc:  # pragma: no cover
            log.debug("hot snapshot loop: %s", exc)


# `uvicorn storage_backend.app:app`
#
# There is deliberately no module-level `app = build_app()`: building it needs the
# credentials, and a module that cannot be imported without secrets cannot be
# unit tested. `__getattr__` only runs when normal lookup fails, so the app is
# constructed the first time something asks for it by name — which is exactly what
# `uvicorn storage_backend.app:app` does.
_INSTANCE: FastAPI | None = None


def get_app() -> FastAPI:
    global _INSTANCE
    if _INSTANCE is None:
        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        _INSTANCE = build_app()
    return _INSTANCE


def __getattr__(name: str) -> Any:
    if name == "app":
        return get_app()
    raise AttributeError(name)
