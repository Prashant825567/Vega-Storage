"""One error type, and the exact JSON shape the existing clients already read.

`lib/zentra.ts` in the admin panel parses `body.error.message` **or**
`body.message`, and reads `body.error.code`. So every failure is answered with
both spellings at once — a flat `{error, code}` plus a nested
`{error: {message, code}}` — and no client has to be touched.
"""

from __future__ import annotations

from typing import Any


class StorageError(Exception):
    """Something the caller should be told about, with a status and a code."""

    def __init__(
        self,
        message: str,
        status: int = 400,
        code: str = "VALIDATION_ERROR",
        *,
        retry_after: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code
        self.retry_after = retry_after
        self.details = details or {}

    def body(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            # Flat form (what most callers read) …
            "error": self.message,
            "code": self.code,
            # … and the nested form the panel's ZtError reads.
            "message": self.message,
        }
        if self.retry_after is not None:
            payload["retry_after"] = self.retry_after
        if self.details:
            payload["details"] = self.details
        # `error` as a string is the documented flat shape, but the panel's
        # reader looks for `error.message`; give it an object too, under a name
        # that keeps both readers happy.
        payload["error_detail"] = {"message": self.message, "code": self.code}
        return payload

    @property
    def headers(self) -> dict[str, str]:
        if self.retry_after is None:
            return {}
        return {"Retry-After": str(self.retry_after)}


# --------------------------------------------------------------------------- #
# Named constructors, so call sites read like sentences and codes never drift.
# --------------------------------------------------------------------------- #


def validation_error(message: str, **details: Any) -> StorageError:
    return StorageError(message, 400, "VALIDATION_ERROR", details=details or None)


def unauthorized(message: str = "Invalid or missing API key") -> StorageError:
    # The panel maps `INVALID_ID_TOKEN` to a 401 it can show.
    return StorageError(message, 401, "INVALID_ID_TOKEN")


def not_found(message: str) -> StorageError:
    return StorageError(message, 404, "NOT_FOUND")


def already_exists(collection: str, doc_id: str) -> StorageError:
    """`create_document` in the panel expects exactly this code."""
    return StorageError(
        f"Document {doc_id!r} already exists in {collection!r}",
        409,
        "DOCUMENT_ALREADY_EXISTS",
    )


def revision_conflict(
    collection: str, doc_id: str, expected: int, actual: int
) -> StorageError:
    return StorageError(
        f"Document is at revision {actual}, not {expected}",
        409,
        "REVISION_CONFLICT",
        details={"expected_rev": expected, "rev": actual},
    )


def deleted(collection: str, doc_id: str) -> StorageError:
    """A tombstone is not a document: reads treat it as missing."""
    return StorageError(
        f"Document {doc_id!r} in {collection!r} was deleted",
        404,
        "NOT_FOUND",
        details={"deleted": True},
    )


def flood_wait(seconds: int, where: str) -> StorageError:
    return StorageError(
        f"Telegram asked us to wait {seconds}s before writing to {where}",
        429,
        "RATE_LIMITED",
        retry_after=seconds,
    )


def channel_unavailable(key: str, reason: str) -> StorageError:
    return StorageError(
        f"Channel {key} is not usable: {reason}",
        503,
        "CHANNEL_UNAVAILABLE",
    )


def not_ready(reason: str = "the channel index is still being rebuilt") -> StorageError:
    return StorageError(
        f"The store is not ready yet — {reason}",
        503,
        "NOT_READY",
    )


def upstream_failure(message: str) -> StorageError:
    return StorageError(message, 502, "UPSTREAM_ERROR")
