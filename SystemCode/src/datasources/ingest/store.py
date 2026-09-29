"""File layout and atomic writes for the ingestion worker.

    <root>/raw/sec/submissions/CIK##########/<sha12>.json    submissions snapshots (only when changed)
    <root>/raw/sec/submissions/CIK##########/historical/*.json
    <root>/raw/sec/documents/<cik10>/<accession>/<primaryDocument>
    <root>/raw/alpaca/rest/<YYYYMMDD>/<HHMMSS>_<sha12>.json  REST pages as received
    <root>/raw/alpaca/articles/<id>/<sha12>.json              one file per distinct article payload
    <root>/raw/alpaca/websocket/<YYYYMMDD>.jsonl              WebSocket frames (diagnostic log)
    <root>/processed/sec/<cik10>/<accession>.json             authoritative completed records
    <root>/processed/alpaca/<id>/<revision>.json              one file per article revision
    <root>/state/ledger.json, <root>/state/ingest.lock
    <root>/errors/<provider>/<identity>/<timestamp>.json
    <root>/logs/ingest.log

Completed output is written to a temp file in the destination directory, flushed
and fsync'd, then ``os.replace``'d into place, so a reader never sees a partial
record. The path is deterministic for a source record / revision.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .schema import PreparedRecord

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")
SUBDIRS = (
    "raw/sec", "raw/alpaca", "processed/sec", "processed/alpaca", "state", "errors", "logs",
)


def safe_name(value: str) -> str:
    return _SAFE.sub("_", value).strip("._") or "_"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


REPLACE_ATTEMPTS = 10


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(REPLACE_ATTEMPTS):
            try:
                os.replace(tmp, path)
                break
            except PermissionError:  # Windows: a reader (e.g. the status endpoint) has the target open briefly
                if attempt == REPLACE_ATTEMPTS - 1:
                    raise
                time.sleep(0.02)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def atomic_write_json(path: Path, value: Any, *, pretty: bool = True) -> None:
    text = json.dumps(value, ensure_ascii=False, indent=2 if pretty else None, default=str)
    atomic_write_bytes(path, (text + "\n").encode("utf-8"))


class OutputStore:
    def __init__(self, root: Path):
        self.root = Path(root)

    def ensure_dirs(self) -> None:
        for sub in SUBDIRS:
            (self.root / sub).mkdir(parents=True, exist_ok=True)

    def rel(self, path: Path) -> str:
        return path.relative_to(self.root).as_posix()

    def abs(self, ref: str) -> Path:
        return self.root / ref

    # -- raw --------------------------------------------------------------------
    def save_raw_submissions(self, cik10: str, body: bytes) -> str:
        digest = hashlib.sha256(body).hexdigest()[:12]
        path = self.root / "raw/sec/submissions" / f"CIK{cik10}" / f"{digest}.json"
        if not path.exists():
            atomic_write_bytes(path, body)
        return self.rel(path)

    def save_raw_historical(self, cik10: str, name: str, body: bytes) -> str:
        path = self.root / "raw/sec/submissions" / f"CIK{cik10}" / "historical" / safe_name(name)
        atomic_write_bytes(path, body)
        return self.rel(path)

    def load_raw_historical(self, cik10: str, name: str) -> bytes | None:
        path = self.root / "raw/sec/submissions" / f"CIK{cik10}" / "historical" / safe_name(name)
        return path.read_bytes() if path.exists() else None

    def save_raw_document(self, cik10: str, accession: str, primary_document: str, body: bytes) -> str:
        path = (self.root / "raw/sec/documents" / cik10 / accession / safe_name(primary_document.replace("/", "__")))
        atomic_write_bytes(path, body)
        return self.rel(path)

    def save_raw_alpaca_page(self, payload: dict, request_url: str, retrieved_at: datetime) -> str:
        body = json.dumps({"request_url": request_url, "retrieved_at": retrieved_at.isoformat(), "response": payload},
                          ensure_ascii=False, indent=2)
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]
        path = self.root / "raw/alpaca/rest" / retrieved_at.strftime("%Y%m%d") / f"{retrieved_at.strftime('%H%M%S')}_{digest}.json"
        atomic_write_bytes(path, (body + "\n").encode("utf-8"))
        return self.rel(path)

    def save_raw_alpaca_article(self, article: dict, fingerprint: str, *, transport: str,
                                api_endpoint: str, retrieved_at: datetime) -> str:
        path = self.root / "raw/alpaca/articles" / safe_name(str(article.get("id"))) / f"{fingerprint[:12]}.json"
        if not path.exists():
            atomic_write_json(path, {"transport": transport, "api_endpoint": api_endpoint,
                                     "retrieved_at": retrieved_at.isoformat(), "article": article})
        return self.rel(path)

    def append_ws_frame(self, raw_message: str | bytes, received_at: datetime) -> str:
        """Diagnostic JSONL log of WebSocket frames (not authoritative output)."""
        decoded = json.loads(raw_message)
        path = self.root / "raw/alpaca/websocket" / f"{received_at.strftime('%Y%m%d')}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"received_at": received_at.isoformat(), "frame": decoded}, ensure_ascii=False) + "\n")
        return self.rel(path)

    # -- processed ----------------------------------------------------------------
    def processed_path(self, record: PreparedRecord) -> Path:
        if record.record_type == "sec_filing":
            assert record.sec_metadata is not None
            return self.root / "processed/sec" / record.sec_metadata.cik / f"{safe_name(record.source_record_id)}.json"
        revision = record.record_id.split(":", 2)[2]
        return self.root / "processed/alpaca" / safe_name(record.source_record_id) / f"{safe_name(revision)}.json"

    def write_processed(self, record: PreparedRecord) -> str:
        path = self.processed_path(record)
        atomic_write_json(path, record.model_dump(mode="json"))
        return self.rel(path)

    def iter_processed(self) -> Iterator[tuple[Path, Any]]:
        for sub in ("processed/sec", "processed/alpaca"):
            base = self.root / sub
            if not base.exists():
                continue
            for path in sorted(base.glob("*/*.json")):
                try:
                    yield path, json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    yield path, None

    def cleanup_temp_files(self) -> int:
        removed = 0
        for path in self.root.glob("**/.*.tmp"):
            try:
                path.unlink()
                removed += 1
            except OSError:
                pass
        return removed

    def quarantine(self, path: Path) -> str:
        target = self.root / "errors/quarantine" / f"{_utcnow().strftime('%Y%m%dT%H%M%S')}_{safe_name(self.rel(path))}"
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(path, target)
        return self.rel(target)

    # -- errors -------------------------------------------------------------------
    def write_error(self, provider: str, identity: str, payload: dict) -> str:
        now = _utcnow()
        path = self.root / "errors" / provider / safe_name(identity) / f"{now.strftime('%Y%m%dT%H%M%S%fZ')}.json"
        atomic_write_json(path, {"recorded_at": now.isoformat(), **payload})
        return self.rel(path)
