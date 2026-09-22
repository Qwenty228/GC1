"""File-based ingestion ledger for source-identity duplicate tracking.

Identities (never compared across each other, never by text similarity):
    SEC     ``sec_edgar:<cik10>:<accession>`` - one completed record per filing;
            amendments (8-K/A) have their own accession, so they stay separate.
    Alpaca  ``alpaca:<article id>`` - tracks the latest saved ``updated_at``, the
            content level (2 full / 1 summary / 0 headline) and the fingerprints
            of payloads already saved *for that id*.

The processed per-record JSON files are authoritative. The ledger is an index
over them plus retry bookkeeping and collection cursors; `reconcile` rebuilds
completed entries from the files on startup, so a crash between writing a
record and updating the ledger never produces a second completed output.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .schema import PreparedRecord
from .store import OutputStore, atomic_write_json

LOGGER = logging.getLogger(__name__)
LEDGER_VERSION = 1
MAX_FINGERPRINTS_PER_ARTICLE = 50
MAX_RETRY_BACKOFF = timedelta(hours=6)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def _parse(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


@dataclass(frozen=True)
class Decision:
    process: bool
    reason: str


class Ledger:
    def __init__(self, store: OutputStore):
        self.store = store
        self.path = store.root / "state" / "ledger.json"
        self._lock = threading.RLock()
        self.data: dict[str, Any] = {"version": LEDGER_VERSION, "sec": {}, "alpaca": {}, "cursors": {}}

    # -- persistence -----------------------------------------------------------
    def load(self) -> None:
        with self._lock:
            if not self.path.exists():
                return
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if data.get("version") != LEDGER_VERSION:
                    raise ValueError(f"unsupported ledger version {data.get('version')!r}")
            except (OSError, ValueError) as error:
                aside = self.path.with_name(f"ledger.corrupt-{_now().strftime('%Y%m%dT%H%M%S')}.json")
                os.replace(self.path, aside)
                LOGGER.error("ledger unreadable (%s); moved to %s and rebuilding from processed files", error, aside.name)
                return
            for key in ("sec", "alpaca", "cursors"):
                data.setdefault(key, {})
            self.data = data

    def save(self) -> None:
        with self._lock:
            atomic_write_json(self.path, self.data)

    # -- SEC ------------------------------------------------------------------
    def sec_entry(self, identity: str) -> dict | None:
        return self.data["sec"].get(identity)

    def sec_decide(self, identity: str, now: datetime | None = None) -> Decision:
        entry = self.sec_entry(identity)
        if entry is None:
            return Decision(True, "new")
        if entry.get("status") == "completed":
            return Decision(False, "already_completed")
        next_retry = _parse(entry.get("next_retry_at"))
        if next_retry and (now or _now()) < next_retry:
            return Decision(False, "retry_backoff")
        return Decision(True, "retry")

    def mark_sec_completed(self, identity: str, output_ref: str, fingerprint: str | None) -> None:
        with self._lock:
            previous = self.data["sec"].get(identity, {})
            self.data["sec"][identity] = {
                "status": "completed",
                "output_path": output_ref,
                "content_fingerprint": fingerprint,
                "completed_at": _iso(_now()),
                "attempts": previous.get("attempts", 0) + 1,
            }
            self.save()

    def mark_sec_failed(self, identity: str, *, kind: str, message: str, retryable: bool,
                        base_delay: float = 60.0, error_ref: str | None = None) -> dict:
        """Record a failure. The filing stays pending and is retried after a backoff."""
        with self._lock:
            previous = self.data["sec"].get(identity, {})
            if previous.get("status") == "completed":
                return previous
            attempts = previous.get("attempts", 0) + 1
            if not retryable:
                delay = timedelta(hours=24)
            elif kind == "access_denied":
                delay = timedelta(hours=1) * min(attempts, 6)
            else:
                delay = min(timedelta(seconds=base_delay * (2 ** (attempts - 1))), MAX_RETRY_BACKOFF)
            entry = {
                "status": "failed",
                "attempts": attempts,
                "error_kind": kind,
                "last_error": message[:500],
                "retryable": retryable,
                "last_attempt_at": _iso(_now()),
                "next_retry_at": _iso(_now() + delay),
                "error_file": error_ref,
            }
            self.data["sec"][identity] = entry
            self.save()
            return entry

    # -- Alpaca ---------------------------------------------------------------
    def alpaca_entry(self, identity: str) -> dict | None:
        return self.data["alpaca"].get(identity)

    def alpaca_decide(self, identity: str, updated_at: datetime | None, fingerprint: str, level: int) -> Decision:
        entry = self.alpaca_entry(identity)
        if entry is None:
            return Decision(True, "new_article")
        if fingerprint in entry.get("fingerprints", []):
            return Decision(False, "unchanged_delivery")
        saved_updated = _parse(entry.get("updated_at"))
        saved_level = entry.get("content_level", 0)
        if updated_at is not None and saved_updated is not None:
            if updated_at > saved_updated:
                return Decision(True, "newer_revision")
            if updated_at < saved_updated:
                return Decision(False, "stale_revision")
            if level > saved_level:
                return Decision(True, "richer_payload")
            return Decision(False, "same_revision_not_richer")
        if updated_at is not None and saved_updated is None:
            return Decision(True, "revision_timestamp_now_available")
        if level > saved_level:
            return Decision(True, "richer_payload")
        return Decision(False, "no_valid_updated_at_and_not_richer")

    def mark_alpaca_saved(self, identity: str, *, updated_at: datetime | None, fingerprint: str,
                          level: int, output_ref: str) -> None:
        with self._lock:
            entry = self.data["alpaca"].get(identity) or {"fingerprints": []}
            self._merge_alpaca(entry, updated_at, fingerprint, level, output_ref)
            self.data["alpaca"][identity] = entry
            self.save()

    @staticmethod
    def _merge_alpaca(entry: dict, updated_at: datetime | None, fingerprint: str, level: int, output_ref: str) -> None:
        fingerprints = entry.setdefault("fingerprints", [])
        if fingerprint not in fingerprints:
            fingerprints.append(fingerprint)
            del fingerprints[:-MAX_FINGERPRINTS_PER_ARTICLE]
        saved_updated = _parse(entry.get("updated_at"))
        is_latest = (
            "output_path" not in entry
            or (updated_at is not None and (saved_updated is None or updated_at > saved_updated))
            or (updated_at == saved_updated and level >= entry.get("content_level", 0))
        )
        if is_latest:
            entry.update({"updated_at": _iso(updated_at), "content_level": level,
                          "latest_fingerprint": fingerprint, "output_path": output_ref,
                          "saved_at": _iso(_now())})

    # -- cursors --------------------------------------------------------------
    def get_cursor(self, name: str) -> datetime | None:
        return _parse(self.data["cursors"].get(name))

    def set_cursor(self, name: str, value: datetime, *, save: bool = True) -> None:
        with self._lock:
            self.data["cursors"][name] = _iso(value)
            if save:
                self.save()

    def get_value(self, name: str) -> Any:
        return self.data["cursors"].get(name)

    def set_value(self, name: str, value: Any) -> None:
        with self._lock:
            self.data["cursors"][name] = value
            self.save()

    # -- reconciliation -------------------------------------------------------
    def reconcile(self) -> dict[str, int]:
        """Make completed ledger entries agree with processed files (files win)."""
        with self._lock:
            summary = {"temp_files_removed": self.store.cleanup_temp_files(), "sec_added": 0,
                       "sec_dropped": 0, "alpaca_rebuilt": 0, "alpaca_dropped": 0, "quarantined": 0}
            sec_found: dict[str, tuple[str, str | None]] = {}
            alpaca_found: dict[str, list[tuple[datetime | None, int, str, str]]] = {}
            for path, doc in self.store.iter_processed():
                try:
                    record = PreparedRecord.model_validate(doc)
                except ValidationError:
                    ref = self.store.quarantine(path)
                    LOGGER.error("invalid processed record moved to %s", ref)
                    summary["quarantined"] += 1
                    continue
                dedup = record.provenance.dedup
                ref = self.store.rel(path)
                if record.record_type == "sec_filing":
                    sec_found[dedup.identity] = (ref, dedup.content_fingerprint)
                else:
                    alpaca_found.setdefault(dedup.identity, []).append(
                        (dedup.revision_updated_at, dedup.content_level, dedup.content_fingerprint or "", ref))

            for identity, entry in list(self.data["sec"].items()):
                if entry.get("status") == "completed" and identity not in sec_found:
                    del self.data["sec"][identity]
                    summary["sec_dropped"] += 1
            for identity, (ref, fingerprint) in sec_found.items():
                entry = self.data["sec"].get(identity)
                if not entry or entry.get("status") != "completed" or entry.get("output_path") != ref:
                    self.data["sec"][identity] = {
                        "status": "completed", "output_path": ref, "content_fingerprint": fingerprint,
                        "completed_at": _iso(_now()), "attempts": (entry or {}).get("attempts", 1),
                        "reconciled": True,
                    }
                    summary["sec_added"] += 1

            for identity in list(self.data["alpaca"]):
                if identity not in alpaca_found:
                    del self.data["alpaca"][identity]
                    summary["alpaca_dropped"] += 1
            epoch = datetime.min.replace(tzinfo=timezone.utc)
            for identity, revisions in alpaca_found.items():
                revisions.sort(key=lambda r: (r[0] or epoch, r[1]))
                rebuilt: dict = {"fingerprints": []}
                for updated_at, level, fingerprint, ref in revisions:
                    self._merge_alpaca(rebuilt, updated_at, fingerprint, level, ref)
                current = self.data["alpaca"].get(identity)
                comparable = {k: v for k, v in (current or {}).items() if k != "saved_at"}
                if {k: v for k, v in rebuilt.items() if k != "saved_at"} != comparable:
                    if current and "saved_at" in current and current.get("output_path") == rebuilt.get("output_path"):
                        rebuilt["saved_at"] = current["saved_at"]
                    self.data["alpaca"][identity] = rebuilt
                    summary["alpaca_rebuilt"] += 1
            self.save()
            return summary

    def counts(self) -> dict[str, int]:
        sec_completed = sum(1 for e in self.data["sec"].values() if e.get("status") == "completed")
        return {
            "sec_completed": sec_completed,
            "sec_pending_retry": len(self.data["sec"]) - sec_completed,
            "alpaca_articles": len(self.data["alpaca"]),
            "alpaca_revisions": sum(len(e.get("fingerprints", [])) for e in self.data["alpaca"].values()),
        }


def ledger_path(root: Path) -> Path:
    return root / "state" / "ledger.json"
