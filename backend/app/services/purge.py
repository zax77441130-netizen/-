from __future__ import annotations

import os
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.models.domain import ExternalRetentionPurge
from backend.app.services.queue_utils import utc_now


class PurgeAdapter:
    def __init__(self, purge: ExternalRetentionPurge):
        self.purge = purge

    def delete(self) -> dict:
        raise NotImplementedError

    def read_back_deleted(self) -> dict:
        raise NotImplementedError


class LocalFilePurgeAdapter(PurgeAdapter):
    def _target(self) -> Path:
        root = Path(os.getenv("CONTROLLED_STORAGE_ROOT", "data")).resolve()
        raw = Path(self.purge.object_key)
        if raw.is_absolute():
            target = raw.resolve()
        else:
            cwd_relative = (Path.cwd() / raw).resolve()
            target = cwd_relative if (root in cwd_relative.parents or cwd_relative == root) else (root / raw).resolve()
        if root not in target.parents and target != root:
            raise RuntimeError("Refusing to purge a file outside the controlled storage root.")
        return target

    def delete(self) -> dict:
        target = self._target()
        if target.exists():
            target.unlink()
        return {"provider": "local_file", "path": str(target), "delete_requested_at": utc_now()}

    def read_back_deleted(self) -> dict:
        target = self._target()
        return {"deleted": not target.exists(), "path": str(target)}


class HttpPurgeAdapter(PurgeAdapter):
    def _headers(self) -> dict:
        token = os.getenv("PURGE_PROVIDER_TOKEN")
        if not token:
            raise RuntimeError("PURGE_PROVIDER_TOKEN is not configured.")
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def delete(self) -> dict:
        endpoint = os.getenv("PURGE_PROVIDER_URL")
        if not endpoint:
            raise RuntimeError("PURGE_PROVIDER_URL is not configured.")
        response = httpx.post(endpoint, json={"object_key": self.purge.object_key, "purge_type": self.purge.purge_type}, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.json()

    def read_back_deleted(self) -> dict:
        endpoint = os.getenv("PURGE_PROVIDER_READBACK_URL")
        if not endpoint:
            raise RuntimeError("PURGE_PROVIDER_READBACK_URL is not configured.")
        response = httpx.get(endpoint, params={"object_key": self.purge.object_key, "purge_type": self.purge.purge_type}, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.json()


def request_external_purge(db: Session, *, provider: str, object_key: str, purge_type: str = "object", legal_hold: bool = False) -> ExternalRetentionPurge:
    existing = db.scalar(
        select(ExternalRetentionPurge).where(
            ExternalRetentionPurge.provider == provider,
            ExternalRetentionPurge.object_key == object_key,
            ExternalRetentionPurge.purge_type == purge_type,
        )
    )
    if existing:
        return existing
    purge = ExternalRetentionPurge(provider=provider, object_key=object_key, purge_type=purge_type, legal_hold=legal_hold, requested_at=utc_now())
    db.add(purge)
    db.flush()
    return purge


def _adapter_for(purge: ExternalRetentionPurge) -> PurgeAdapter:
    provider = purge.provider.lower()
    if provider in {"local", "local_file", "filesystem"}:
        return LocalFilePurgeAdapter(purge)
    if provider in {"http", "s3", "object_storage", "email_archive", "backup"}:
        return HttpPurgeAdapter(purge)
    raise RuntimeError(f"No purge adapter is configured for provider '{purge.provider}'.")


def execute_external_purge(purge: ExternalRetentionPurge, *, dry_run: bool = True) -> None:
    if purge.legal_hold:
        purge.status = "Legal Hold"
        purge.last_error = "Legal hold blocks purge."
        purge.evidence = {**(purge.evidence or {}), "legal_hold": True, "provider_deleted": False}
        return
    if dry_run:
        purge.status = "Dry Run - Not Deleted"
        purge.confirmed_at = None
        purge.evidence = {"dry_run": True, "provider_deleted": False, "note": "Simulation only; provider deletion was not executed."}
        return
    try:
        adapter = _adapter_for(purge)
        delete_evidence = adapter.delete()
        read_back = adapter.read_back_deleted()
    except Exception as exc:
        purge.retry_count += 1
        purge.status = "Manual Review Required"
        purge.last_error = str(exc)
        permanent = any(marker in str(exc).lower() for marker in ["not found", "404", "permission", "forbidden", "legal"])
        purge.evidence = {**(purge.evidence or {}), "provider_deleted": False, "error": str(exc), "permanent_failure": permanent}
        return
    if read_back.get("deleted") is True or read_back.get("exists") is False:
        purge.status = "Deleted"
        purge.confirmed_at = utc_now()
        purge.last_error = None
        purge.evidence = {"provider_deleted": True, "delete": delete_evidence, "read_back": read_back}
    else:
        purge.status = "Manual Review Required"
        purge.last_error = "Provider deletion was requested but read-back did not confirm deletion."
        purge.evidence = {"provider_deleted": False, "delete": delete_evidence, "read_back": read_back}
