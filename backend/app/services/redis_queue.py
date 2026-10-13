from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime

from redis import Redis
from redis.exceptions import RedisError
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from backend.app.core.config import get_settings
from backend.app.models.domain import Candidate, DoNotContact, ExternalActionLog, Interview, QueueDlqRecord, QueueOutbox, QueueRecoveryCursor, QueueReplayRecord, QueueTask, SystemError
from backend.app.services.queue import LEASE_SECONDS, claim_task, enqueue_outbox_for_task, process_task, reclaim_stale_running_tasks, renew_task_lease_by_id
from backend.app.services.queue_utils import parse_time, utc_now


DEFAULT_GROUP = get_settings().redis_consumer_group
PENDING_IDLE_MS = int(get_settings().redis_pending_idle_ms)
DLQ_MAX_DELIVERIES = int(get_settings().redis_dlq_max_deliveries)
PUBLISHED_LOSS_CURSOR = "redis_published_outbox_loss"
STREAM_DISCOVERY_WARNING = "configured_streams"


def redis_client() -> Redis:
    return Redis.from_url(get_settings().redis_url, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)


def serialize_outbox_payload(row: QueueOutbox) -> dict[str, str]:
    return {
        "payload": json.dumps(row.payload or {}, ensure_ascii=False, sort_keys=True),
        "queue_task_id": str(row.queue_task_id),
        "outbox_id": str(row.id),
        "delivery_revision": str(row.delivery_revision or 1),
        "event_type": row.event_type,
    }


def publish_pending_outbox(db: Session, *, limit: int = 100, client: Redis | None = None) -> int:
    redis = client or redis_client()
    now = utc_now()
    rows = db.execute(
        select(QueueOutbox)
        .join(QueueTask, QueueTask.id == QueueOutbox.queue_task_id)
        .where(QueueOutbox.status.in_(["Pending", "Retry"]))
        .where(QueueTask.status.in_(["Queued", "Retry"]))
        .where(QueueTask.delivery_revision == QueueOutbox.delivery_revision)
        .where(or_(QueueTask.scheduled_at.is_(None), QueueTask.scheduled_at <= now))
        .order_by(QueueOutbox.created_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    ).scalars().all()
    published = 0
    for row in rows:
        try:
            stream_id = redis.xadd(row.stream_name, serialize_outbox_payload(row))
        except RedisError as exc:
            row.status = "Retry"
            row.retry_count += 1
            row.last_error = str(exc)
            continue
        row.status = "Published"
        row.published_at = utc_now()
        row.redis_stream_id = stream_id
        row.last_error = None
        task = db.get(QueueTask, row.queue_task_id)
        if task and int(task.delivery_revision or 1) == int(row.delivery_revision or 1):
            task.redis_stream_id = stream_id
        published += 1
    db.commit()
    return published


def _get_recovery_cursor(db: Session, name: str) -> QueueRecoveryCursor:
    cursor = db.get(QueueRecoveryCursor, name)
    if cursor:
        return cursor
    cursor = QueueRecoveryCursor(name=name, cursor_id=0, metadata_json={})
    db.add(cursor)
    db.flush()
    return cursor


def repair_lost_published_outbox(db: Session, *, limit: int = 100, scan_limit: int = 1000, client: Redis | None = None) -> int:
    """Return DB-published rows to Retry when their Redis stream entry is gone."""
    redis = client or redis_client()
    cursor = _get_recovery_cursor(db, PUBLISHED_LOSS_CURSOR)
    repaired = 0
    scanned = 0
    cursor_id = int(cursor.cursor_id or 0)
    page_size = min(max(limit, 1), 100)
    wrapped = False
    while scanned < scan_limit and repaired < limit:
        rows = db.execute(
            select(QueueOutbox)
            .join(QueueTask, QueueTask.id == QueueOutbox.queue_task_id)
            .where(QueueOutbox.id > cursor_id)
            .where(QueueOutbox.status == "Published")
            .where(QueueOutbox.redis_stream_id.is_not(None))
            .where(QueueTask.status.in_(["Queued", "Retry"]))
            .where(QueueTask.delivery_revision == QueueOutbox.delivery_revision)
            .order_by(QueueOutbox.id)
            .limit(min(page_size, scan_limit - scanned))
            .with_for_update(skip_locked=True)
        ).scalars().all()
        if not rows:
            if cursor_id == 0 or wrapped:
                break
            cursor_id = 0
            cursor.cursor_id = 0
            cursor.wrapped_at = utc_now()
            wrapped = True
            continue
        for row in rows:
            cursor_id = max(cursor_id, int(row.id or 0))
            cursor.cursor_id = cursor_id
            scanned += 1
            try:
                exists = bool(redis.xrange(row.stream_name, min=row.redis_stream_id, max=row.redis_stream_id, count=1))
            except RedisError as exc:
                row.last_error = f"Redis stream verification failed: {exc}"
                continue
            if exists:
                continue
            row.status = "Retry"
            row.redis_stream_id = None
            row.published_at = None
            row.retry_count += 1
            row.last_error = "Redis stream entry was missing; DB outbox scheduled republish."
            task = db.get(QueueTask, row.queue_task_id)
            if task and int(task.delivery_revision or 1) == int(row.delivery_revision or 1):
                task.redis_stream_id = None
            repaired += 1
            if repaired >= limit or scanned >= scan_limit:
                break
    cursor.metadata_json = {"last_scan_at": utc_now(), "last_scanned": scanned, "last_repaired": repaired}
    if repaired or scanned or wrapped:
        db.commit()
    return repaired


def ensure_consumer_groups(streams: Iterable[str], *, group: str | None = None, client: Redis | None = None) -> None:
    redis = client or redis_client()
    group = group or DEFAULT_GROUP
    for stream in streams:
        try:
            redis.xgroup_create(stream, group, id="0", mkstream=True)
        except RedisError as exc:
            if "BUSYGROUP" not in str(exc):
                raise


def _throttled_system_warning(db: Session, *, feature: str, message: str, impact: str) -> None:
    existing = db.scalar(
        select(SystemError).where(
            SystemError.service == "RedisQueue",
            SystemError.feature == feature,
            SystemError.status == "Open",
        )
    )
    if existing:
        existing.retry_count = int(existing.retry_count or 0) + 1
        existing.message = message
        existing.impact = impact
        return
    db.add(SystemError(service="RedisQueue", feature=feature, severity="Warning", message=message, impact=impact))


def configured_streams(db: Session, queue_names: set[str] | None = None) -> list[str]:
    query = select(QueueTask.queue_name).distinct().where(QueueTask.status.in_(["Queued", "Retry", "Running"]))
    if queue_names:
        query = query.where(QueueTask.queue_name.in_(queue_names))
    names = {row for row in db.execute(query).scalars().all()}
    outbox_query = select(QueueOutbox.stream_name).distinct().where(QueueOutbox.status.in_(["Pending", "Retry", "Published"]))
    if queue_names:
        outbox_query = outbox_query.where(QueueOutbox.stream_name.in_({f"recruitment:{name}" for name in queue_names}))
    outbox_names = db.execute(outbox_query).scalars().all()
    streams = {f"recruitment:{name}" for name in names}
    streams.update(outbox_names)
    if len(streams) > 500:
        _throttled_system_warning(
            db,
            feature=STREAM_DISCOVERY_WARNING,
            message=f"Redis worker discovered {len(streams)} configured streams.",
            impact="All streams were included; review queue registration or worker filters to keep operations bounded.",
        )
        db.flush()
    return sorted(streams)


def cleanup_stale_dlq_indexes(redis: Redis, *, limit: int = 100, scan_count: int = 100) -> int:
    cleaned = 0
    pattern = "recruitment:dlq:index:*"
    if not hasattr(redis, "scan_iter"):
        return 0
    for key in redis.scan_iter(match=pattern, count=max(1, min(scan_count, 1000))):
        if cleaned >= limit:
            break
        indexed = redis.get(key)
        if not indexed or indexed == "__pending__":
            redis.delete(key)
            cleaned += 1
            continue
        # The index key encodes source stream/message, so only remove obviously stale
        # indexes whose target DLQ entry is unavailable when no DLQ stream can be inferred.
        if isinstance(indexed, str) and indexed.count("-") == 1:
            continue
    return cleaned


def _decode_message(fields: dict) -> tuple[int | None, int | None, dict | None, str | None]:
    payload = fields.get("payload") or "{}"
    try:
        decoded = json.loads(payload)
    except (TypeError, json.JSONDecodeError) as exc:
        return None, None, None, f"Redis message payload is not valid JSON: {exc}"
    if not isinstance(decoded, dict):
        return None, None, None, "Redis message payload must be a JSON object."
    queue_task_id = decoded.get("queue_task_id") or fields.get("queue_task_id")
    revision = decoded.get("delivery_revision") or fields.get("delivery_revision") or 1
    try:
        task_id = int(queue_task_id) if queue_task_id is not None else None
        delivery_revision = int(revision)
    except (TypeError, ValueError) as exc:
        return None, None, decoded, f"Redis message has invalid queue_task_id or delivery_revision: {exc}"
    return task_id, delivery_revision, decoded, None


def _ack(redis: Redis, stream: str, group: str, message_id: str) -> None:
    redis.xack(stream, group, message_id)


def _int_or_none(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _dlq_index_key(stream: str, message_id: str) -> str:
    digest = hashlib.sha256(f"{stream}:{message_id}".encode("utf-8")).hexdigest()
    return f"recruitment:dlq:index:{digest}"


def _redis_stream_id_exists(redis: Redis, stream: str, stream_id: str | None) -> bool:
    if not stream_id:
        return False
    rows = redis.xrange(stream, min=stream_id, max=stream_id, count=1) or []
    return bool(rows and rows[0][0] == stream_id)


def _atomic_indexed_xadd(redis: Redis, *, dlq_stream: str, index_key: str, fields: dict[str, str]) -> str:
    argv: list[str] = []
    for key, value in fields.items():
        argv.extend([str(key), str(value)])
    script = """
local indexed = redis.call('GET', KEYS[2])
if indexed and indexed ~= '__pending__' then
  local found = redis.call('XRANGE', KEYS[1], indexed, indexed, 'COUNT', 1)
  if #found > 0 then
    return indexed
  end
end
local entry_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV))
redis.call('SET', KEYS[2], entry_id)
return entry_id
"""
    if hasattr(redis, "eval"):
        return redis.eval(script, 2, dlq_stream, index_key, *argv)
    dlq_stream_id = redis.xadd(dlq_stream, fields)
    if hasattr(redis, "set"):
        redis.set(index_key, dlq_stream_id)
    return dlq_stream_id


def _ensure_redis_dlq_entry(redis: Redis, *, dlq_stream: str, stream: str, message_id: str, fields: dict, reason: str) -> str:
    index_key = _dlq_index_key(stream, message_id)
    indexed = redis.get(index_key) if hasattr(redis, "get") else None
    if indexed and indexed != "__pending__" and _redis_stream_id_exists(redis, dlq_stream, indexed):
        return indexed
    return _atomic_indexed_xadd(
        redis,
        dlq_stream=dlq_stream,
        index_key=index_key,
        fields={**fields, "failed_stream": stream, "failed_message_id": message_id, "dlq_reason": reason},
    )


def move_to_dlq(db: Session, *, stream: str, message_id: str, fields: dict, reason: str, client: Redis, group: str) -> None:
    dlq_stream = f"{stream}:dlq"
    queue_task_id, revision, _decoded, _decode_error = _decode_message(fields)
    raw_queue_task_id = fields.get("queue_task_id")
    if raw_queue_task_id is None and isinstance(_decoded, dict):
        raw_queue_task_id = _decoded.get("queue_task_id")
    raw_outbox_id = fields.get("outbox_id")
    outbox_id = _int_or_none(raw_outbox_id)
    if queue_task_id is not None and not db.get(QueueTask, queue_task_id):
        queue_task_fk = None
    else:
        queue_task_fk = queue_task_id
    if outbox_id is not None and not db.get(QueueOutbox, outbox_id):
        outbox_fk = None
    else:
        outbox_fk = outbox_id
    record = db.scalar(select(QueueDlqRecord).where(QueueDlqRecord.stream_name == stream, QueueDlqRecord.redis_stream_id == message_id))
    if not record:
        record = QueueDlqRecord(stream_name=stream, redis_stream_id=message_id, reason=reason)
        db.add(record)
    record.queue_task_id = queue_task_fk
    record.outbox_id = outbox_fk
    record.delivery_revision = revision
    record.reason = reason
    record.status = "Moving"
    record.db_write_status = "Saved"
    record.metadata_json = {
        **(record.metadata_json or {}),
        "fields": fields,
        "raw_queue_task_id": raw_queue_task_id,
        "raw_outbox_id": raw_outbox_id,
        "invalid_queue_task_id": raw_queue_task_id if queue_task_id is not None and queue_task_fk is None else None,
        "invalid_outbox_id": raw_outbox_id if outbox_id is not None and outbox_fk is None else None,
        "consumer_group": group,
    }
    db.add(SystemError(service="RedisQueue", feature=stream, severity="Error", message=reason, impact="Message moved to DLQ after repeated failed delivery."))
    db.commit()
    try:
        if record.dlq_stream_id and _redis_stream_id_exists(client, dlq_stream, record.dlq_stream_id):
            dlq_stream_id = record.dlq_stream_id
        else:
            if record.dlq_stream_id:
                db.add(
                    SystemError(
                        service="RedisQueue",
                        feature=dlq_stream,
                        severity="Critical",
                        message=f"DLQ stream pointer {record.dlq_stream_id} was missing before ACK.",
                        impact="Original Redis message was not ACKed until a replacement DLQ entry was written.",
                    )
                )
            dlq_stream_id = _ensure_redis_dlq_entry(client, dlq_stream=dlq_stream, stream=stream, message_id=message_id, fields=fields, reason=reason)
        record.dlq_stream_id = dlq_stream_id
        record.redis_dlq_status = "Written"
        record.last_error = None
        db.commit()
    except Exception as exc:
        record.redis_dlq_status = "Failed"
        record.status = "Redis Failed"
        record.last_error = str(exc)
        db.commit()
        raise
    try:
        _ack(client, stream, group, message_id)
    except Exception as exc:
        record.ack_status = "Failed"
        record.status = "Ack Pending"
        record.last_error = str(exc)
        db.commit()
        raise
    record.ack_status = "Acked"
    record.status = "Open"
    record.last_error = None
    db.commit()


async def _process_redis_message(db: Session, *, redis: Redis, stream: str, group: str, worker_id: str, message_id: str, fields: dict) -> int:
    queue_task_id, revision, _decoded, decode_error = _decode_message(fields)
    if decode_error:
        move_to_dlq(db, stream=stream, message_id=message_id, fields=fields, reason=decode_error, client=redis, group=group)
        return 1
    if queue_task_id is None or revision is None:
        move_to_dlq(db, stream=stream, message_id=message_id, fields=fields, reason="Redis message missing queue_task_id.", client=redis, group=group)
        return 1
    task = db.get(QueueTask, queue_task_id)
    if not task:
        move_to_dlq(db, stream=stream, message_id=message_id, fields=fields, reason=f"QueueTask {queue_task_id} not found.", client=redis, group=group)
        return 1
    if int(task.delivery_revision or 1) != revision:
        _ack(redis, stream, group, message_id)
        return 1
    claimed = claim_task(db, task, worker_id=worker_id)
    if not claimed:
        if task.status in {"Completed", "Cancelled", "Manual Review Required"}:
            _ack(redis, stream, group, message_id)
            return 1
        return 0
    claimed.redis_stream_id = message_id
    claimed.redis_claimed_at = utc_now()
    db.commit()
    heartbeat_stop = asyncio.Event()
    ownership_lost = asyncio.Event()
    heartbeat = asyncio.create_task(
        _heartbeat_task(
            db,
            task_id=claimed.id,
            worker_id=worker_id,
            fencing_token=int(claimed.fencing_token or 0),
            stop_event=heartbeat_stop,
            ownership_lost=ownership_lost,
        )
    )
    try:
        await process_task(db, claimed)
        if ownership_lost.is_set():
            raise RuntimeError("Queue ownership was lost during task execution; refusing Redis ACK.")
    except Exception:
        db.rollback()
        raise
    finally:
        heartbeat_stop.set()
        await heartbeat
    _ack(redis, stream, group, message_id)
    return 1


TERMINAL_TASK_STATUSES = {"Completed", "Cancelled", "Manual Review Required"}


async def _heartbeat_task(
    db: Session,
    *,
    task_id: int,
    worker_id: str,
    fencing_token: int,
    stop_event: asyncio.Event,
    ownership_lost: asyncio.Event | None = None,
) -> None:
    interval = max(1, min(30, LEASE_SECONDS // 3))
    SessionForHeartbeat = sessionmaker(bind=db.get_bind(), autoflush=False, autocommit=False)
    consecutive_errors = 0
    last_successful_renewal = datetime.now(UTC)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
        try:
            renewed = await asyncio.to_thread(_renew_once, SessionForHeartbeat, task_id, worker_id, fencing_token)
            if not renewed:
                if await asyncio.to_thread(_task_is_terminal, SessionForHeartbeat, task_id):
                    stop_event.set()
                    break
                if ownership_lost:
                    ownership_lost.set()
                stop_event.set()
                break
            consecutive_errors = 0
            last_successful_renewal = datetime.now(UTC)
        except Exception:
            consecutive_errors += 1
            elapsed_since_success = (datetime.now(UTC) - last_successful_renewal).total_seconds()
            if consecutive_errors >= 3 and elapsed_since_success >= max(1, LEASE_SECONDS - 30):
                if ownership_lost:
                    ownership_lost.set()
                stop_event.set()
                break


def _renew_once(SessionForHeartbeat, task_id: int, worker_id: str, fencing_token: int) -> bool:
    with SessionForHeartbeat() as heartbeat_db:
        return renew_task_lease_by_id(heartbeat_db, task_id=task_id, worker_id=worker_id, fencing_token=fencing_token)


def _task_is_terminal(SessionForHeartbeat, task_id: int) -> bool:
    with SessionForHeartbeat() as heartbeat_db:
        status = heartbeat_db.execute(select(QueueTask.status).where(QueueTask.id == task_id)).scalar_one_or_none()
        return status in TERMINAL_TASK_STATUSES


def _lease_is_near_expiry(SessionForHeartbeat, task_id: int, *, safety_seconds: int = 30) -> bool:
    with SessionForHeartbeat() as heartbeat_db:
        row = heartbeat_db.execute(select(QueueTask.lease_expires_at).where(QueueTask.id == task_id)).first()
        if not row:
            return True
        lease = parse_time(row[0])
        if not lease:
            return True
        return (lease - datetime.now(UTC)).total_seconds() <= safety_seconds


async def consume_redis_once(
    db: Session,
    *,
    streams: list[str],
    worker_id: str,
    group: str | None = None,
    client: Redis | None = None,
    block_ms: int = 1000,
    count: int = 10,
) -> int:
    if not streams:
        return 0
    redis = client or redis_client()
    group = group or DEFAULT_GROUP
    ensure_consumer_groups(streams, group=group, client=redis)
    response = redis.xreadgroup(group, worker_id, {stream: ">" for stream in streams}, count=count, block=block_ms)
    processed = 0
    for stream, messages in response or []:
        for message_id, fields in messages:
            processed += await _process_redis_message(db, redis=redis, stream=stream, group=group, worker_id=worker_id, message_id=message_id, fields=fields)
    return processed


async def reclaim_pending_messages_async(
    db: Session,
    *,
    streams: list[str],
    worker_id: str,
    group: str | None = None,
    client: Redis | None = None,
    idle_ms: int = PENDING_IDLE_MS,
    limit: int = 50,
) -> int:
    redis = client or redis_client()
    group = group or DEFAULT_GROUP
    ensure_consumer_groups(streams, group=group, client=redis)
    reclaimed = 0
    for stream in streams:
        start = "-"
        while True:
            try:
                pending = redis.xpending_range(stream, group, min=start, max="+", count=limit)
            except RedisError:
                break
            if not pending:
                break
            stale_ids: list[str] = []
            last_id = None
            for item in pending:
                message_id = item.get("message_id") if isinstance(item, dict) else item[0]
                idle = item.get("time_since_delivered") if isinstance(item, dict) else item[2]
                deliveries = item.get("times_delivered") if isinstance(item, dict) else item[3]
                last_id = message_id
                if idle and int(idle) >= idle_ms:
                    queue_task_id, _revision, _decoded, _error = _decode_message((redis.xrange(stream, min=message_id, max=message_id, count=1) or [(None, {})])[0][1])
                    task = db.get(QueueTask, queue_task_id) if queue_task_id else None
                    lease = parse_time(task.lease_expires_at) if task else None
                    lease_valid = bool(task and task.status == "Running" and lease and lease > datetime.now(UTC))
                    if deliveries and int(deliveries) >= DLQ_MAX_DELIVERIES and not lease_valid:
                        messages = redis.xrange(stream, min=message_id, max=message_id, count=1)
                        fields = messages[0][1] if messages else {}
                        move_to_dlq(db, stream=stream, message_id=message_id, fields=fields, reason="Redis message exceeded pending delivery limit.", client=redis, group=group)
                        reclaimed += 1
                        continue
                    if not lease_valid:
                        stale_ids.append(message_id)
            if stale_ids:
                claimed = redis.xclaim(stream, group, worker_id, min_idle_time=idle_ms, message_ids=stale_ids)
                for message_id, fields in claimed or []:
                    reclaimed += await _process_redis_message(
                        db,
                        redis=redis,
                        stream=stream,
                        group=group,
                        worker_id=worker_id,
                        message_id=message_id,
                        fields=fields,
                    )
            if len(pending) < limit or not last_id:
                break
            start = f"({last_id}"
    return reclaimed


def reclaim_pending_messages(
    db: Session,
    *,
    streams: list[str],
    worker_id: str,
    group: str | None = None,
    client: Redis | None = None,
    idle_ms: int = PENDING_IDLE_MS,
    limit: int = 50,
) -> int:
    return asyncio.run(
        reclaim_pending_messages_async(
            db,
            streams=streams,
            worker_id=worker_id,
            group=group,
            client=client,
            idle_ms=idle_ms,
            limit=limit,
        )
    )


def replay_task(
    db: Session,
    task_id: int,
    *,
    expected_delivery_revision: int | None = None,
    replay_request_key: str | None = None,
    dlq_record_id: int | None = None,
) -> QueueTask:
    current = db.get(QueueTask, task_id)
    if not current:
        raise ValueError(f"QueueTask {task_id} not found")
    replay_record = None
    if replay_request_key:
        replay_record = db.scalar(select(QueueReplayRecord).where(QueueReplayRecord.replay_request_key == replay_request_key))
        if replay_record:
            if replay_record.queue_task_id != task_id:
                raise ValueError("Replay request key was already used for a different QueueTask.")
            if expected_delivery_revision is not None and replay_record.expected_delivery_revision not in {None, int(expected_delivery_revision)}:
                raise ValueError("Replay request key was already used with a different expected delivery revision.")
            if replay_record.dlq_record_id != dlq_record_id:
                raise ValueError("Replay request key was already used with a different DLQ record.")
            if replay_record.status == "Succeeded":
                return db.get(QueueTask, task_id)
            if replay_record.status == "In Progress":
                return current
        else:
            replay_record = QueueReplayRecord(
                replay_request_key=replay_request_key,
                queue_task_id=task_id,
                dlq_record_id=dlq_record_id,
                expected_delivery_revision=expected_delivery_revision,
                status="In Progress",
                metadata_json={"created_by": "replay_task"},
            )
            db.add(replay_record)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                return replay_task(
                    db,
                    task_id,
                    expected_delivery_revision=expected_delivery_revision,
                    replay_request_key=replay_request_key,
                    dlq_record_id=dlq_record_id,
                )
    current_revision = int(current.delivery_revision or 1)
    if current.task_type.startswith("privacy."):
        raise ValueError(f"QueueTask {task_id} in task_type {current.task_type} cannot be replayed.")
    if int(current.retry_count or 0) >= int(current.max_retries or 0):
        raise ValueError("QueueTask retry budget is exhausted.")
    if expected_delivery_revision is not None and current_revision != int(expected_delivery_revision):
        if replay_record:
            replay_record.status = "Failed"
            replay_record.failure_reason = f"QueueTask {task_id} delivery_revision {current_revision} does not match expected {expected_delivery_revision}."
            db.commit()
        raise ValueError(f"QueueTask {task_id} delivery_revision {current_revision} does not match expected {expected_delivery_revision}.")
    now = utc_now()
    replay_payload = {**(current.payload or {})}
    if replay_request_key:
        replay_payload["last_replay_request_key"] = replay_request_key
        replay_payload["last_replay_requested_at"] = now
    result = db.execute(
        update(QueueTask)
        .where(QueueTask.id == task_id)
        .where(QueueTask.status.not_in(["Running", "Completed", "Cancelled"]))
        .where(QueueTask.task_type.not_like("privacy.%"))
        .where(QueueTask.retry_count < QueueTask.max_retries)
        .where(QueueTask.delivery_revision == current_revision)
        .values(
            payload=replay_payload,
            status="Retry",
            scheduled_at=now,
            locked_at=None,
            locked_by=None,
            lease_expires_at=None,
            redis_stream_id=None,
            redis_claimed_at=None,
            delivery_revision=QueueTask.delivery_revision + 1,
            last_error=None,
        )
        .returning(QueueTask.id)
    ).first()
    if result is None:
        db.rollback()
        fresh = db.get(QueueTask, task_id)
        if not fresh:
            raise ValueError(f"QueueTask {task_id} not found")
        raise ValueError(f"QueueTask {task_id} in status {fresh.status} cannot be replayed.")
    db.expire_all()
    task = db.get(QueueTask, task_id)
    outbox = enqueue_outbox_for_task(db, task)
    if replay_record:
        replay_record.status = "Succeeded"
        replay_record.replayed_delivery_revision = int(task.delivery_revision or 1)
        replay_record.replayed_outbox_id = outbox.id
        replay_record.metadata_json = {**(replay_record.metadata_json or {}), "completed_at": utc_now()}
    db.commit()
    db.refresh(task)
    return task


def replay_dlq_record(db: Session, dlq_record_id: int) -> QueueDlqRecord:
    claimed = db.execute(
        update(QueueDlqRecord)
        .where(QueueDlqRecord.id == dlq_record_id)
        .where(QueueDlqRecord.status.in_(["Open", "Replay Failed"]))
        .values(status="Replay In Progress", replay_started_at=utc_now(), last_error=None)
        .returning(QueueDlqRecord.id)
    ).first()
    if claimed is None:
        db.rollback()
        record = db.get(QueueDlqRecord, dlq_record_id)
        if not record:
            raise ValueError(f"QueueDlqRecord {dlq_record_id} not found")
        raise ValueError(f"QueueDlqRecord {dlq_record_id} in status {record.status} cannot be replayed.")
    db.commit()
    record = db.get(QueueDlqRecord, dlq_record_id)
    if not record:
        raise ValueError(f"QueueDlqRecord {dlq_record_id} not found")
    if record.queue_task_id is None:
        record.status = "Replay Blocked"
        record.last_error = "DLQ record has no queue_task_id to replay."
        db.commit()
        raise ValueError(record.last_error)
    task = db.get(QueueTask, record.queue_task_id)
    if not task:
        record.status = "Replay Blocked"
        record.last_error = f"QueueTask {record.queue_task_id} not found"
        db.commit()
        raise ValueError(record.last_error)
    if record.delivery_revision is not None and int(record.delivery_revision) != int(task.delivery_revision or 1):
        record.status = "Replay Blocked"
        record.last_error = f"DLQ revision {record.delivery_revision} does not match QueueTask revision {task.delivery_revision}."
        db.commit()
        raise ValueError(record.last_error)
    if task.status in {"Completed", "Cancelled", "Running"} or task.task_type.startswith("privacy."):
        record.status = "Replay Blocked"
        record.last_error = f"QueueTask {task.id} in status {task.status} cannot be replayed."
        db.commit()
        raise ValueError(record.last_error)
    if int(task.retry_count or 0) >= int(task.max_retries or 0):
        record.status = "Replay Blocked"
        record.last_error = "QueueTask retry budget is exhausted."
        db.commit()
        raise ValueError(record.last_error)
    if db.scalar(select(ExternalActionLog).where(ExternalActionLog.queue_task_id == task.id, ExternalActionLog.status.in_(["Accepted", "Sent", "Delivered", "Read", "Verified", "Booked", "Completed"]))):
        record.status = "Replay Blocked"
        record.last_error = "QueueTask already has a completed external action."
        db.commit()
        raise ValueError(record.last_error)
    candidate_id = (task.payload or {}).get("candidate_id")
    if not candidate_id and (task.payload or {}).get("conversation_id"):
        from backend.app.models.domain import Conversation

        conversation = db.get(Conversation, (task.payload or {}).get("conversation_id"))
        candidate_id = conversation.candidate_id if conversation else None
    if candidate_id:
        candidate = db.get(Candidate, candidate_id)
        if not candidate or "deleted" in (candidate.tags or []):
            record.status = "Replay Blocked"
            record.last_error = "Candidate was deleted or anonymized."
            db.commit()
            raise ValueError(record.last_error)
        if db.scalar(select(DoNotContact).where(DoNotContact.candidate_id == candidate_id, DoNotContact.status == "Active")):
            record.status = "Replay Blocked"
            record.last_error = "Candidate is on the do-not-contact list."
            db.commit()
            raise ValueError(record.last_error)
    interview_id = (task.payload or {}).get("interview_id")
    if interview_id:
        interview = db.get(Interview, interview_id)
        expected_version = (task.payload or {}).get("booking_version")
        if not interview or interview.status in {"Cancelled", "Canceled"} or getattr(interview, "attendance_status", None) == "Arrived":
            record.status = "Replay Blocked"
            record.last_error = "Interview is missing, cancelled, or already attended."
            db.commit()
            raise ValueError(record.last_error)
        if expected_version:
            from backend.app.services.queue import interview_booking_version

            if expected_version != interview_booking_version(interview):
                record.status = "Replay Blocked"
                record.last_error = "Interview booking version changed."
                db.commit()
                raise ValueError(record.last_error)
    try:
        replay_key = f"dlq-replay:{record.id}:{record.delivery_revision}"
        replayed = replay_task(db, task.id, expected_delivery_revision=record.delivery_revision, replay_request_key=replay_key, dlq_record_id=record.id)
    except ValueError as exc:
        record = db.get(QueueDlqRecord, dlq_record_id)
        record.status = "Replay Failed"
        record.last_error = str(exc)
        db.commit()
        raise
    record = db.get(QueueDlqRecord, dlq_record_id)
    record.status = "Replayed"
    record.replayed_task_id = replayed.id
    record.resolved_at = utc_now()
    record.last_error = None
    db.commit()
    db.refresh(record)
    return record


def reconcile_dlq_records(
    db: Session,
    *,
    client: Redis | None = None,
    group: str | None = None,
    limit: int = 50,
    replay_timeout_seconds: int = 300,
) -> int:
    redis = client or redis_client()
    group = group or DEFAULT_GROUP
    rows = db.execute(
        select(QueueDlqRecord)
        .where(QueueDlqRecord.status.in_(["Moving", "Redis Failed", "Ack Pending", "Replay In Progress"]))
        .order_by(QueueDlqRecord.created_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    ).scalars().all()
    repaired = 0
    for record in rows:
        try:
            if record.status in {"Moving", "Redis Failed", "Ack Pending"}:
                fields = dict((record.metadata_json or {}).get("fields") or {})
                reason = record.reason or "DLQ recovery"
                dlq_stream = f"{record.stream_name}:dlq"
                if record.dlq_stream_id and not _redis_stream_id_exists(redis, dlq_stream, record.dlq_stream_id):
                    db.add(
                        SystemError(
                            service="RedisQueue",
                            feature=dlq_stream,
                            severity="Critical",
                            message=f"Stored DLQ stream pointer {record.dlq_stream_id} was missing during recovery.",
                            impact="Recovery rewrote DLQ evidence before ACK reconciliation.",
                        )
                    )
                    record.dlq_stream_id = None
                    record.redis_dlq_status = "Missing"
                    db.commit()
                if not record.dlq_stream_id:
                    record.dlq_stream_id = _ensure_redis_dlq_entry(
                        redis,
                        dlq_stream=dlq_stream,
                        stream=record.stream_name,
                        message_id=record.redis_stream_id,
                        fields=fields,
                        reason=reason,
                    )
                    record.redis_dlq_status = "Written"
                    db.commit()
                if record.ack_status != "Acked":
                    _ack(redis, record.stream_name, (record.metadata_json or {}).get("consumer_group") or group, record.redis_stream_id)
                    record.ack_status = "Acked"
                record.status = "Open"
                record.last_error = None
                repaired += 1
            elif record.status == "Replay In Progress":
                task = db.get(QueueTask, record.queue_task_id) if record.queue_task_id else None
                replay_key = f"dlq-replay:{record.id}:{record.delivery_revision}"
                if (
                    task
                    and record.delivery_revision is not None
                    and int(task.delivery_revision or 1) > int(record.delivery_revision)
                    and db.scalar(select(QueueReplayRecord).where(QueueReplayRecord.replay_request_key == replay_key, QueueReplayRecord.status == "Succeeded"))
                ):
                    record.status = "Replayed"
                    record.replayed_task_id = task.id
                    record.resolved_at = utc_now()
                    record.last_error = None
                    repaired += 1
                else:
                    started = parse_time(record.replay_started_at)
                    if not started or (datetime.now(UTC) - started).total_seconds() >= replay_timeout_seconds:
                        record.status = "Replay Failed"
                        record.last_error = "Replay did not complete before timeout; safe to retry."
                        repaired += 1
            db.commit()
        except Exception as exc:
            record.last_error = str(exc)
            if record.status == "Moving":
                record.status = "Redis Failed"
                record.redis_dlq_status = "Failed"
            elif record.status == "Ack Pending":
                record.ack_status = "Failed"
            db.commit()
    return repaired


def reconcile_outbox(db: Session) -> dict:
    repaired_lost = repair_lost_published_outbox(db)
    dlq_repaired = reconcile_dlq_records(db)
    pending_count = db.scalar(select(func.count(QueueOutbox.id)).where(QueueOutbox.status.in_(["Pending", "Retry"]))) or 0
    orphaned = db.execute(
        select(QueueTask)
        .where(QueueTask.status.in_(["Queued", "Retry"]))
        .where(QueueTask.id.not_in(select(QueueOutbox.queue_task_id).where(QueueOutbox.delivery_revision == QueueTask.delivery_revision)))
        .order_by(QueueTask.id)
        .limit(100)
    ).scalars().all()
    for task in orphaned:
        enqueue_outbox_for_task(db, task)
    stale = reclaim_stale_running_tasks(db)
    db.commit()
    return {"pending_outbox": pending_count, "published_streams_repaired": repaired_lost, "dlq_records_reconciled": dlq_repaired, "orphaned_tasks_repaired": len(orphaned), "stale_running_reclaimed": stale}


async def run_redis_worker(db_factory, *, queue_names: set[str] | None = None, worker_id: str | None = None, stop_event: asyncio.Event | None = None, periodic_hook=None) -> None:
    worker_id = worker_id or f"worker:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"
    stop_event = stop_event or asyncio.Event()
    backoff = 1
    while not stop_event.is_set():
        try:
            with db_factory() as db:
                if periodic_hook:
                    periodic_hook(db)
                streams = configured_streams(db, queue_names)
                reconcile_outbox(db)
                publish_pending_outbox(db)
                await reclaim_pending_messages_async(db, streams=streams, worker_id=worker_id)
                processed = await consume_redis_once(db, streams=streams, worker_id=worker_id, block_ms=1000)
            backoff = 1
        except Exception as exc:
            try:
                with db_factory() as db:
                    db.add(SystemError(service="RedisQueue", feature="worker_loop", severity="Warning", message=str(exc), impact="Redis worker backed off and will retry instead of exiting."))
                    db.commit()
            except Exception:
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        if not processed:
            await asyncio.sleep(1)
