import hashlib
import re
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select, update
from sqlalchemy.orm import Session

from backend.app.connectors.registry import get_adapter
from backend.app.core.config import get_settings
from backend.app.models.domain import (
    Application,
    AuditLog,
    Candidate,
    CandidateMergeConflict,
    Conversation,
    DataRetentionRun,
    EmailCursor,
    ExternalActionLog,
    DoNotContact,
    InboundEmail,
    Interview,
    Job,
    Message,
    PhoneCallLog,
    PlatformWebhookEvent,
    PlatformAccount,
    PlatformAutomationRun,
    PlatformIdentityMap,
    PlatformRateLimitState,
    Post,
    QueueDlqRecord,
    QueueTask,
    QueueOutbox,
    RecruitmentBlacklist,
    Resume,
    SystemError,
    AIRequest,
    StoredDocument,
    StorageObject,
    ExternalRetentionPurge,
)
from backend.app.services.calendar import book_interview_calendar
from backend.app.services.email_worker import acknowledge_imap_messages, parse_fixture_mailbox, poll_imap
from backend.app.services.flow import (
    UNSUITABLE_STATUSES,
    cancel_pending_interview_tasks,
    cancel_pending_unsuitable_rejections,
    find_or_create_conversation,
    has_active_dnc,
    process_inbound_candidate_message,
    process_interview_reply,
    schedule_interview_tasks,
    schedule_inactive_follow_up,
    set_application_status,
)
from backend.app.services.queue_utils import parse_time, utc_now
from backend.app.services.ocr import extract_pdf_text
from backend.app.services.purge import execute_external_purge
from backend.app.services.resume_parser import parse_resume_text
from backend.app.services.voice import SUCCESS_STATUSES, classify_voice_transcript, disclosure_was_confirmed, synthesize_disclosure, voice_agent_state


def is_due(value: str | None, now: datetime | None = None) -> bool:
    scheduled = parse_time(value)
    return scheduled is None or scheduled <= (now or datetime.now(UTC))


def adapter_config(account: PlatformAccount | None) -> dict | None:
    if not account:
        return None
    config = dict(account.config or {})
    config.setdefault("capabilities", account.capabilities or [])
    config.setdefault("work_windows", account.work_windows or [])
    return config


def next_retry_at(task: QueueTask) -> str:
    delay = min(60 * 60, 60 * (2 ** max(task.retry_count - 1, 0)))
    return (datetime.now(UTC) + timedelta(seconds=delay)).isoformat(timespec="seconds").replace("+00:00", "Z")


LEASE_SECONDS = int(get_settings().queue_lease_seconds)


def enqueue_outbox_for_task(db: Session, task: QueueTask) -> QueueOutbox:
    revision = int(task.delivery_revision or 1)
    for pending in db.new:
        if (
            isinstance(pending, QueueOutbox)
            and pending.queue_task_id == task.id
            and pending.event_type == "queue.publish"
            and int(pending.delivery_revision or 1) == revision
        ):
            return pending
    existing = db.scalar(
        select(QueueOutbox).where(
            QueueOutbox.queue_task_id == task.id,
            QueueOutbox.event_type == "queue.publish",
            QueueOutbox.delivery_revision == revision,
        )
    )
    if existing:
        return existing
    outbox = QueueOutbox(
        queue_task_id=task.id,
        event_type="queue.publish",
        delivery_revision=revision,
        stream_name=f"recruitment:{task.queue_name}",
        payload={
            "queue_task_id": task.id,
            "queue_name": task.queue_name,
            "task_type": task.task_type,
            "idempotency_key": task.idempotency_key,
            "delivery_revision": revision,
        },
        status="Pending",
    )
    db.add(outbox)
    db.flush()
    return outbox


def iso_after(seconds: int) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def interview_booking_version(interview: Interview) -> str:
    return hashlib.sha256(
        f"{interview.id}:{interview.interview_date}:{interview.interview_time}:{interview.timezone}".encode("utf-8")
    ).hexdigest()[:16]


def ensure_interview_side_effect_allowed(db: Session, task: QueueTask, *, phase: str) -> Interview | None:
    interview_id = (task.payload or {}).get("interview_id")
    if not interview_id:
        return None
    interview = db.get(Interview, interview_id)
    if not interview:
        raise ValueError("Interview not found")
    expected_version = (task.payload or {}).get("booking_version")
    if expected_version and expected_version != interview_booking_version(interview):
        task.status = "Cancelled"
        task.last_error = f"{phase} cancelled because booking version changed."
        return interview
    if getattr(interview, "attendance_status", None) == "Arrived" or interview.attended_at:
        task.status = "Cancelled"
        task.last_error = f"{phase} cancelled because HR verified arrival."
        return interview
    if interview.status in {"Cancelled", "Canceled"}:
        task.status = "Cancelled"
        task.last_error = f"{phase} cancelled because interview status is {interview.status}."
        return interview
    if has_active_dnc(db, interview.candidate_id):
        task.status = "Cancelled"
        task.last_error = f"{phase} cancelled because candidate is on do-not-contact."
        return interview
    return interview


def content_hash(body: str | None) -> str:
    return hashlib.sha256((body or "").strip().encode("utf-8")).hexdigest()


def _policy(account: PlatformAccount | None) -> dict:
    return ((account.config or {}).get("rate_policy") or {}) if account else {}


def _quiet_hours_active(policy: dict, now: datetime) -> bool:
    quiet = policy.get("quiet_hours") or {}
    start = quiet.get("start")
    end = quiet.get("end")
    if not start or not end:
        return False
    current = now.strftime("%H:%M")
    return (start <= current < end) if start < end else (current >= start or current < end)


def get_rate_state(db: Session, platform: str, account_id: int | None, scope_type: str, scope_id: str) -> PlatformRateLimitState:
    state = db.scalar(
        select(PlatformRateLimitState).where(
            PlatformRateLimitState.platform == platform,
            PlatformRateLimitState.account_id == account_id,
            PlatformRateLimitState.scope_type == scope_type,
            PlatformRateLimitState.scope_id == str(scope_id),
        )
    )
    if state:
        return state
    state = PlatformRateLimitState(platform=platform, account_id=account_id, scope_type=scope_type, scope_id=str(scope_id))
    db.add(state)
    db.flush()
    return state


def has_active_recruitment_blacklist(db: Session, candidate: Candidate | None, job_id: int | None = None) -> bool:
    if not candidate:
        return False
    return bool(matching_recruitment_blacklist_entries(db, candidate, job_id=job_id, statuses={"Active"}))


def _blacklist_identity_matches(entry: RecruitmentBlacklist, candidate: Candidate) -> bool:
    return bool(
        (entry.candidate_id and entry.candidate_id == candidate.id)
        or (entry.phone and candidate.phone and entry.phone == candidate.phone)
        or (entry.email and candidate.email and entry.email == candidate.email)
    )


def _blacklist_scope_matches(entry: RecruitmentBlacklist, job_id: int | None) -> bool:
    scope_type = (getattr(entry, "scope_type", None) or "").strip().lower()
    scope_id = str(getattr(entry, "scope_id", None) or "").strip()
    if scope_type == "job" and scope_id.isdigit():
        scope_id = str(int(scope_id))
    if scope_type in {"global", "candidate", "all"} and not scope_id:
        return True
    if scope_type == "job":
        return job_id is not None and scope_id == str(int(job_id))
    evidence = entry.evidence or ""
    rule = entry.rule_code or ""
    legacy_job_ids = {str(int(item)) for item in re.findall(r"\bjob:(\d+)\b", f"{evidence} {rule}") if item.isdigit()}
    if legacy_job_ids:
        return job_id is not None and str(int(job_id)) in legacy_job_ids
    if scope_type == "legacy":
        return True
    return scope_type == ""


def matching_recruitment_blacklist_entries(
    db: Session,
    candidate: Candidate | None,
    *,
    job_id: int | None = None,
    statuses: set[str] | None = None,
) -> list[RecruitmentBlacklist]:
    if not candidate:
        return []
    statuses = statuses or {"Active"}
    query = select(RecruitmentBlacklist).where(RecruitmentBlacklist.status.in_(statuses))
    query = query.where(
        or_(
            RecruitmentBlacklist.candidate_id == candidate.id,
            RecruitmentBlacklist.phone == candidate.phone if candidate.phone else False,
            RecruitmentBlacklist.email == candidate.email if candidate.email else False,
        )
    )
    return [entry for entry in db.execute(query).scalars().all() if _blacklist_identity_matches(entry, candidate) and _blacklist_scope_matches(entry, job_id)]


def pending_recruitment_scope_entries(db: Session, candidate: Candidate | None, job_id: int | None = None) -> list[RecruitmentBlacklist]:
    return matching_recruitment_blacklist_entries(db, candidate, job_id=job_id, statuses={"Pending Scope Confirmation"})


def enforce_pending_recruitment_scope_confirmation(
    db: Session,
    *,
    candidate: Candidate | None,
    job_id: int | None = None,
    conversation: Conversation | None = None,
    action: str = "external action",
) -> None:
    pending = pending_recruitment_scope_entries(db, candidate, job_id)
    if not pending:
        return
    if conversation:
        conversation.status = "manual_review"
    for entry in pending:
        key = f"manual-blacklist-scope:{entry.id}:{candidate.id if candidate else 'unknown'}:{job_id or 'unknown'}"
        if not db.scalar(select(QueueTask).where(QueueTask.idempotency_key == key)):
            db.add(
                QueueTask(
                    queue_name="manual-exception",
                    task_type="manual.blacklist_scope_confirmation",
                    payload={
                        "blacklist_id": entry.id,
                        "candidate_id": candidate.id if candidate else None,
                        "job_id": job_id,
                        "action": action,
                        "scope_type": entry.scope_type,
                        "scope_id": entry.scope_id,
                    },
                    idempotency_key=key,
                    status="Manual Review Required",
                    max_retries=0,
                )
            )
    raise RuntimeError("Pending Scope Confirmation matches this identity; automation is paused for manual scope confirmation.")


def enforce_platform_policy(
    db: Session,
    *,
    platform: str,
    account: PlatformAccount | None,
    candidate: Candidate | None = None,
    group_id: int | None = None,
    body: str | None = None,
    job_id: int | None = None,
) -> None:
    if account and account.status in {"Paused", "Permission Lost", "Rate Limited", "Captcha Required"}:
        raise RuntimeError(f"{platform} account is paused: {account.status}.")
    if candidate and has_active_dnc(db, candidate.id):
        raise RuntimeError("Do-not-contact blocks this external action.")
    if candidate:
        enforce_pending_recruitment_scope_confirmation(db, candidate=candidate, job_id=job_id, action=f"{platform} policy")
    if candidate and has_active_recruitment_blacklist(db, candidate, job_id):
        raise RuntimeError("RecruitmentBlacklist blocks this recruiting contact or match.")
    policy = _policy(account)
    if not policy:
        return
    now = datetime.now(UTC)
    if _quiet_hours_active(policy, now):
        raise RuntimeError("Platform quiet hours are active; task must wait.")
    account_id = account.id if account else None
    scopes = [("account", str(account_id or "default"))]
    if candidate:
        scopes.append(("candidate", str(candidate.id)))
    if group_id:
        scopes.append(("group", str(group_id)))
    body_hash = content_hash(body)
    for scope_type, scope_id in scopes:
        state = get_rate_state(db, platform, account_id, scope_type, scope_id)
        if parse_iso(state.next_allowed_at) and parse_iso(state.next_allowed_at) > now:
            raise RuntimeError(f"{platform} {scope_type} cooldown active until {state.next_allowed_at}.")
        hour_bucket = now.strftime("%Y-%m-%dT%H")
        day_bucket = now.strftime("%Y-%m-%d")
        if state.hour_bucket != hour_bucket:
            state.hour_bucket = hour_bucket
            state.hour_count = 0
        if state.day_bucket != day_bucket:
            state.day_bucket = day_bucket
            state.day_count = 0
        hourly_quota = int(policy.get(f"{scope_type}_hourly_quota") or policy.get("hourly_quota") or 0)
        daily_quota = int(policy.get(f"{scope_type}_daily_quota") or policy.get("daily_quota") or 0)
        if hourly_quota and state.hour_count >= hourly_quota:
            state.next_allowed_at = iso_after(3600)
            raise RuntimeError(f"{platform} {scope_type} hourly quota exceeded.")
        if daily_quota and state.day_count >= daily_quota:
            state.next_allowed_at = iso_after(24 * 3600)
            raise RuntimeError(f"{platform} {scope_type} daily quota exceeded.")
        cooldown_seconds = int(policy.get(f"{scope_type}_cooldown_seconds") or policy.get("cooldown_seconds") or 0)
        last_sent = parse_iso(state.last_sent_at)
        if cooldown_seconds and last_sent and last_sent + timedelta(seconds=cooldown_seconds) > now:
            state.next_allowed_at = (last_sent + timedelta(seconds=cooldown_seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
            raise RuntimeError(f"{platform} {scope_type} cooldown active until {state.next_allowed_at}.")
        if policy.get("suppress_duplicate_messages", True) and body and state.last_content_hash == body_hash:
            raise RuntimeError(f"{platform} duplicate content suppressed for {scope_type}:{scope_id}.")


def outbound_preflight(
    db: Session,
    *,
    platform: str,
    account: PlatformAccount | None,
    candidate: Candidate | None = None,
    conversation: Conversation | None = None,
    group_id: int | None = None,
    body: str | None = None,
    job_id: int | None = None,
) -> None:
    if conversation and conversation.status in {"paused", "manual_review", "closed"}:
        raise RuntimeError("Conversation automation is paused, under manual review, or closed.")
    if conversation and conversation.ai_mode in {"paused", "manual"}:
        raise RuntimeError("Conversation AI mode is paused or manual.")
    effective_candidate = candidate
    if not effective_candidate and conversation and conversation.candidate_id:
        effective_candidate = db.get(Candidate, conversation.candidate_id)
    effective_job_id = job_id if job_id is not None else (conversation.job_id if conversation else None)
    enforce_pending_recruitment_scope_confirmation(
        db,
        candidate=effective_candidate,
        job_id=effective_job_id,
        conversation=conversation,
        action=f"{platform} outbound preflight",
    )
    enforce_platform_policy(
        db,
        platform=platform,
        account=account,
        candidate=effective_candidate,
        group_id=group_id,
        body=body,
        job_id=effective_job_id,
    )


def record_platform_success(db: Session, *, platform: str, account: PlatformAccount | None, candidate: Candidate | None = None, group_id: int | None = None, body: str | None = None) -> None:
    policy = _policy(account)
    if not policy:
        return
    now = utc_now()
    scopes = [("account", str(account.id if account else "default"))]
    if candidate:
        scopes.append(("candidate", str(candidate.id)))
    if group_id:
        scopes.append(("group", str(group_id)))
    body_hash = content_hash(body)
    for scope_type, scope_id in scopes:
        state = get_rate_state(db, platform, account.id if account else None, scope_type, scope_id)
        state.hour_count += 1
        state.day_count += 1
        state.last_sent_at = now
        state.last_content_hash = body_hash
        state.consecutive_errors = 0
        state.last_error = None


def record_platform_failure(db: Session, *, platform: str, account: PlatformAccount | None, error: str) -> None:
    if not account:
        return
    lowered = error.lower()
    state = get_rate_state(db, platform, account.id, "account", str(account.id))
    state.consecutive_errors += 1
    state.last_error = error
    if "429" in lowered or "rate limit" in lowered:
        account.status = "Rate Limited"
        state.next_allowed_at = iso_after(int(_policy(account).get("rate_limit_pause_seconds") or 3600))
    elif "403" in lowered or "permission" in lowered:
        account.status = "Permission Lost"
    elif "captcha" in lowered:
        account.status = "Captcha Required"
    elif state.consecutive_errors >= int(_policy(account).get("circuit_breaker_failures") or 5):
        account.status = "Paused"
        state.next_allowed_at = iso_after(int(_policy(account).get("circuit_breaker_seconds") or 900))
    account.last_failure_reason = error


def _account_clause(query, account_id: int | None):
    if account_id is None:
        return query.where(PlatformIdentityMap.account_id.is_(None))
    return query.where(PlatformIdentityMap.account_id == account_id)


def upsert_platform_identity(
    db: Session,
    *,
    platform: str,
    account_id: int | None,
    entity_type: str,
    external_id: str,
    candidate_id: int | None = None,
    conversation_id: int | None = None,
    job_id: int | None = None,
    external_candidate_id: str | None = None,
    external_conversation_id: str | None = None,
    external_job_id: str | None = None,
    metadata_json: dict | None = None,
) -> PlatformIdentityMap:
    existing = db.scalar(
        _account_clause(
            select(PlatformIdentityMap).where(
                PlatformIdentityMap.platform == platform,
                PlatformIdentityMap.entity_type == entity_type,
                PlatformIdentityMap.external_id == external_id,
            ),
            account_id,
        )
    )
    if existing:
        existing.candidate_id = candidate_id or existing.candidate_id
        existing.conversation_id = conversation_id or existing.conversation_id
        existing.job_id = job_id or existing.job_id
        existing.external_candidate_id = external_candidate_id or existing.external_candidate_id
        existing.external_conversation_id = external_conversation_id or existing.external_conversation_id
        existing.external_job_id = external_job_id or existing.external_job_id
        existing.metadata_json = {**(existing.metadata_json or {}), **(metadata_json or {})}
        return existing
    mapping = PlatformIdentityMap(
        platform=platform,
        account_id=account_id,
        entity_type=entity_type,
        external_id=external_id,
        candidate_id=candidate_id,
        conversation_id=conversation_id,
        job_id=job_id,
        external_candidate_id=external_candidate_id,
        external_conversation_id=external_conversation_id,
        external_job_id=external_job_id,
        metadata_json=metadata_json or {},
    )
    db.add(mapping)
    db.flush()
    return mapping


def _is_verified_private_mapping(platform: str, mapping: PlatformIdentityMap) -> bool:
    metadata = mapping.metadata_json or {}
    if metadata.get("private_recipient_verified") is True:
        return True
    external_id = mapping.external_conversation_id or mapping.external_id or ""
    if platform == "facebook":
        return ":messenger:" in external_id and bool(mapping.external_candidate_id)
    if platform == "line":
        if metadata.get("line_source_type") in {"group", "room"}:
            return False
        if ":group:" in external_id or ":room:" in external_id:
            return False
        return bool(mapping.external_candidate_id or external_id.startswith("U") or metadata.get("line_source_type") == "user")
    return bool(mapping.external_candidate_id or metadata.get("private_channel"))


def resolve_external_conversation_target(db: Session, platform: str, account_id: int | None, conversation: Conversation) -> dict:
    mapping = db.scalar(
        _account_clause(
            select(PlatformIdentityMap).where(
                PlatformIdentityMap.platform == platform,
                PlatformIdentityMap.entity_type == "conversation",
                PlatformIdentityMap.conversation_id == conversation.id,
            ),
            account_id,
        )
    )
    if mapping and (mapping.external_conversation_id or mapping.external_id):
        if not _is_verified_private_mapping(platform, mapping):
            raise RuntimeError("External conversation mapping is not a verified private recipient; refusing candidate-specific send.")
        conversation_key = mapping.external_conversation_id or mapping.external_id
        recipient_id = mapping.external_candidate_id if platform in {"facebook", "line"} else (mapping.external_candidate_id or conversation_key)
        if not recipient_id:
            raise RuntimeError("Verified private recipient is missing; refusing candidate-specific send.")
        return {
            "conversation_key": conversation_key,
            "recipient_id": recipient_id,
            "mapping_id": mapping.id,
            "mapping_external_id": mapping.external_id,
        }
    raise RuntimeError("Missing external conversation mapping; refusing to use internal conversation.id as platform target.")


def claim_task(db: Session, task: QueueTask, *, worker_id: str = "manual", now: str | None = None) -> QueueTask | None:
    now = now or utc_now()
    lease = (datetime.now(UTC) + timedelta(seconds=LEASE_SECONDS)).isoformat(timespec="seconds").replace("+00:00", "Z")
    due_clause = or_(QueueTask.scheduled_at.is_(None), QueueTask.scheduled_at <= now)
    result = db.execute(
        update(QueueTask)
        .where(QueueTask.id == task.id)
        .where(QueueTask.status.in_(["Queued", "Retry"]))
        .where(due_clause)
        .values(
            status="Running",
            locked_at=now,
            locked_by=worker_id,
            lease_expires_at=lease,
            fencing_token=QueueTask.fencing_token + 1,
            last_error=None,
        )
        .returning(QueueTask.id)
    )
    if result.first() is None:
        db.rollback()
        return None
    db.commit()
    return db.get(QueueTask, task.id)


def renew_task_lease(db: Session, task: QueueTask, *, worker_id: str | None = None) -> bool:
    worker_id = worker_id or task.locked_by
    lease = (datetime.now(UTC) + timedelta(seconds=LEASE_SECONDS)).isoformat(timespec="seconds").replace("+00:00", "Z")
    result = db.execute(
        update(QueueTask)
        .where(QueueTask.id == task.id)
        .where(QueueTask.status == "Running")
        .where(QueueTask.locked_by == worker_id)
        .where(QueueTask.fencing_token == int(task.fencing_token or 0))
        .values(lease_expires_at=lease)
        .returning(QueueTask.id)
    )
    ok = result.first() is not None
    db.commit()
    if ok:
        task.lease_expires_at = lease
    return ok


def renew_task_lease_by_id(db: Session, *, task_id: int, worker_id: str, fencing_token: int) -> bool:
    lease = (datetime.now(UTC) + timedelta(seconds=LEASE_SECONDS)).isoformat(timespec="seconds").replace("+00:00", "Z")
    result = db.execute(
        update(QueueTask)
        .where(QueueTask.id == task_id)
        .where(QueueTask.status == "Running")
        .where(QueueTask.locked_by == worker_id)
        .where(QueueTask.fencing_token == fencing_token)
        .values(lease_expires_at=lease)
        .returning(QueueTask.id)
    )
    ok = result.first() is not None
    db.commit()
    return ok


def assert_task_fence(db: Session, task: QueueTask, *, phase: str = "commit") -> None:
    with db.no_autoflush:
        row = db.execute(
            select(QueueTask.fencing_token, QueueTask.locked_by, QueueTask.status)
            .where(QueueTask.id == task.id)
        ).first()
    if not row:
        raise RuntimeError(f"Queue task disappeared before {phase}; refusing stale worker action.")
    token, locked_by, status = row
    if int(token or 0) != int(task.fencing_token or 0) or locked_by != task.locked_by or status != "Running":
        raise RuntimeError(f"Queue fencing check failed before {phase}; refusing stale worker action.")


def task_fence_is_current(db: Session, task: QueueTask) -> bool:
    with db.no_autoflush:
        row = db.execute(
            select(QueueTask.fencing_token, QueueTask.locked_by, QueueTask.status)
            .where(QueueTask.id == task.id)
        ).first()
    return bool(row and int(row[0] or 0) == int(task.fencing_token or 0) and row[1] == task.locked_by and row[2] == "Running")


def reserve_next_task(db: Session, queue_names: set[str] | None = None, *, worker_id: str = "worker") -> QueueTask | None:
    now = utc_now()
    reclaim_stale_running_tasks(db, now=now)
    query = (
        select(QueueTask)
        .where(QueueTask.status.in_(["Queued", "Retry"]))
        .where(or_(QueueTask.scheduled_at.is_(None), QueueTask.scheduled_at <= now))
    )
    if queue_names:
        query = query.where(QueueTask.queue_name.in_(queue_names))
    task = db.scalar(query.order_by(QueueTask.created_at).limit(1).with_for_update(skip_locked=True))
    if not task:
        return None
    return claim_task(db, task, worker_id=worker_id, now=now)


def reclaim_stale_running_tasks(db: Session, *, now: str | None = None, limit: int = 100) -> int:
    now = now or utc_now()
    stale_ids = db.execute(
        select(QueueTask.id)
        .where(QueueTask.status == "Running")
        .where(or_(QueueTask.lease_expires_at.is_(None), QueueTask.lease_expires_at < now))
        .order_by(QueueTask.locked_at)
        .limit(limit)
    ).scalars().all()
    reclaimed = 0
    for task_id in stale_ids:
        result = db.execute(
            update(QueueTask)
            .where(QueueTask.id == task_id)
            .where(QueueTask.status == "Running")
            .where(or_(QueueTask.lease_expires_at.is_(None), QueueTask.lease_expires_at < now))
            .values(
                status="Retry",
                last_error="Worker lease expired; task returned to queue.",
                scheduled_at=now,
                locked_by=None,
                lease_expires_at=None,
                delivery_revision=QueueTask.delivery_revision + 1,
            )
            .returning(QueueTask.id)
        )
        if result.first() is not None:
            reclaimed += 1
    if reclaimed:
        db.commit()
        for task in db.execute(select(QueueTask).where(QueueTask.id.in_(stale_ids))).scalars().all():
            enqueue_outbox_for_task(db, task)
        db.commit()
    return reclaimed


async def process_task(db: Session, task: QueueTask) -> QueueTask:
    if task.status != "Running":
        claimed = claim_task(db, task, worker_id="inline")
        if not claimed:
            raise RuntimeError(f"Task {task.id} is {task.status}; it must be claimed before execution.")
        task = claimed
    starting_fence = int(task.fencing_token or 0)
    run = PlatformAutomationRun(
        platform=task.payload.get("platform", "internal"),
        capability=task.task_type,
        account_id=task.payload.get("account_id"),
        queue_task_id=task.id,
        status="Running",
        started_at=utc_now(),
        evidence={"idempotency_key": task.idempotency_key},
    )
    db.add(run)
    db.flush()
    try:
        assert_task_fence(db, task, phase="execution")
        if task.task_type == "platform.create_post":
            await process_platform_post(db, task)
        elif task.task_type == "message.send":
            await process_message_send(db, task)
        elif task.task_type == "message.private_reply":
            await process_private_reply(db, task)
        elif task.task_type == "message.verify":
            await process_message_verify(db, task)
        elif task.task_type == "post.verify":
            await process_post_verify(db, task)
        elif task.task_type == "platform.fetch_resume":
            await process_fetch_resume(db, task)
        elif task.task_type == "platform.sync_messages":
            await process_platform_sync_messages(db, task)
        elif task.task_type == "email.poll_518":
            await process_email_poll(db, task)
        elif task.task_type == "email.ack_518":
            await process_email_ack(db, task)
        elif task.task_type == "flow.follow_up_inactive":
            await process_inactive_follow_up(db, task)
        elif task.task_type == "flow.unsuitable_rejection_due":
            await process_unsuitable_rejection(db, task)
        elif task.task_type == "flow.process_inbound_message":
            await process_inbound_message(db, task)
        elif task.task_type == "flow.interview_reminder":
            await process_interview_reminder(db, task)
        elif task.task_type == "flow.interview_no_show_check":
            await process_interview_no_show_check(db, task)
        elif task.task_type == "flow.regional_notify":
            await process_regional_notify(db, task)
        elif task.task_type == "flow.shortage_recall":
            await process_shortage_recall(db, task)
        elif task.task_type == "phone.dial":
            await process_phone_dial(db, task)
        elif task.task_type == "calendar.book_interview":
            await process_calendar_book_interview(db, task)
        elif task.task_type == "resume.ocr_pdf":
            await process_resume_ocr_pdf(db, task)
        elif task.task_type == "privacy.external_purge":
            await process_external_purge(db, task)
        elif task.task_type == "privacy.retention_cleanup":
            await process_retention_cleanup(db, task)
        elif task.task_type.startswith("redis."):
            raise RuntimeError("Redis distributed queue is disabled until transactional outbox, leases, DLQ and recovery tests are complete.")
        else:
            raise ValueError(f"Unsupported task type: {task.task_type}")
        assert_task_fence(db, task, phase="completion")
        if task.status == "Cancelled":
            task.completed_at = utc_now()
            task.locked_by = None
            task.lease_expires_at = None
            run.status = "Cancelled"
            run.finished_at = utc_now()
            db.add(AuditLog(action="queue.cancelled", entity_type="QueueTask", entity_id=str(task.id), details={"task_type": task.task_type, "reason": task.last_error}))
            db.commit()
            db.refresh(task)
            return task
        if task.status == "Manual Review Required":
            task.locked_by = None
            task.lease_expires_at = None
            run.status = "Manual Review Required"
            run.finished_at = utc_now()
            run.error_reason = task.last_error
            db.add(AuditLog(action="queue.manual_review", entity_type="QueueTask", entity_id=str(task.id), details={"task_type": task.task_type, "reason": task.last_error}))
            db.commit()
            db.refresh(task)
            return task
        task.status = "Completed"
        task.completed_at = utc_now()
        task.last_error = None
        task.locked_by = None
        task.lease_expires_at = None
        run.status = "Completed"
        run.finished_at = utc_now()
        db.add(AuditLog(action="queue.completed", entity_type="QueueTask", entity_id=str(task.id), details={"task_type": task.task_type}))
    except Exception as exc:
        if not task_fence_is_current(db, task):
            db.rollback()
            raise
        task.retry_count += 1
        task.last_error = str(exc)
        run.status = "Failed"
        run.finished_at = utc_now()
        run.error_reason = str(exc)
        if task.retry_count >= task.max_retries:
            task.status = "Manual Review Required"
            db.add(
                SystemError(
                    service="Queue",
                    feature=task.task_type,
                    severity="Warning",
                    message=str(exc),
                    impact="Task exhausted retries and requires HR/operator review before reprocessing.",
                    retry_count=task.retry_count,
                )
            )
        else:
            task.status = "Retry"
            task.scheduled_at = next_retry_at(task)
            task.delivery_revision = int(task.delivery_revision or 1) + 1
            enqueue_outbox_for_task(db, task)
        task.locked_by = None
        task.lease_expires_at = None
        db.add(AuditLog(action="queue.failed", entity_type="QueueTask", entity_id=str(task.id), details={"error": str(exc), "retry_count": task.retry_count}))
    if int(task.fencing_token or 0) != starting_fence:
        db.rollback()
        raise RuntimeError("Queue fencing token changed during execution; refusing stale worker commit.")
    db.commit()
    db.refresh(task)
    return task


async def process_platform_post(db: Session, task: QueueTask) -> None:
    post_id = task.payload.get("post_id")
    post = db.get(Post, post_id)
    if not post:
        raise ValueError(f"Post {post_id} not found")
    account = db.get(PlatformAccount, post.account_id) if post.account_id else None
    enforce_platform_policy(db, platform=post.platform, account=account, group_id=post.group_id, body=post.content, job_id=post.job_id)
    if post.platform_post_id:
        verify = await get_adapter(post.platform, adapter_config(account)).verify_post(post.platform_post_id)
        log_external_action(db, task, post.platform, "post.verify_before_retry", account, verify.status, verify.message, post.platform_post_id, verify.data or {})
        if verify.ok:
            post.status = verify.status
            post.verified_at = utc_now()
            return
    result = await get_adapter(post.platform, adapter_config(account)).create_post(post.content, {"account_id": post.account_id, "group_id": post.group_id})
    log_external_action(db, task, post.platform, "platform.create_post", account, result.status, result.message, (result.data or {}).get("platform_post_id"), result.data or {})
    if not result.ok:
        post.status = result.status
        post.error_reason = result.message
        if account:
            record_platform_failure(db, platform=post.platform, account=account, error=result.message)
        raise RuntimeError(result.message)
    post.status = result.status
    post.post_url = (result.data or {}).get("post_url")
    post.platform_post_id = (result.data or {}).get("platform_post_id")
    post.verified_at = utc_now() if result.status in {"Verified", "Delivered"} else None
    post.error_reason = None
    if account:
        account.status = "Connected"
        account.last_success_at = utc_now()
        account.last_failure_reason = None
    record_platform_success(db, platform=post.platform, account=account, group_id=post.group_id, body=post.content)
    if post.platform_post_id:
        upsert_platform_identity(
            db,
            platform=post.platform,
            account_id=post.account_id,
            entity_type="post",
            external_id=post.platform_post_id,
            job_id=post.job_id,
            external_job_id=str(post.job_id),
            metadata_json={"source": "platform.create_post", "post_url": post.post_url},
        )
        db.add(
            QueueTask(
                queue_name="verification",
                task_type="post.verify",
                payload={"post_id": post.id, "platform": post.platform, "platform_post_id": post.platform_post_id, "account_id": post.account_id},
                idempotency_key=f"post-verify:{post.platform}:{post.platform_post_id}",
                scheduled_at=utc_now(),
                max_retries=3,
            )
        )


async def process_message_send(db: Session, task: QueueTask) -> None:
    if task.payload.get("interview_id"):
        guarded_interview = ensure_interview_side_effect_allowed(db, task, phase="message.send")
        if task.status == "Cancelled":
            message = db.get(Message, task.payload.get("message_id")) if task.payload.get("message_id") else None
            if message:
                message.delivery_status = "Cancelled"
            return
        db.refresh(guarded_interview)
    message_id = task.payload.get("message_id")
    message = db.get(Message, message_id)
    if not message:
        raise ValueError(f"Message {message_id} not found")
    conversation = db.get(Conversation, message.conversation_id)
    if not conversation:
        raise ValueError(f"Conversation {message.conversation_id} not found")
    if conversation.status in {"paused", "manual_review", "closed"} or conversation.ai_mode in {"paused", "manual"}:
        message.delivery_status = "Manual Review Required"
        task.status = "Manual Review Required"
        task.last_error = "Conversation automation is paused or manual; refusing automatic outbound send."
        return
    if has_active_dnc(db, conversation.candidate_id):
        message.delivery_status = "Manual Review Required"
        raise RuntimeError("Candidate is on the do-not-contact list.")
    candidate = db.get(Candidate, conversation.candidate_id) if conversation.candidate_id else None
    platform = task.payload.get("platform") or conversation.channel
    if pending_recruitment_scope_entries(db, candidate, job_id=conversation.job_id):
        try:
            enforce_pending_recruitment_scope_confirmation(
                db,
                candidate=candidate,
                job_id=conversation.job_id,
                conversation=conversation,
                action="message.send",
            )
        except RuntimeError as exc:
            message.delivery_status = "Manual Review Required"
            task.status = "Manual Review Required"
            task.last_error = str(exc)
            return
    account_id = task.payload.get("account_id")
    account = db.get(PlatformAccount, account_id) if account_id else db.scalar(
        select(PlatformAccount)
        .where(PlatformAccount.platform == platform)
        .where(PlatformAccount.status.in_(["Configured", "Connected"]))
        .order_by(PlatformAccount.id.desc())
        .limit(1)
    )
    if message.platform_message_id:
        verify = await get_adapter(platform, adapter_config(account)).verify_message(message.platform_message_id)
        log_external_action(db, task, platform, "message.verify_before_retry", account, verify.status, verify.message, message.platform_message_id, verify.data or {})
        if verify.ok:
            message.delivery_status = verify.status
            message.external_status_checked_at = utc_now()
            return
    external_target = resolve_external_conversation_target(db, platform, account.id if account else None, conversation)
    outbound_preflight(db, platform=platform, account=account, candidate=candidate, conversation=conversation, body=message.body)
    send_config = adapter_config(account) or {}
    if platform == "line":
        metadata = dict(message.metadata_json or {})
        line_retry_key = metadata.get("line_retry_key")
        if not line_retry_key:
            line_retry_key = str(uuid.uuid4())
            metadata["line_retry_key"] = line_retry_key
            message.metadata_json = metadata
            db.flush()
        send_config["line_retry_key"] = line_retry_key
    result = await get_adapter(platform, send_config).send_message(external_target["recipient_id"], message.body)
    evidence = {
        **(result.data or {}),
        "conversation_key": external_target["conversation_key"],
        "recipient_id": external_target["recipient_id"],
        "mapping_id": external_target["mapping_id"],
    }
    log_external_action(db, task, platform, "message.send", account, result.status, result.message, (result.data or {}).get("platform_message_id"), evidence)
    if not result.ok:
        message.delivery_status = result.status
        if account:
            record_platform_failure(db, platform=platform, account=account, error=result.message)
        raise RuntimeError(result.message)
    message.delivery_status = result.status
    message.platform_message_id = (result.data or {}).get("platform_message_id") or message.platform_message_id
    message.external_status_checked_at = utc_now()
    message.candidate_read_status = (result.data or {}).get("candidate_read_status") or message.candidate_read_status
    message.candidate_read_at = (result.data or {}).get("candidate_read_at") or message.candidate_read_at
    if not message.platform_message_id and message.delivery_status not in {"Sent", "Accepted", "Verified", "Delivered", "Read"}:
        message.delivery_status = "Delivery Unconfirmed"
    if account:
        account.status = "Connected"
        account.last_success_at = utc_now()
        account.last_failure_reason = None
    record_platform_success(db, platform=platform, account=account, candidate=candidate, body=message.body)
    if message.platform_message_id:
        db.add(
            QueueTask(
                queue_name="verification",
                task_type="message.verify",
                payload={"message_id": message.id, "platform": platform, "platform_message_id": message.platform_message_id, "account_id": account.id if account else account_id},
                idempotency_key=f"message-verify:{platform}:{message.platform_message_id}",
                scheduled_at=utc_now(),
                max_retries=3,
            )
        )


async def process_private_reply(db: Session, task: QueueTask) -> None:
    platform = task.payload.get("platform")
    if platform != "facebook":
        raise ValueError("Private reply is only implemented for Facebook comments.")
    message = db.get(Message, task.payload.get("message_id"))
    if not message:
        raise ValueError("Private reply message not found")
    comment_id = task.payload.get("comment_id")
    if not comment_id:
        raise RuntimeError("Facebook private reply requires original comment_id.")
    account_id = task.payload.get("account_id")
    account = db.get(PlatformAccount, account_id) if account_id else None
    conversation = db.get(Conversation, message.conversation_id)
    candidate = db.get(Candidate, conversation.candidate_id) if conversation and conversation.candidate_id else None
    outbound_preflight(db, platform="facebook", account=account, candidate=candidate, conversation=conversation, body=message.body, job_id=conversation.job_id if conversation else None)
    adapter = get_adapter("facebook", adapter_config(account))
    if not hasattr(adapter, "send_private_reply"):
        raise RuntimeError("Facebook adapter does not support private replies.")
    result = await adapter.send_private_reply(comment_id, message.body)
    log_external_action(db, task, "facebook", "message.private_reply", account, result.status, result.message, (result.data or {}).get("platform_message_id"), {**(result.data or {}), "comment_id": comment_id})
    if not result.ok:
        message.delivery_status = result.status
        raise RuntimeError(result.message)
    message.delivery_status = result.status
    message.platform_message_id = (result.data or {}).get("platform_message_id")
    message.external_status_checked_at = utc_now()


def has_blocked_job_candidates(db: Session, job_id: int) -> bool:
    blocked = db.execute(
        select(Application.id)
        .join(Candidate, Candidate.id == Application.candidate_id)
        .where(Application.job_id == job_id)
        .where(Candidate.id.in_(select(DoNotContact.candidate_id).where(DoNotContact.status == "Active")))
        .limit(1)
    ).first()
    return bool(blocked)


def scrub_evidence(value):
    if isinstance(value, dict):
        safe = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if any(secret in lowered for secret in ["password", "token", "secret", "key", "cookie", "session"]):
                safe[key] = "***redacted***" if item else None
            else:
                safe[key] = scrub_evidence(item)
        return safe
    if isinstance(value, list):
        return [scrub_evidence(item) for item in value]
    return value


def log_external_action(db: Session, task: QueueTask, platform: str, action_type: str, account: PlatformAccount | None, status: str, result: str, platform_object_id: str | None, evidence: dict) -> None:
    safe_evidence = scrub_evidence(evidence or {})
    db.add(
        ExternalActionLog(
            action_type=action_type,
            platform=platform,
            account_id=account.id if account else None,
            target_account=account.display_name if account else None,
            platform_object_id=platform_object_id,
            queue_task_id=task.id,
            status=status,
            attempted_at=utc_now(),
            verified_at=utc_now() if status == "Delivered" else None,
            result=result,
            evidence=safe_evidence,
        )
    )


async def process_message_verify(db: Session, task: QueueTask) -> None:
    message = db.get(Message, task.payload.get("message_id"))
    if not message:
        raise ValueError("Message not found")
    account = db.get(PlatformAccount, task.payload.get("account_id")) if task.payload.get("account_id") else None
    result = await get_adapter(task.payload.get("platform"), adapter_config(account)).verify_message(task.payload.get("platform_message_id"))
    message.external_status_checked_at = utc_now()
    if result.ok and result.status != "Unknown":
        message.delivery_status = result.status
    elif not result.ok and message.delivery_status not in {"Accepted", "Sent", "Delivered", "Verified", "Read"}:
        message.delivery_status = "Delivery Unconfirmed"
    message.candidate_read_status = (result.data or {}).get("candidate_read_status") or message.candidate_read_status
    message.candidate_read_at = (result.data or {}).get("candidate_read_at") or message.candidate_read_at
    if result.ok:
        question_answer_id = (message.metadata_json or {}).get("question_answer_id")
        if question_answer_id and message.delivery_status in {"Delivered", "Verified", "Read", "Sent"}:
            from backend.app.models.domain import CandidateQuestionAnswer

            answer = db.get(CandidateQuestionAnswer, int(question_answer_id))
            if answer:
                answer.asked_at = message.external_status_checked_at
                answer.metadata_json = {
                    **(answer.metadata_json or {}),
                    "delivery_evidence_message_id": message.id,
                    "delivery_status": message.delivery_status,
                    "delivery_checked_at": message.external_status_checked_at,
                }
                schedule_inactive_follow_up(db, message.conversation_id, int(question_answer_id), message.id, message.external_status_checked_at)
        log_external_action(db, task, task.payload.get("platform"), "message.verify", account, message.delivery_status, result.message, message.platform_message_id, result.data or {})
    else:
        raise RuntimeError(result.message)


async def process_post_verify(db: Session, task: QueueTask) -> None:
    post = db.get(Post, task.payload.get("post_id"))
    if not post:
        raise ValueError("Post not found")
    account = db.get(PlatformAccount, task.payload.get("account_id")) if task.payload.get("account_id") else None
    result = await get_adapter(task.payload.get("platform"), adapter_config(account)).verify_post(task.payload.get("platform_post_id"))
    if not result.ok:
        post.status = "Delivery Unconfirmed"
        raise RuntimeError(result.message)
    post.status = result.status
    post.verified_at = utc_now()
    log_external_action(db, task, task.payload.get("platform"), "post.verify", account, result.status, result.message, post.platform_post_id, result.data or {})


async def process_fetch_resume(db: Session, task: QueueTask) -> None:
    platform = task.payload.get("platform") or "518"
    account_id = task.payload.get("account_id")
    account = db.get(PlatformAccount, account_id) if account_id else db.scalar(
        select(PlatformAccount)
        .where(PlatformAccount.platform == platform)
        .order_by(PlatformAccount.status.desc(), PlatformAccount.id.desc())
        .limit(1)
    )
    result = await get_adapter(platform, adapter_config(account)).fetch_resume(task.payload.get("platform_link") or task.payload.get("applicant_id") or "")
    log_external_action(db, task, platform, "platform.fetch_resume", account, result.status, result.message, (result.data or {}).get("resume_id"), result.data or {})
    if not result.ok:
        raise RuntimeError(result.message)
    text = (result.data or {}).get("raw_text") or ""
    parsed = parse_resume_text(text)
    candidate = upsert_candidate_from_parsed(db, parsed, platform, (result.data or {}).get("platform_candidate_id"))
    resume = Resume(candidate_id=candidate.id, source=platform, file_name=(result.data or {}).get("file_name"), raw_text=text, parsed_json=parsed, status="Parsed")
    db.add(resume)
    application_id = None
    inbound_id = task.payload.get("inbound_email_id")
    if inbound_id:
        inbound = db.get(InboundEmail, inbound_id)
        if inbound:
            inbound.candidate_id = candidate.id
            job_id = inbound.job_id or task.payload.get("job_id")
            if job_id:
                application = db.scalar(select(Application).where(Application.candidate_id == candidate.id, Application.job_id == job_id))
                if not application:
                    application = Application(candidate_id=candidate.id, job_id=job_id, status="New")
                    db.add(application)
                    db.flush()
                inbound.application_id = application.id
                application_id = application.id
            inbound.import_status = "Parsed"
    if application_id is None and task.payload.get("job_id"):
        existing = db.scalar(select(Application).where(Application.candidate_id == candidate.id, Application.job_id == task.payload.get("job_id")))
        if not existing:
            db.add(Application(candidate_id=candidate.id, job_id=task.payload.get("job_id"), status="New"))


async def process_platform_sync_messages(db: Session, task: QueueTask) -> None:
    platform = task.payload.get("platform")
    account = db.get(PlatformAccount, task.payload.get("account_id")) if task.payload.get("account_id") else None
    result = await get_adapter(platform, adapter_config(account)).fetch_messages()
    log_external_action(db, task, platform, "platform.sync_messages", account, result.status, result.message, None, result.data or {})
    if not result.ok:
        raise RuntimeError(result.message)
    for item in (result.data or {}).get("items", []):
        parsed = parse_resume_text(item.get("resume_text") or item.get("body") or "")
        candidate = upsert_candidate_from_parsed(db, parsed, platform, item.get("candidate_platform_id"))
        conversation = find_or_create_conversation(db, platform, candidate, item.get("job_id"))
        external_message_id = item.get("platform_message_id")
        external_conversation_id = item.get("external_conversation_id") or item.get("conversation_platform_id")
        external_candidate_id = item.get("candidate_platform_id")
        external_job_id = item.get("external_job_id") or (str(item.get("job_id")) if item.get("job_id") else None)
        if external_candidate_id:
            upsert_platform_identity(
                db,
                platform=platform,
                account_id=account.id if account else None,
                entity_type="candidate",
                external_id=external_candidate_id,
                candidate_id=candidate.id,
                external_candidate_id=external_candidate_id,
                metadata_json={"source": "platform.sync_messages"},
            )
        if external_conversation_id:
            upsert_platform_identity(
                db,
                platform=platform,
                account_id=account.id if account else None,
                entity_type="conversation",
                external_id=external_conversation_id,
                candidate_id=candidate.id,
                conversation_id=conversation.id,
                job_id=item.get("job_id"),
                external_candidate_id=external_candidate_id,
                external_conversation_id=external_conversation_id,
                external_job_id=external_job_id,
                metadata_json={"source": "platform.sync_messages"},
            )
        if external_job_id and item.get("job_id"):
            upsert_platform_identity(
                db,
                platform=platform,
                account_id=account.id if account else None,
                entity_type="job",
                external_id=external_job_id,
                job_id=item.get("job_id"),
                external_job_id=external_job_id,
                metadata_json={"source": "platform.sync_messages"},
            )
        if external_message_id and db.scalar(select(Message).where(Message.platform_message_id == external_message_id)):
            continue
        message = Message(conversation_id=conversation.id, sender_type="candidate", body=item.get("body") or "", platform_message_id=external_message_id, delivery_status="Delivered", metadata_json={"source": "platform.sync_messages"})
        db.add(message)
        db.flush()
        process_inbound_candidate_message(db, message)


async def process_email_poll(db: Session, task: QueueTask) -> None:
    config = task.payload.get("config") or {}
    account = db.get(PlatformAccount, task.payload.get("account_id")) if task.payload.get("account_id") else None
    if account:
        config = {**(account.config or {}), **config}
    mailbox = config.get("mailbox", "INBOX")
    cursor = db.scalar(
        select(EmailCursor)
        .where(
            EmailCursor.account_id == (account.id if account else None),
            EmailCursor.mailbox == mailbox,
            EmailCursor.status == "Active",
        )
        .order_by(EmailCursor.updated_at.desc())
        .limit(1)
    )
    min_uid = cursor.last_imported_uid if cursor else 0
    if cursor:
        config["uid_validity"] = cursor.uid_validity
    messages = parse_fixture_mailbox(config["fixture_path"]) if config.get("fixture_path") else poll_imap(config, int(config.get("limit") or 20), mark_seen=False, min_uid=min_uid)
    from backend.app.api.routes import import_inbound_email
    from backend.app.schemas.dto import InboundEmailImport

    imported_uids: list[str] = []
    for item in messages:
        message_scope = f"{account.id if account else 'default'}:{item.mailbox or mailbox}"
        stable_message_id = f"<imap:{message_scope}:{item.message_id.strip('<>')}>" if item.message_id else f"<imap:{message_scope}:{item.uid_validity or 'unknown'}:{item.imap_uid or 'unknown'}>"
        payload = InboundEmailImport(
            message_id=stable_message_id,
            sender=item.sender,
            received_at=item.received_at or utc_now(),
            subject=item.subject,
            raw_text=item.raw_text,
            attachment_names=item.attachment_names,
            source_platform="518",
            job_id=task.payload.get("job_id"),
            mailbox=item.mailbox or mailbox,
            imap_uid=item.imap_uid,
            uid_validity=item.uid_validity,
        )
        inbound = import_inbound_email(payload, db=db, user=None, commit=False)
        if item.imap_uid:
            imported_uids.append(item.imap_uid)
            if not cursor or cursor.uid_validity != (item.uid_validity or "unknown"):
                cursor = EmailCursor(account_id=account.id if account else None, mailbox=item.mailbox or mailbox, uid_validity=item.uid_validity or "unknown", last_imported_uid=0, last_ack_uid=0)
                db.add(cursor)
                db.flush()
            cursor.last_imported_uid = max(cursor.last_imported_uid, int(item.imap_uid))
        inbound.mailbox = item.mailbox or mailbox
        inbound.imap_uid = item.imap_uid
        inbound.uid_validity = item.uid_validity
    db.flush()
    if imported_uids and not config.get("fixture_path"):
        db.add(
            QueueTask(
                queue_name="email",
                task_type="email.ack_518",
                payload={"account_id": account.id if account else None, "mailbox": mailbox, "uids": imported_uids, "uid_validity": cursor.uid_validity if cursor else "unknown"},
                idempotency_key=f"email-ack:{account.id if account else 'default'}:{mailbox}:{cursor.uid_validity if cursor else 'unknown'}:{'-'.join(imported_uids)}",
                max_retries=5,
            )
        )
    log_external_action(db, task, "518", "email.poll_518", account, "Completed", f"Imported {len(messages)} message(s).", None, {"count": len(messages)})


async def process_email_ack(db: Session, task: QueueTask) -> None:
    account = db.get(PlatformAccount, task.payload.get("account_id")) if task.payload.get("account_id") else None
    config = dict(account.config or {}) if account else (task.payload.get("config") or {})
    mailbox = task.payload.get("mailbox") or config.get("mailbox", "INBOX")
    config["mailbox"] = mailbox
    uids = [str(uid) for uid in (task.payload.get("uids") or [])]
    if not uids:
        return
    acknowledge_imap_messages(config, uids, uid_validity=task.payload.get("uid_validity"))
    cursor = db.scalar(
        select(EmailCursor).where(
            EmailCursor.account_id == (account.id if account else None),
            EmailCursor.mailbox == mailbox,
            EmailCursor.uid_validity == (task.payload.get("uid_validity") or "unknown"),
        )
    )
    if cursor:
        cursor.last_ack_uid = max(cursor.last_ack_uid, *(int(uid) for uid in uids))
    log_external_action(db, task, "518", "email.ack_518", account, "Completed", f"Acknowledged {len(uids)} IMAP message(s).", None, {"count": len(uids), "mailbox": mailbox})


async def process_inbound_message(db: Session, task: QueueTask) -> None:
    message = db.get(Message, task.payload.get("message_id"))
    if not message:
        raise ValueError("Inbound message not found")
    process_inbound_candidate_message(db, message)


def upsert_candidate_from_parsed(db: Session, parsed: dict, platform: str, platform_id: str | None = None) -> Candidate:
    candidate = None
    if platform_id:
        candidate = db.scalar(select(Candidate).where(Candidate.platform_id == platform_id, Candidate.source_platform == platform))
    if candidate:
        return candidate
    contact_candidate = None
    if parsed.get("phone"):
        contact_candidate = db.scalar(select(Candidate).where(Candidate.phone == parsed.get("phone")))
    if not contact_candidate and parsed.get("email"):
        contact_candidate = db.scalar(select(Candidate).where(Candidate.email == parsed.get("email")))
    if contact_candidate:
        if contact_candidate.source_platform != platform and contact_candidate.platform_id != platform_id:
            db.add(
                CandidateMergeConflict(
                    candidate_id=contact_candidate.id,
                    incoming_source=platform,
                    conflict_type="cross_source_contact_match",
                    incoming_payload={**parsed, "platform_id": platform_id},
                    existing_payload={"source_platform": contact_candidate.source_platform, "platform_id": contact_candidate.platform_id},
                )
            )
        return contact_candidate
    candidate = Candidate(
        name=parsed.get("name") or "Unknown Candidate",
        phone=parsed.get("phone"),
        email=parsed.get("email"),
        region=parsed.get("region"),
        skills=parsed.get("skills") or [],
        licenses=parsed.get("licenses") or [],
        preferred_area=parsed.get("preferred_area"),
        preferred_shift=parsed.get("preferred_shift"),
        source_platform=platform,
        platform_id=platform_id,
        tags=["worker_import"],
        notes=parsed.get("summary"),
    )
    db.add(candidate)
    db.flush()
    return candidate


async def process_inactive_follow_up(db: Session, task: QueueTask) -> None:
    conversation = db.get(Conversation, task.payload.get("conversation_id"))
    if not conversation:
        raise ValueError("Conversation not found")
    if has_active_dnc(db, conversation.candidate_id):
        raise RuntimeError("Do-not-contact blocks inactive follow-up.")
    from backend.app.models.domain import CandidateQuestionAnswer

    question_answer_id = task.payload.get("question_answer_id")
    answer = db.get(CandidateQuestionAnswer, question_answer_id) if question_answer_id else None
    delivered_at = parse_iso(task.payload.get("delivered_at"))
    if answer and answer.answer_status == "Answered":
        task.status = "Cancelled"
        task.last_error = "Candidate answered this question before follow-up was due."
        return
    if answer and answer.evidence_message_id:
        evidence_message = db.get(Message, answer.evidence_message_id)
        if evidence_message and delivered_at and evidence_message.created_at >= delivered_at.replace(tzinfo=None):
            task.status = "Cancelled"
            task.last_error = "Candidate replied to this delivered question before follow-up was due."
            return
    if not answer or not delivered_at:
        task.status = "Manual Review Required"
        task.last_error = "Follow-up task is missing question delivery evidence."
        return
    body = task.payload.get("body") or "您好，想確認您是否仍有意願應徵？若方便，請回覆上一題資訊，我們會協助安排後續流程。"
    message = Message(conversation_id=conversation.id, sender_type="system", body=body, delivery_status="Pending Send", metadata_json={"flow": "three_day_follow_up", "question_answer_id": question_answer_id, "source_message_id": task.payload.get("message_id")})
    db.add(message)
    db.flush()
    db.add(QueueTask(queue_name="messaging", task_type="message.send", payload={"message_id": message.id, "conversation_id": conversation.id, "platform": conversation.channel}, idempotency_key=f"inactive-follow-up:{conversation.id}:{question_answer_id}:{task.payload.get('message_id')}"))


async def process_unsuitable_rejection(db: Session, task: QueueTask) -> None:
    application = db.get(Application, task.payload.get("application_id"))
    if not application:
        raise ValueError("Application not found")
    if application.status not in UNSUITABLE_STATUSES:
        task.status = "Cancelled"
        task.last_error = f"Application status is {application.status}; automatic unsuitable rejection cancelled."
        return
    if has_active_dnc(db, application.candidate_id):
        task.status = "Cancelled"
        task.last_error = "Do-not-contact blocks unsuitable rejection notice."
        return
    conversation = db.scalar(
        select(Conversation)
        .where(
            Conversation.candidate_id == application.candidate_id,
            Conversation.job_id == application.job_id,
            Conversation.status == "open",
        )
        .order_by(Conversation.updated_at.desc())
        .limit(1)
    )
    if not conversation:
        task.status = "Manual Review Required"
        task.last_error = "No verified private conversation exists for unsuitable rejection."
        return
    marked_at = parse_iso(task.payload.get("marked_unsuitable_at"))
    if marked_at:
        replied = db.scalar(
            select(Message)
            .where(Message.conversation_id == conversation.id, Message.sender_type == "candidate")
            .where(Message.created_at >= marked_at.replace(tzinfo=None))
            .limit(1)
        )
        if replied:
            task.status = "Cancelled"
            task.last_error = "Candidate replied after unsuitable status; automatic rejection cancelled."
            cancel_pending_unsuitable_rejections(db, application.id, task.last_error)
            return
    duplicate = db.scalar(
        select(Message)
        .where(Message.conversation_id == conversation.id)
        .where(Message.metadata_json["flow"].as_string() == "three_day_unsuitable_rejection")
        .limit(1)
    )
    if duplicate:
        task.status = "Cancelled"
        task.last_error = "Unsuitable rejection notice was already created."
        return
    body = task.payload.get("body") or "您好，感謝您應徵。經本次職缺條件評估，目前暫不進入下一階段；我們會保留您的資料供合適職缺參考。祝您一切順利。"
    message = Message(
        conversation_id=conversation.id,
        sender_type="system",
        body=body,
        delivery_status="Pending Send",
        metadata_json={"flow": "three_day_unsuitable_rejection", "application_id": application.id, "approved_template": True},
    )
    db.add(message)
    db.flush()
    db.add(
        QueueTask(
            queue_name="messaging",
            task_type="message.send",
            payload={"message_id": message.id, "conversation_id": conversation.id, "platform": conversation.channel},
            idempotency_key=f"unsuitable-rejection-message:{application.id}",
            max_retries=5,
        )
    )
    set_application_status(
        db,
        application,
        to_status="Closed",
        source="system",
        reason="Three-day unsuitable rejection notice queued after final status re-check.",
        event_key=f"unsuitable-rejection-closed:{application.id}",
    )


async def process_calendar_book_interview(db: Session, task: QueueTask) -> None:
    interview = db.get(Interview, task.payload.get("interview_id"))
    if not interview:
        raise ValueError("Interview not found")
    if getattr(interview, "attendance_status", None) == "Arrived":
        task.status = "Cancelled"
        task.last_error = "Interview no-show check cancelled because HR verified arrival."
        return
    config = dict(task.payload.get("calendar_config") or {})
    if {"access_token", "oauth_token", "refresh_token", "client_secret", "token", "secret"}.intersection(set(config.keys())):
        raise RuntimeError("Calendar credentials must not be stored in QueueTask calendar_config.")
    account_id = config.pop("account_id", None) or config.pop("calendar_account_id", None)
    if account_id:
        account = db.get(PlatformAccount, account_id)
        if not account or account.platform != "calendar":
            raise RuntimeError("Calendar PlatformAccount not found.")
        config = {**(account.config or {}), **config, "account_id": account.id}
    if {"access_token", "oauth_token", "refresh_token", "client_secret", "token", "secret"}.intersection(set(task.payload.keys())):
        raise RuntimeError("Calendar credentials must not be stored in QueueTask payload.")
    booking = book_interview_calendar(db, interview, config)
    task.payload = {**(task.payload or {}), "calendar_booking_id": booking.id, "booking_status": booking.status}
    if booking.status in {"Manual Review Required", "Unverified"}:
        task.status = "Manual Review Required"
        task.last_error = booking.failure_reason or "Calendar booking is unverified and requires manual review."
    elif booking.status == "Booked":
        cancel_pending_interview_tasks(db, interview.id, "Cancelled before verified calendar reminder reschedule.")
        schedule_interview_tasks(db, interview)


async def process_resume_ocr_pdf(db: Session, task: QueueTask) -> None:
    payload = task.payload or {}
    if "raw_text" in payload or "file_bytes" in payload or "file_content" in payload:
        raise RuntimeError("OCR queue payload must reference stored document metadata only, not resume text or file bytes.")
    document = db.get(StoredDocument, payload.get("document_id"))
    if not document:
        raise ValueError("Stored document not found")
    if document.deleted_at or document.status in {"Deletion Pending", "Deleted"}:
        task.status = "Cancelled"
        task.last_error = "Stored document is deleted or pending deletion; OCR cancelled."
        return
    resume = extract_pdf_text(db, document)
    task.payload = {**payload, "resume_id": resume.id, "document_status": document.status}
    if resume.status == "Manual Review Required":
        task.status = "Manual Review Required"
        task.last_error = "OCR result requires manual review."


async def process_external_purge(db: Session, task: QueueTask) -> None:
    purge = db.get(ExternalRetentionPurge, (task.payload or {}).get("purge_id"))
    if not purge:
        raise ValueError("External purge record not found")
    execute_external_purge(purge, dry_run=bool((task.payload or {}).get("dry_run", True)))
    task.payload = {**(task.payload or {}), "purge_status": purge.status}
    document_id = (task.payload or {}).get("stored_document_id")
    if document_id:
        document = db.get(StoredDocument, document_id)
        if document:
            if purge.status == "Deleted":
                document.status = "Deleted"
                document.deleted_at = purge.confirmed_at
                storage_object = db.scalar(select(StorageObject).where(StorageObject.object_key == document.storage_path))
                if storage_object:
                    storage_object.status = "Deleted"
                    storage_object.deleted_at = purge.confirmed_at
                    storage_object.evidence = {**(storage_object.evidence or {}), "purge_id": purge.id, "delete_evidence": purge.evidence}
                document.delete_audit = {
                    **(document.delete_audit or {}),
                    "status": "deleted",
                    "purge_id": purge.id,
                    "confirmed_at": purge.confirmed_at,
                    "evidence": purge.evidence,
                }
            elif purge.status in {"Legal Hold", "Manual Review Required"}:
                document.status = purge.status
                document.delete_audit = {
                    **(document.delete_audit or {}),
                    "status": "purge_blocked",
                    "purge_id": purge.id,
                    "last_error": purge.last_error,
                    "evidence": purge.evidence,
                }
    if purge.status == "Manual Review Required" and not (purge.evidence or {}).get("permanent_failure"):
        raise RuntimeError(purge.last_error or "External purge failed temporarily.")
    if purge.status in {"Legal Hold", "Manual Review Required"}:
        task.status = "Manual Review Required"
        task.last_error = purge.last_error


def _retention_cutoff(days: int) -> datetime:
    return datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)


RETENTION_MARKERS = {
    "ai_request": "[retained audit metadata only]",
    "inbound_email": "[retained import metadata only]",
    "resume": "[retained resume metadata only]",
    "message": "[retained conversation metadata only]",
}

RETENTION_STRATEGIES = {
    ("PlatformWebhookEvent", "mask_payload_hash_only"),
    ("AIRequest", "mask_prompt_output"),
    ("QueueTask", "delete_completed"),
    ("SystemError", "delete_closed"),
    ("ExternalActionLog", "mask_evidence"),
    ("QueueDlqRecord", "mask_metadata"),
    ("InboundEmail", "mask_raw_text"),
    ("Resume", "mask_raw_text"),
    ("Message", "mask_body"),
}


def _retention_query(model, days: int, strategy: str):
    query = select(model).where(model.created_at < _retention_cutoff(days))
    if model is QueueTask and strategy == "delete_completed":
        query = query.where(QueueTask.status.in_(["Completed", "Cancelled"]))
    elif model is SystemError and strategy == "delete_closed":
        query = query.where(SystemError.status == "Closed")
    elif model is AIRequest and strategy == "mask_prompt_output":
        query = query.where(AIRequest.prompt_masked != RETENTION_MARKERS["ai_request"])
    elif model is InboundEmail and strategy == "mask_raw_text":
        query = query.where(InboundEmail.raw_text != RETENTION_MARKERS["inbound_email"])
    elif model is Resume and strategy == "mask_raw_text":
        query = query.where(Resume.raw_text != RETENTION_MARKERS["resume"])
    elif model is Message and strategy == "mask_body":
        query = query.where(Message.body != RETENTION_MARKERS["message"])
    elif model is PlatformWebhookEvent and strategy == "mask_payload_hash_only":
        query = query.where(PlatformWebhookEvent.status != "Payload Retained")
    elif model is ExternalActionLog and strategy == "mask_evidence":
        query = query.where(ExternalActionLog.result != "[retained action metadata only]")
    elif model is QueueDlqRecord and strategy == "mask_metadata":
        query = query.where(QueueDlqRecord.metadata_json != {"retained": "dlq metadata only"})
    return query


def _select_old(db: Session, model, days: int, limit: int, strategy: str):
    return db.execute(_retention_query(model, days, strategy).order_by(model.created_at, model.id).limit(limit)).scalars().all()


def _count_retention_backlog(db: Session, model, days: int, strategy: str) -> int:
    return int(db.scalar(select(__import__("sqlalchemy").func.count()).select_from(_retention_query(model, days, strategy).subquery())) or 0)


def _enqueue_retention_continuation(db: Session, task: QueueTask, *, policy_name: str, batch_size: int) -> None:
    payload = {**(task.payload or {}), "batch_size": batch_size, "continuation_of": task.id, "source": "retention_continuation"}
    key = f"retention-continuation:{task.id}:{policy_name}:{batch_size}"
    if db.scalar(select(QueueTask).where(QueueTask.idempotency_key == key)):
        return
    db.add(
        QueueTask(
            queue_name=task.queue_name or "privacy",
            task_type="privacy.retention_cleanup",
            payload=payload,
            idempotency_key=key,
            max_retries=task.max_retries,
        )
    )


async def process_retention_cleanup(db: Session, task: QueueTask) -> None:
    policies = task.payload.get("policies") or [
        {"name": "webhook_payload", "model": "PlatformWebhookEvent", "days": 180, "strategy": "mask_payload_hash_only"},
        {"name": "ai_request", "model": "AIRequest", "days": 180, "strategy": "mask_prompt_output"},
        {"name": "queue_completed", "model": "QueueTask", "days": 90, "strategy": "delete_completed"},
        {"name": "system_error_closed", "model": "SystemError", "days": 365, "strategy": "delete_closed"},
        {"name": "external_action_log", "model": "ExternalActionLog", "days": 365, "strategy": "mask_evidence"},
        {"name": "queue_dlq", "model": "QueueDlqRecord", "days": 365, "strategy": "mask_metadata"},
        {"name": "raw_inbound_email", "model": "InboundEmail", "days": 365, "strategy": "mask_raw_text"},
        {"name": "resume_raw_text", "model": "Resume", "days": 365, "strategy": "mask_raw_text"},
        {"name": "old_messages", "model": "Message", "days": 730, "strategy": "mask_body"},
    ]
    model_map = {
        "AIRequest": AIRequest,
        "ExternalActionLog": ExternalActionLog,
        "InboundEmail": InboundEmail,
        "Message": Message,
        "PlatformWebhookEvent": PlatformWebhookEvent,
        "QueueDlqRecord": QueueDlqRecord,
        "QueueTask": QueueTask,
        "Resume": Resume,
        "SystemError": SystemError,
    }
    limit = min(max(int(task.payload.get("batch_size") or 200), 1), 1000)
    max_batches = min(max(int(task.payload.get("max_policy_batches") or len(policies)), 1), len(policies))
    total_changed = 0
    total_scanned = 0
    continuation_needed = False
    started_at = datetime.now(UTC)
    max_seconds = min(max(int(task.payload.get("max_seconds") or 20), 1), 120)
    for index, policy in enumerate(policies[:max_batches]):
        model = model_map.get(policy.get("model"))
        if not model:
            raise ValueError(f"Unsupported retention model: {policy.get('model')}")
        days = int(policy.get("days") or 365)
        strategy = policy.get("strategy") or "delete"
        if (policy.get("model"), strategy) not in RETENTION_STRATEGIES:
            db.add(
                SystemError(
                    service="Retention",
                    feature=policy.get("name") or str(policy.get("model")),
                    severity="Error",
                    message=f"Unsupported retention strategy combination: {policy.get('model')}/{strategy}",
                    impact="Retention cleanup rejected the policy instead of falling back to deletion.",
                )
            )
            raise ValueError(f"Unsupported retention strategy combination: {policy.get('model')}/{strategy}")
        rows = _select_old(db, model, days, limit, strategy)
        changed = 0
        for row in rows:
            if strategy == "mask_prompt_output" and isinstance(row, AIRequest):
                row.prompt_masked = RETENTION_MARKERS["ai_request"]
                row.response_masked = RETENTION_MARKERS["ai_request"]
            elif strategy == "mask_payload_hash_only" and isinstance(row, PlatformWebhookEvent):
                row.evidence = {"retained": "payload_hash_only"}
                row.status = "Payload Retained"
            elif strategy == "mask_evidence" and isinstance(row, ExternalActionLog):
                row.evidence = {"retained": "action metadata only"}
                row.result = "[retained action metadata only]"
            elif strategy == "mask_metadata" and isinstance(row, QueueDlqRecord):
                row.metadata_json = {"retained": "dlq metadata only"}
            elif strategy == "mask_raw_text" and isinstance(row, InboundEmail):
                row.raw_text = RETENTION_MARKERS["inbound_email"]
                row.attachment_names = []
            elif strategy == "mask_raw_text" and isinstance(row, Resume):
                row.raw_text = RETENTION_MARKERS["resume"]
                row.parsed_json = {}
            elif strategy == "mask_body" and isinstance(row, Message):
                row.body = RETENTION_MARKERS["message"]
                row.metadata_json = {}
            else:
                db.delete(row)
            changed += 1
        backlog_remaining = _count_retention_backlog(db, model, days, strategy)
        db.add(
            DataRetentionRun(
                policy_name=policy.get("name") or str(model.__tablename__),
                target_table=model.__tablename__,
                strategy=strategy,
                cutoff_at=_retention_cutoff(days).isoformat(timespec="seconds") + "Z",
                rows_scanned=len(rows),
                rows_changed=changed,
                status="Completed",
                details={
                    "batch_size": limit,
                    "backlog_remaining_estimate": backlog_remaining,
                    "oldest_pending_age_days": days if backlog_remaining else 0,
                    "policy_index": index,
                    "max_seconds": max_seconds,
                    "backup_note": "This cleanup only affects the live application database; backup/media purge must be handled by the backup/archive/object-storage lifecycle and purge drill.",
                },
            )
        )
        total_changed += changed
        total_scanned += len(rows)
        if len(rows) >= limit or backlog_remaining:
            continuation_needed = True
        if (datetime.now(UTC) - started_at).total_seconds() >= max_seconds:
            continuation_needed = True
            break
    if continuation_needed:
        _enqueue_retention_continuation(db, task, policy_name="batch", batch_size=limit)
    task.payload = {**(task.payload or {}), "rows_scanned": total_scanned, "rows_changed": total_changed, "continuation_queued": continuation_needed}


async def process_interview_reminder(db: Session, task: QueueTask) -> None:
    interview = ensure_interview_side_effect_allowed(db, task, phase="Interview reminder")
    if task.status == "Cancelled":
        return
    if interview.status not in {"Scheduled", "Reminder Sent", "Pending Confirmation", "Confirmed"}:
        task.status = "Cancelled"
        task.last_error = f"Interview reminder cancelled because status is {interview.status}."
        return
    conversation = db.scalar(select(Conversation).where(Conversation.candidate_id == interview.candidate_id, Conversation.job_id == interview.job_id).limit(1))
    if not conversation:
        raise RuntimeError("No conversation available for interview reminder.")
    body = task.payload.get("body") or f"提醒您面試時間為 {interview.interview_date} {interview.interview_time}，地點/方式：{interview.location or interview.method}。請回覆確認。"
    booking_version = task.payload.get("booking_version") or interview_booking_version(interview)
    message = Message(
        conversation_id=conversation.id,
        sender_type="system",
        body=body,
        delivery_status="Pending Send",
        metadata_json={"flow": "interview_reminder", "interview_id": interview.id, "booking_version": booking_version, "offset": task.payload.get("offset", "manual")},
    )
    db.add(message)
    db.flush()
    db.add(
        QueueTask(
            queue_name="messaging",
            task_type="message.send",
            payload={"message_id": message.id, "conversation_id": conversation.id, "platform": conversation.channel, "interview_id": interview.id, "booking_version": booking_version, "offset": task.payload.get("offset", "manual")},
            idempotency_key=f"interview-reminder:{interview.id}:{booking_version}:{task.payload.get('offset', 'manual')}",
        )
    )


async def process_interview_no_show_check(db: Session, task: QueueTask) -> None:
    interview = ensure_interview_side_effect_allowed(db, task, phase="Interview no-show check")
    if task.status == "Cancelled":
        return
    conversation = db.scalar(select(Conversation).where(Conversation.candidate_id == interview.candidate_id, Conversation.job_id == interview.job_id).limit(1))
    if conversation:
        latest_candidate_reply = db.scalar(
            select(Message)
            .where(Message.conversation_id == conversation.id, Message.sender_type == "candidate")
            .where(Message.created_at >= interview.created_at)
            .order_by(Message.created_at.desc())
            .limit(1)
        )
        if latest_candidate_reply:
            process_interview_reply(db, latest_candidate_reply)
            db.flush()
    if interview.status not in {"Scheduled", "Reminder Sent", "Pending Confirmation", "Confirmed"}:
        task.status = "Cancelled"
        task.last_error = f"Interview no-show check cancelled because status is {interview.status}."
        return
    if not conversation:
        raise RuntimeError("No conversation available for interview no-show check.")
    db.refresh(interview)
    ensure_interview_side_effect_allowed(db, task, phase="Interview no-show check before send")
    if task.status == "Cancelled":
        return
    interview.status = "No Response Follow-up"
    booking_version = task.payload.get("booking_version") or interview_booking_version(interview)
    body = task.payload.get("body") or "You missed the scheduled interview. Please reply with a new available time if you would like to reschedule; HR has also been notified."
    message = Message(conversation_id=conversation.id, sender_type="system", body=body, delivery_status="Pending Send", metadata_json={"flow": "interview_no_show_30m", "interview_id": interview.id, "booking_version": booking_version})
    db.add(message)
    db.flush()
    db.add(QueueTask(queue_name="messaging", task_type="message.send", payload={"message_id": message.id, "conversation_id": conversation.id, "platform": conversation.channel, "interview_id": interview.id, "booking_version": booking_version}, idempotency_key=f"interview-no-show-message:{interview.id}:{booking_version}"))


async def process_regional_notify(db: Session, task: QueueTask) -> None:
    platform = task.payload.get("platform", "line")
    account = db.get(PlatformAccount, task.payload.get("account_id")) if task.payload.get("account_id") else None
    outbound_preflight(db, platform=platform, account=account, group_id=task.payload.get("target_group_id"), body=task.payload.get("body") or "")
    result = await get_adapter(platform, adapter_config(account)).send_message(task.payload.get("target_group_id") or "regional-group", task.payload.get("body") or "")
    log_external_action(db, task, platform, "flow.regional_notify", account, result.status, result.message, (result.data or {}).get("platform_message_id"), result.data or {})
    if not result.ok:
        record_platform_failure(db, platform=platform, account=account, error=result.message)
        raise RuntimeError(result.message)
    record_platform_success(db, platform=platform, account=account, group_id=task.payload.get("target_group_id"), body=task.payload.get("body") or "")


async def process_shortage_recall(db: Session, task: QueueTask) -> None:
    job_id = task.payload.get("job_id")
    job = db.get(__import__("backend.app.models.domain", fromlist=["Job"]).Job, job_id)
    if not job:
        raise ValueError("Job not found")
    candidates = db.execute(select(Candidate).where(Candidate.status == "Talent Pool", Candidate.preferred_area == job.area).limit(int(task.payload.get("limit") or 20))).scalars().all()
    for candidate in candidates:
        if has_active_dnc(db, candidate.id):
            continue
        if has_active_recruitment_blacklist(db, candidate, job.id):
            continue
        conversation = find_or_create_conversation(db, task.payload.get("platform", "line"), candidate, job.id)
        message = Message(conversation_id=conversation.id, sender_type="system", body=task.payload.get("body") or f"{job.title} 目前有缺額，想確認您是否有意願了解？", delivery_status="Pending Send", metadata_json={"flow": "shortage_recall"})
        db.add(message)
        db.flush()
        db.add(QueueTask(queue_name="messaging", task_type="message.send", payload={"message_id": message.id, "conversation_id": conversation.id, "platform": conversation.channel}, idempotency_key=f"shortage-recall:{job.id}:{candidate.id}"))


async def process_phone_dial(db: Session, task: QueueTask) -> None:
    candidate_id = task.payload.get("candidate_id")
    if not candidate_id:
        raise ValueError("candidate_id is required for phone.dial.")
    if has_active_dnc(db, candidate_id):
        raise RuntimeError("Do-not-contact blocks phone dialing.")
    if not task.payload.get("consent_disclosed"):
        raise RuntimeError("AI phone dialing requires consent_disclosed=true and a disclosure script.")
    duplicate = db.scalar(
        select(PhoneCallLog)
        .where(PhoneCallLog.candidate_id == candidate_id)
        .where(PhoneCallLog.dial_status.in_(["Accepted", "Ringing", "Connected"]))
        .order_by(PhoneCallLog.created_at.desc())
        .limit(1)
    )
    if duplicate:
        raise RuntimeError("Duplicate active phone dial is blocked for this candidate.")
    if task.payload.get("provider_config"):
        forbidden_keys = {"api_token", "token", "secret", "password", "dial_url", "callback_url"}
        if forbidden_keys.intersection(set(task.payload["provider_config"].keys())):
            raise RuntimeError("Phone provider secrets and URLs must be stored on PlatformAccount config, not QueueTask payload.")
    account_id = task.payload.get("account_id") or task.payload.get("provider_account_id")
    account = db.get(PlatformAccount, account_id) if account_id else None
    provider_config = dict(account.config or {}) if account else {}
    if account and account.platform != "phone":
        raise RuntimeError("phone.dial requires a PlatformAccount with platform='phone'.")
    candidate = db.get(Candidate, candidate_id)
    outbound_preflight(db, platform="phone", account=account, candidate=candidate, body=task.payload.get("script") or "", job_id=task.payload.get("job_id"))
    disclosure_script = synthesize_disclosure(task.payload.get("script") or "", task.payload.get("company_name"))
    call = PhoneCallLog(
        candidate_id=candidate_id,
        job_id=task.payload.get("job_id"),
        conversation_id=task.payload.get("conversation_id"),
        provider=provider_config.get("provider") or (account.display_name if account else None),
        dial_status="Not Configured",
        script=disclosure_script,
        evidence={
            "required": ["phone PlatformAccount", "dial endpoint", "signed status callback", "recording/transcript callback"],
            "mock_or_unverified": True,
            "voice_agent_state": str(voice_agent_state(provider_accepted=False, disclosure_confirmed=False, transcript=None)),
        },
        started_at=utc_now(),
        finished_at=utc_now(),
    )
    db.add(call)
    if not provider_config.get("provider") or not provider_config.get("dial_url"):
        db.add(
            SystemError(
                service="Phone",
                feature="phone.dial",
                severity="Warning",
                message="Phone provider is not configured; this node cannot pass full automation acceptance.",
                impact="The workflow must use message follow-up until a real telephony provider is configured.",
            )
        )
        raise RuntimeError("Phone provider is not configured.")
    if provider_config.get("provider") == "mock":
        call.external_call_id = f"mock-call-{task.id}"
        call.dial_status = task.payload.get("mock_dial_status") or "Accepted"
        call.call_result = "Mock provider accepted the dial; no live phone call was made."
        call.evidence = {
            "mock": True,
            "provider": "mock",
            "consent_disclosed": True,
            "disclosure_played": False,
            "outbound_request": {"script": disclosure_script, "phone": task.payload.get("phone"), "callback_url": provider_config.get("callback_url")},
            "voice_agent_state": str(voice_agent_state(provider_accepted=True, disclosure_confirmed=False, transcript=None)),
        }
        return
    import httpx

    payload = {
        "candidate_id": candidate_id,
        "job_id": task.payload.get("job_id"),
        "phone": task.payload.get("phone"),
        "script": disclosure_script,
        "callback_url": provider_config.get("callback_url"),
        "metadata": {"queue_task_id": task.id},
    }
    headers = {"Content-Type": "application/json"}
    if provider_config.get("api_token"):
        headers["Authorization"] = f"Bearer {provider_config['api_token']}"
    async with httpx.AsyncClient(timeout=float(provider_config.get("timeout_seconds") or 30)) as client:
        response = await client.post(provider_config["dial_url"], json=payload, headers=headers)
    if response.status_code >= 400:
        call.dial_status = "Failed"
        call.call_result = f"HTTP {response.status_code}"
        call.finished_at = utc_now()
        raise RuntimeError(f"Phone provider HTTP {response.status_code}: {response.text[:500]}")
    data = response.json()
    disclosure_confirmed = disclosure_was_confirmed(data)
    call.external_call_id = data.get("call_id") or data.get("id")
    call.dial_status = data.get("dial_status") or data.get("status") or "Accepted"
    call.call_result = data.get("call_result") or data.get("result")
    call.transcript = data.get("transcript")
    call.recording_url = data.get("recording_url")
    call.evidence = {**scrub_evidence(data), "outbound_request": scrub_evidence(payload), "disclosure_played": disclosure_confirmed}
    decision = classify_voice_transcript(call.transcript)
    state = voice_agent_state(provider_accepted=True, disclosure_confirmed=disclosure_confirmed, transcript=call.transcript, decision=decision)
    call.evidence = {**(call.evidence or {}), "voice_agent_decision": decision.__dict__, "voice_agent_state": str(state)}
    if "dnc_request" in decision.risk_flags and not has_active_dnc(db, candidate_id):
        db.add(DoNotContact(candidate_id=candidate_id, reason="Candidate requested no further contact during phone call.", evidence=str({"phone_call_id": call.external_call_id, "transcript": call.transcript})))
        call.call_result = "Candidate requested no further contact; DNC recorded and automation stopped."
        call.dial_status = "Human Review Required"
        return
    if str(call.dial_status) in SUCCESS_STATUSES and not disclosure_confirmed:
        call.dial_status = "Human Review Required"
        call.call_result = "Provider did not prove AI disclosure playback/completion; no automated success decision was made."
    if call.dial_status in SUCCESS_STATUSES and (decision.confidence < 0.8 or decision.risk_flags):
        call.dial_status = "Human Review Required"
        call.call_result = "Voice transcript was low-confidence or risky; no automated success decision was made."
    call.finished_at = utc_now() if call.dial_status in {"Connected", "No Answer", "Rejected", "Failed", "Completed"} else None
    if call.dial_status in {"No Answer", "Rejected", "Failed"} and task.payload.get("conversation_id"):
        conversation = db.get(Conversation, task.payload.get("conversation_id"))
        if not conversation:
            raise RuntimeError("Phone fallback conversation not found.")
        fallback_platform = task.payload.get("fallback_platform") or conversation.channel
        fallback_account_id = task.payload.get("fallback_account_id")
        fallback_account = db.get(PlatformAccount, fallback_account_id) if fallback_account_id else None
        fallback_candidate = db.get(Candidate, conversation.candidate_id) if conversation.candidate_id else None
        resolve_external_conversation_target(db, fallback_platform, fallback_account_id, conversation)
        enforce_platform_policy(db, platform=fallback_platform, account=fallback_account, candidate=fallback_candidate, body=task.payload.get("fallback_body"), job_id=conversation.job_id)
        message = Message(
            conversation_id=conversation.id,
            sender_type="system",
            body=task.payload.get("fallback_body") or "您好，剛才電話未能聯繫上，想跟您確認是否仍方便了解這個職缺？",
            delivery_status="Pending Send",
            metadata_json={"flow": "phone_fallback", "phone_call_id": call.external_call_id},
        )
        db.add(message)
        db.flush()
        db.add(
            QueueTask(
                queue_name="messaging",
                task_type="message.send",
                payload={"message_id": message.id, "conversation_id": conversation.id, "platform": fallback_platform, "account_id": fallback_account_id},
                idempotency_key=f"phone-fallback:{task.id}:{message.id}",
            )
        )

