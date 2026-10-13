import hashlib
import hmac
import base64
import json
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

from fastapi import APIRouter, Body, Depends, File, HTTPException, Request, Response, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from backend.app.connectors.registry import SUPPORTED_PLATFORMS, get_adapter
from backend.app.core.db import get_db
from backend.app.models.domain import (
    AIProvider,
    AIRequest,
    Application,
    ApplicationStatusHistory,
    AuditLog,
    BusinessFlowState,
    CalendarBooking,
    Candidate,
    CandidateQuestionAnswer,
    CandidateMergeConflict,
    Company,
    Conversation,
    DoNotContact,
    ExternalActionLog,
    ExternalRetentionPurge,
    InboundEmail,
    Job,
    Interview,
    MatchRecord,
    Message,
    PhoneCallLog,
    PhoneWebhookEvent,
    PlatformAutomationRun,
    PlatformWebhookEvent,
    Post,
    PlatformAccount,
    PlatformGroup,
    PlatformIdentityMap,
    Prompt,
    PromptVersion,
    QueueDlqRecord,
    QueueOutbox,
    QueueTask,
    Resume,
    RecruitmentBlacklist,
    SystemError,
    StoredDocument,
    TaskModelRoute,
    User,
    DEFAULT_COMPANY_NAME,
)
from backend.app.schemas.dto import (
    AIProviderCreate,
    AIProviderOut,
    AIRunRequest,
    AIRunResponse,
    AnalyticsSummary,
    ApplicationCreate,
    ApplicationOut,
    ApplicationStatusHistoryOut,
    ApplicationStatusUpdate,
    AuditLogOut,
    BulkCandidateImport,
    BulkCandidateImportResult,
    CalendarBookRequest,
    CandidateCreate,
    CandidateExport,
    CandidateOut,
    CandidateUpdate,
    CandidateQuestionAnswerOut,
    ConversationCreate,
    ConversationOut,
    CopywritingOut,
    CopywritingRequest,
    DoNotContactCreate,
    DoNotContactLift,
    DoNotContactOut,
    ExternalPurgeRequest,
    ExternalActionLogOut,
    InboundEmailImport,
    InboundEmailOut,
    JobCreate,
    JobOut,
    JobUpdate,
    InterviewCreate,
    InterviewOut,
    LoginRequest,
    MessageCreate,
    MessageOut,
    MatchOut,
    MatchRequest,
    PhoneCallLogOut,
    PostCreate,
    PostMetricsUpdate,
    PostOut,
    PlatformAccountCreate,
    PlatformAccountOut,
    PlatformAutomationRunOut,
    PlatformGroupCreate,
    PlatformGroupOut,
    PlatformIdentityMapCreate,
    PlatformIdentityMapOut,
    PromptCreate,
    PromptOut,
    PromptVersionOut,
    QueueDlqRecordOut,
    QueueReplayRequest,
    QueueTaskCreate,
    QueueTaskOut,
    RecruitmentBlacklistCreate,
    RecruitmentBlacklistOut,
    ResumeOut,
    ResumeParseRequest,
    SystemErrorOut,
    TaskModelRouteCreate,
    TaskModelRouteOut,
    UsageSummary,
    UserOut,
)
from backend.app.services.ai import generate_recruitment_copy, now_iso, run_task_with_router, test_provider, usage_summary
from backend.app.services.flow import ensure_required_questions, enqueue_next_question, process_inbound_candidate_message, schedule_interview_tasks, set_application_status
from backend.app.services.matching import analytics_summary, build_match
from backend.app.services.ocr import MAX_OCR_BYTES, store_resume_document
from backend.app.services.purge import request_external_purge
from backend.app.services.queue import claim_task, enqueue_outbox_for_task, process_task
from backend.app.services.redis_queue import configured_streams, redis_client, replay_dlq_record, replay_task
from backend.app.services.resume_parser import parse_resume_text
from backend.app.services.voice import SUCCESS_STATUSES, classify_voice_transcript, disclosure_was_confirmed, voice_agent_state
from backend.app.core.config import get_settings
from backend.app.services.security import create_persistent_session, encrypt_secret, require_role, revoke_current_session, verify_password


router = APIRouter()
_login_attempts: dict[str, list[float]] = {}


def audit(db: Session, actor: User | None, action: str, entity_type: str, entity_id: str | None, details: dict | None = None) -> None:
    db.add(
        AuditLog(
            actor_user_id=actor.id if actor else None,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details or {},
        )
    )


def serialize_model(model):
    return {column.name: getattr(model, column.name) for column in model.__table__.columns}


def bounded_page(limit: int = 100, offset: int = 0) -> tuple[int, int]:
    return min(max(limit, 1), 500), max(offset, 0)


ZH_TEXT = {
    "Healthy": "健康",
    "Degraded": "降級",
    "Error": "錯誤",
    "Unavailable": "無法使用",
    "Idle": "閒置",
    "Running": "執行中",
    "Manual Review Required": "需要人工審核",
    "Database fallback": "資料庫備援",
    "Redis Streams": "Redis 串流",
    "Not Configured": "未設定",
    "Configured": "已設定",
    "Connected": "已連線",
    "Paused": "已暫停",
    "Permission Lost": "權限失效",
    "Rate Limited": "速率受限",
    "Captcha Required": "需要驗證碼",
    "unconfigured": "未設定",
    "all": "全部",
    "db": "資料庫",
    "redis": "Redis",
    "chi_tra+eng": "繁體中文+英文",
}


ZH_KEYS = {
    "status": "狀態",
    "database": "資料庫",
    "redis": "Redis",
    "queue": "佇列",
    "queue_outbox_pending": "待發布佇列數",
    "worker": "工作器",
    "worker_config": "工作器設定",
    "queue_backend": "佇列後端",
    "worker_queues": "工作佇列",
    "queue_lease_seconds": "佇列租約秒數",
    "redis_consumer_group": "Redis 消費群組",
    "redis_pending_idle_ms": "Redis 待處理閒置毫秒",
    "redis_dlq_max_deliveries": "Redis 死信最大投遞次數",
    "ocr_engine": "OCR 引擎",
    "ocr_languages": "OCR 語言",
    "ocr_raster_dpi": "OCR 影像 DPI",
    "ocr_timeout_seconds": "OCR 逾時秒數",
    "retention_cleanup_interval_seconds": "保留清理間隔秒數",
    "retention_cleanup_batch_size": "保留清理批次大小",
    "external_services": "外部服務",
    "last_success_at": "最後成功時間",
    "last_failure_reason": "最後失敗原因",
    "llm_providers": "LLM 供應商",
    "name": "名稱",
    "details": "詳細資料",
    "database_error": "資料庫錯誤",
    "redis_error": "Redis 錯誤",
}


def zh_value(value, *, translate_keys: bool = False):
    if isinstance(value, str):
        return ZH_TEXT.get(value, value)
    if isinstance(value, list):
        return [zh_value(item, translate_keys=translate_keys) for item in value]
    if isinstance(value, dict):
        return {
            (ZH_KEYS.get(key, key) if translate_keys else key): zh_value(item, translate_keys=translate_keys)
            for key, item in value.items()
        }
    return value


def localized_redis_error(exc: Exception) -> str:
    message = str(exc)
    if "localhost:6379" in message and ("10061" in message or "refused" in message.lower() or "拒絕" in message):
        return "Redis 連線失敗：localhost:6379 目標電腦拒絕連線。"
    return f"Redis 連線失敗：{message}"


def wants_browser_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept


def json_html_page(title: str, payload: dict) -> HTMLResponse:
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    escaped = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return HTMLResponse(
        f"""<!doctype html>
<html lang="zh-Hant">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title}</title>
    <style>
      :root {{ --bg: #f4f7fb; --ink: #18212f; --muted: #647084; --line: #dce4ef; --accent: #0f766e; }}
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; min-height: 100vh; background: linear-gradient(180deg, rgba(37, 99, 235, .07), transparent 280px), var(--bg); color: var(--ink); font-family: "Noto Sans TC", "Microsoft JhengHei", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
      main {{ max-width: 1120px; margin: 0 auto; padding: 34px 20px; }}
      .eyebrow {{ margin: 0 0 8px; color: var(--accent); font-size: 13px; font-weight: 800; }}
      h1 {{ margin: 0 0 18px; font-size: 30px; letter-spacing: 0; }}
      pre {{ margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; background: rgba(255,255,255,.94); border: 1px solid var(--line); border-radius: 8px; padding: 20px; line-height: 1.6; box-shadow: 0 18px 45px rgba(30, 43, 66, .09); }}
    </style>
  </head>
  <body>
    <main>
      <p class="eyebrow">RecruitOps API</p>
      <h1>{title}</h1>
      <pre>{escaped}</pre>
    </main>
  </body>
</html>""",
        headers={"Cache-Control": "no-store", "Content-Language": "zh-Hant"},
    )


def find_existing_candidate(db: Session, phone: str | None, email: str | None) -> Candidate | None:
    existing = None
    if phone:
        existing = db.scalar(select(Candidate).where(Candidate.phone == phone))
    if not existing and email:
        existing = db.scalar(select(Candidate).where(Candidate.email == email))
    return existing


def enqueue_task(db: Session, task: QueueTask) -> QueueTask:
    if task.idempotency_key:
        existing = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == task.idempotency_key))
        if existing:
            return existing
    db.add(task)
    db.flush()
    enqueue_outbox_for_task(db, task)
    return task


@router.get("/health")
def health(request: Request, db: Session = Depends(get_db)):
    settings = get_settings()
    details: dict = {}
    try:
        db.execute(select(User.id).limit(1)).first()
        database = "Healthy"
    except Exception as exc:
        database = "Error"
        details["database_error"] = str(exc)
    providers = db.execute(select(AIProvider)).scalars().all()
    blocked_tasks = db.scalar(select(QueueTask).where(QueueTask.status == "Manual Review Required").limit(1))
    running_tasks = db.scalar(select(QueueTask).where(QueueTask.status == "Running").limit(1))
    platform_accounts = db.execute(select(PlatformAccount)).scalars().all()
    external = {account.platform: {"status": account.status, "last_success_at": account.last_success_at, "last_failure_reason": account.last_failure_reason} for account in platform_accounts}
    warnings = [name for name, item in external.items() if item["status"] not in {"Connected", "Configured"}]
    status = "Healthy" if database == "Healthy" and not blocked_tasks and not warnings else "Degraded"
    pending_outbox = db.scalar(select(__import__("sqlalchemy").func.count(QueueOutbox.id)).where(QueueOutbox.status.in_(["Pending", "Retry"]))) or 0
    try:
        redis_client().ping()
        redis_status = "Healthy"
    except Exception as exc:
        redis_status = "Unavailable"
        details["redis_error"] = localized_redis_error(exc)
    worker_status = "Redis Streams" if settings.queue_backend.lower() == "redis" else "Database fallback"
    if redis_status != "Healthy" and worker_status == "Redis Streams":
        status = "Degraded"
    payload = zh_value({
        "status": status,
        "database": database,
        "redis": redis_status,
        "queue": "Manual Review Required" if blocked_tasks else ("Running" if running_tasks else "Idle"),
        "queue_outbox_pending": pending_outbox,
        "worker": worker_status,
        "worker_config": {
            "queue_backend": settings.queue_backend.lower(),
            "worker_queues": sorted(settings.worker_queue_set) if settings.worker_queue_set else ["all"],
            "queue_lease_seconds": settings.queue_lease_seconds,
            "redis_consumer_group": settings.redis_consumer_group,
            "redis_pending_idle_ms": settings.redis_pending_idle_ms,
            "redis_dlq_max_deliveries": settings.redis_dlq_max_deliveries,
            "ocr_engine": settings.ocr_engine or "unconfigured",
            "ocr_languages": settings.ocr_languages,
            "ocr_raster_dpi": settings.ocr_raster_dpi,
            "ocr_timeout_seconds": settings.ocr_timeout_seconds,
            "retention_cleanup_interval_seconds": settings.retention_cleanup_interval_seconds,
            "retention_cleanup_batch_size": settings.retention_cleanup_batch_size,
        },
        "external_services": external,
        "llm_providers": [{"name": p.name, "status": p.last_status} for p in providers],
        "details": details,
    }, translate_keys=True)
    if wants_browser_html(request):
        return json_html_page("系統健康狀態", payload)
    return payload


def _check_login_rate_limit(request: Request) -> None:
    settings = get_settings()
    key = request.client.host if request.client else "unknown"
    now = time.time()
    attempts = [item for item in _login_attempts.get(key, []) if now - item < settings.login_rate_limit_window_seconds]
    if len(attempts) >= settings.login_rate_limit_max_attempts:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    attempts.append(now)
    _login_attempts[key] = attempts


@router.post("/auth/login", response_model=UserOut)
def login(payload: LoginRequest, request: Request, response: Response, db: Session = Depends(get_db)):
    _check_login_rate_limit(request)
    user = db.scalar(select(User).where(User.email == payload.email, User.active.is_(True)))
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    create_persistent_session(db, user, request, response)
    audit(db, user, "auth.login", "User", str(user.id), {"email": user.email})
    db.commit()
    return user


@router.post("/auth/logout")
def logout(request: Request, response: Response, db: Session = Depends(get_db)):
    revoke_current_session(request, response, db)
    db.commit()
    return {"ok": True}


@router.get("/auth/me", response_model=UserOut)
def me(user: User = Depends(require_role("Viewer"))):
    return user


@router.get("/jobs", response_model=list[JobOut])
def list_jobs(_user: User = Depends(require_role("Viewer")), db: Session = Depends(get_db)):
    return db.execute(select(Job).order_by(Job.created_at.desc())).scalars().all()


@router.post("/jobs", response_model=JobOut)
def create_job(payload: JobCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    values = payload.model_dump()
    company_id = values.pop("company_id")
    company = db.get(Company, company_id) if company_id is not None else db.scalar(select(Company).order_by(Company.id))
    if company_id is not None and company is None:
        raise HTTPException(status_code=422, detail="指定的公司不存在")
    if company is None:
        company = Company(name=DEFAULT_COMPANY_NAME)
        db.add(company)
        db.flush()
    job = Job(company_id=company.id, **values)
    db.add(job)
    db.flush()
    audit(db, user, "job.create", "Job", str(job.id), {"title": job.title})
    db.commit()
    db.refresh(job)
    return job


@router.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: int, _user: User = Depends(require_role("Viewer")), db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.patch("/jobs/{job_id}", response_model=JobOut)
def update_job(job_id: int, payload: JobUpdate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    changes = payload.model_dump(exclude_unset=True)
    for field, value in changes.items():
        setattr(job, field, value)
    if changes:
        job.version += 1
    audit(db, user, "job.update", "Job", str(job.id), {"fields": sorted(changes.keys()), "version": job.version})
    db.commit()
    db.refresh(job)
    return job


@router.post("/jobs/{job_id}/duplicate", response_model=JobOut)
def duplicate_job(job_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    job = db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    copy_fields = {column.name: getattr(job, column.name) for column in Job.__table__.columns if column.name not in {"id", "created_at", "updated_at"}}
    copy_fields["title"] = f"{job.title} Copy"
    copy_fields["status"] = "Draft"
    copy_fields["version"] = 1
    new_job = Job(**copy_fields)
    db.add(new_job)
    db.flush()
    audit(db, user, "job.duplicate", "Job", str(new_job.id), {"source_job_id": job.id})
    db.commit()
    db.refresh(new_job)
    return new_job


@router.post("/ai/copywriting", response_model=CopywritingOut)
def copywriting(payload: CopywritingRequest, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    job = db.get(Job, payload.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    result = generate_recruitment_copy(db, job)
    audit(db, user, "ai.copywriting", "Job", str(job.id), {"feature": "RecruitCopywriting"})
    db.commit()
    return result


@router.get("/candidates", response_model=list[CandidateOut])
def list_candidates(search: str | None = None, limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Viewer")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    query = select(Candidate)
    if search:
        pattern = f"%{search}%"
        query = query.where(or_(Candidate.name.like(pattern), Candidate.phone.like(pattern), Candidate.email.like(pattern)))
    return db.execute(query.order_by(Candidate.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/candidates", response_model=CandidateOut)
def create_candidate(payload: CandidateCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    existing = find_existing_candidate(db, payload.phone, payload.email)
    if existing:
        audit(db, user, "candidate.deduplicate", "Candidate", str(existing.id), {"phone": bool(payload.phone), "email": bool(payload.email)})
        db.commit()
        return existing
    candidate = Candidate(**payload.model_dump())
    db.add(candidate)
    db.flush()
    audit(db, user, "candidate.create", "Candidate", str(candidate.id), {"source": candidate.source_platform})
    db.commit()
    db.refresh(candidate)
    return candidate


@router.post("/imports/candidates", response_model=BulkCandidateImportResult)
def bulk_import_candidates(payload: BulkCandidateImport, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    created = 0
    deduplicated = 0
    candidate_ids: list[int] = []
    for candidate_payload in payload.candidates:
        existing = find_existing_candidate(db, candidate_payload.phone, candidate_payload.email)
        if existing:
            deduplicated += 1
            candidate_ids.append(existing.id)
            continue
        candidate = Candidate(**candidate_payload.model_dump())
        db.add(candidate)
        db.flush()
        created += 1
        candidate_ids.append(candidate.id)
    audit(db, user, "candidate.bulk_import", "Candidate", None, {"created": created, "deduplicated": deduplicated})
    db.commit()
    return {"created": created, "deduplicated": deduplicated, "candidate_ids": candidate_ids}


@router.get("/resumes", response_model=list[ResumeOut])
def list_resumes(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(Resume).order_by(Resume.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/resumes/parse", response_model=ResumeOut)
def parse_resume(payload: ResumeParseRequest, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    parsed = parse_resume_text(payload.raw_text)
    candidate_id = None
    if payload.create_candidate:
        existing = find_existing_candidate(db, parsed.get("phone"), parsed.get("email"))
        if existing:
            candidate = existing
        else:
            candidate = Candidate(
                name=parsed.get("name") or "Unknown Candidate",
                phone=parsed.get("phone"),
                email=parsed.get("email"),
                region=parsed.get("region"),
                skills=parsed.get("skills") or [],
                licenses=parsed.get("licenses") or [],
                preferred_area=parsed.get("preferred_area"),
                preferred_shift=parsed.get("preferred_shift"),
                source_platform=payload.source,
                tags=["resume_import"],
                notes=parsed.get("summary"),
            )
            db.add(candidate)
            db.flush()
        candidate_id = candidate.id
    resume = Resume(
        candidate_id=candidate_id,
        source=payload.source,
        file_name=payload.file_name,
        raw_text=payload.raw_text,
        parsed_json=parsed,
    )
    db.add(resume)
    db.flush()
    audit(db, user, "resume.parse", "Resume", str(resume.id), {"candidate_id": candidate_id, "missing_info": parsed.get("missing_info", [])})
    db.commit()
    db.refresh(resume)
    return resume


@router.post("/resumes/ocr", response_model=QueueTaskOut)
async def ocr_resume_pdf(
    file: UploadFile = File(...),
    candidate_id: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_role("Recruiter / HR")),
):
    if file.content_type not in {"application/pdf", "application/octet-stream"}:
        raise HTTPException(status_code=415, detail="Only PDF resumes are accepted for OCR.")
    chunks: list[bytes] = []
    total = 0
    first_chunk = b""
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        if not first_chunk:
            first_chunk = chunk[:8]
            if not first_chunk.startswith(b"%PDF-"):
                raise HTTPException(status_code=415, detail="Uploaded file is not a valid PDF by magic bytes.")
        total += len(chunk)
        if total > MAX_OCR_BYTES:
            raise HTTPException(status_code=413, detail="Resume PDF exceeds the configured OCR size limit.")
        chunks.append(chunk)
    content = b"".join(chunks)
    if not content.startswith(b"%PDF-") or b"%%EOF" not in content[-2048:]:
        raise HTTPException(status_code=415, detail="Uploaded file is not a structurally valid PDF.")
    try:
        document = store_resume_document(db, file_name=file.filename or "resume.pdf", content=content, content_type=file.content_type or "application/pdf", candidate_id=candidate_id)
    except ValueError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    task = enqueue_task(
        db,
        QueueTask(
            queue_name="ocr",
            task_type="resume.ocr_pdf",
            payload={"document_id": document.id},
            idempotency_key=f"resume-ocr-document:{document.id}",
            max_retries=1,
        ),
    )
    audit(db, user, "resume.ocr.request", "StoredDocument", str(document.id), {"queue_task_id": task.id, "candidate_id": candidate_id})
    db.commit()
    db.refresh(task)
    return task


@router.get("/inbound-emails", response_model=list[InboundEmailOut])
def list_inbound_emails(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(InboundEmail).order_by(InboundEmail.created_at.desc()).offset(offset).limit(limit)).scalars().all()


ALLOWED_518_HOSTS = {"www.518.com.tw", "518.com.tw", "m.518.com.tw"}


def is_allowed_518_link(url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    return parsed.scheme in {"https", "http"} and parsed.hostname in ALLOWED_518_HOSTS


@router.post("/inbound-emails/import", response_model=InboundEmailOut)
def import_inbound_email(payload: InboundEmailImport, db: Session = Depends(get_db), user: User | None = Depends(require_role("Recruiter / HR")), commit: bool = True):
    existing = db.scalar(select(InboundEmail).where(InboundEmail.message_id == payload.message_id))
    if existing:
        audit(db, user, "email.duplicate", "InboundEmail", str(existing.id), {"message_id": payload.message_id})
        db.commit()
        return existing

    parsed = parse_resume_text(payload.raw_text)
    platform_link = None
    link_match = __import__("re").search(r"https?://\S+", payload.raw_text)
    if link_match:
        platform_link = link_match.group(0).rstrip(").,]")

    candidate_id = None
    application_id = None
    import_status = "Parsed"
    parse_error = None
    has_resume_contact = bool(parsed.get("phone") or parsed.get("email"))
    if has_resume_contact:
        existing_candidate = find_existing_candidate(db, parsed.get("phone"), parsed.get("email"))
        if existing_candidate:
            candidate_id = existing_candidate.id
            conflicts = {}
            if parsed.get("phone") and existing_candidate.phone and parsed.get("phone") != existing_candidate.phone:
                conflicts["phone"] = parsed.get("phone")
            if parsed.get("email") and existing_candidate.email and parsed.get("email") != existing_candidate.email:
                conflicts["email"] = parsed.get("email")
            if conflicts:
                db.add(CandidateMergeConflict(candidate_id=existing_candidate.id, incoming_source=payload.source_platform, conflict_type="contact_conflict", incoming_payload=parsed, existing_payload=serialize_model(existing_candidate)))
                import_status = "Merge Conflict"
        else:
            candidate = Candidate(
                name=parsed.get("name") or "Unknown Candidate",
                phone=parsed.get("phone"),
                email=parsed.get("email"),
                region=parsed.get("region"),
                skills=parsed.get("skills") or [],
                licenses=parsed.get("licenses") or [],
                preferred_area=parsed.get("preferred_area"),
                preferred_shift=parsed.get("preferred_shift"),
                source_platform=payload.source_platform,
                tags=["email_import"],
                notes=parsed.get("summary"),
            )
            db.add(candidate)
            db.flush()
            candidate_id = candidate.id
        if payload.job_id and candidate_id and import_status != "Merge Conflict":
            application = Application(candidate_id=candidate_id, job_id=payload.job_id, status="New")
            db.add(application)
            db.flush()
            application_id = application.id
    else:
        if platform_link and not is_allowed_518_link(platform_link):
            import_status = "Parse Failed"
            parse_error = "Resume notification link is outside the allowed 518 domains."
        else:
            import_status = "Pending Resume Fetch" if platform_link else "Parse Failed"
            parse_error = "Email notification did not include complete resume contact data." if not platform_link else None

    inbound = InboundEmail(
        message_id=payload.message_id,
        sender=payload.sender,
        received_at=payload.received_at,
        subject=payload.subject,
        source_platform=payload.source_platform,
        job_id=payload.job_id,
        candidate_id=candidate_id,
        application_id=application_id,
        raw_text=payload.raw_text,
        attachment_names=payload.attachment_names,
        import_status=import_status,
        parse_error=parse_error,
        platform_link=platform_link,
        mailbox=payload.mailbox,
        imap_uid=payload.imap_uid,
        uid_validity=payload.uid_validity,
    )
    db.add(inbound)
    db.flush()
    if import_status == "Pending Resume Fetch" and is_allowed_518_link(platform_link):
        enqueue_task(
            db,
            QueueTask(
                queue_name="platform-worker",
                task_type="platform.fetch_resume",
                payload={"platform": payload.source_platform, "platform_link": platform_link, "inbound_email_id": inbound.id},
                idempotency_key=f"email-fetch-resume:{payload.message_id}",
                max_retries=1,
            ),
        )
    audit(db, user, "email.import", "InboundEmail", str(inbound.id), {"status": import_status, "candidate_id": candidate_id, "application_id": application_id})
    if commit:
        db.commit()
        db.refresh(inbound)
    return inbound


@router.get("/applications", response_model=list[ApplicationOut])
def list_applications(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(Application).order_by(Application.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/applications", response_model=ApplicationOut)
def create_application(payload: ApplicationCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    if not db.get(Candidate, payload.candidate_id):
        raise HTTPException(status_code=404, detail="Candidate not found")
    if not db.get(Job, payload.job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    application = Application(**payload.model_dump())
    db.add(application)
    db.flush()
    ensure_required_questions(db, application)
    db.add(
        ApplicationStatusHistory(
            application_id=application.id,
            from_status=None,
            to_status=application.status,
            changed_by=user.id,
            reason="Application created",
        )
    )
    audit(db, user, "application.create", "Application", str(application.id), {"candidate_id": application.candidate_id, "job_id": application.job_id})
    db.commit()
    db.refresh(application)
    return application


@router.patch("/applications/{application_id}/status", response_model=ApplicationOut)
def update_application_status(application_id: int, payload: ApplicationStatusUpdate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    application = db.get(Application, application_id)
    if not application:
        raise HTTPException(status_code=404, detail="Application not found")
    old_status = application.status
    set_application_status(
        db,
        application,
        to_status=payload.status,
        source="manual",
        changed_by=user.id,
        reason=payload.reason,
        event_key=f"application-status:{application.id}:{payload.status}:{hashlib.sha256((payload.reason or '').encode('utf-8')).hexdigest()[:16]}:{int(time.time())}",
    )
    audit(db, user, "application.status", "Application", str(application.id), {"from": old_status, "to": payload.status})
    db.commit()
    db.refresh(application)
    return application


@router.get("/applications/{application_id}/history", response_model=list[ApplicationStatusHistoryOut])
def application_history(application_id: int, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    return db.execute(
        select(ApplicationStatusHistory)
        .where(ApplicationStatusHistory.application_id == application_id)
        .order_by(ApplicationStatusHistory.created_at)
    ).scalars().all()


@router.get("/candidates/{candidate_id}", response_model=CandidateOut)
def get_candidate(candidate_id: int, _user: User = Depends(require_role("Viewer")), db: Session = Depends(get_db)):
    candidate = db.get(Candidate, candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return candidate


@router.patch("/candidates/{candidate_id}", response_model=CandidateOut)
def update_candidate(candidate_id: int, payload: CandidateUpdate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    candidate = db.get(Candidate, candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    changes = payload.model_dump(exclude_unset=True)
    for field, value in changes.items():
        setattr(candidate, field, value)
    audit(db, user, "candidate.update", "Candidate", str(candidate.id), {"fields": sorted(changes.keys())})
    db.commit()
    db.refresh(candidate)
    return candidate


@router.get("/candidates/{candidate_id}/export", response_model=CandidateExport)
def export_candidate(candidate_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    candidate = db.get(Candidate, candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    applications = db.execute(select(Application).where(Application.candidate_id == candidate_id)).scalars().all()
    resumes = db.execute(select(Resume).where(Resume.candidate_id == candidate_id)).scalars().all()
    stored_documents = db.execute(select(StoredDocument).where(StoredDocument.candidate_id == candidate_id)).scalars().all()
    interviews = db.execute(select(Interview).where(Interview.candidate_id == candidate_id)).scalars().all()
    matches = db.execute(select(MatchRecord).where(MatchRecord.candidate_id == candidate_id)).scalars().all()
    conversations = db.execute(select(Conversation).where(Conversation.candidate_id == candidate_id)).scalars().all()
    conversation_ids = [conversation.id for conversation in conversations]
    messages = []
    if conversation_ids:
        messages = db.execute(select(Message).where(Message.conversation_id.in_(conversation_ids))).scalars().all()
    audit(db, user, "candidate.export", "Candidate", str(candidate_id), {"applications": len(applications), "conversations": len(conversations)})
    db.commit()
    return {
        "candidate": jsonable_encoder(serialize_model(candidate)),
        "resumes": jsonable_encoder([serialize_model(item) for item in resumes]),
        "stored_documents": jsonable_encoder([serialize_model(item) for item in stored_documents]),
        "applications": jsonable_encoder([serialize_model(item) for item in applications]),
        "interviews": jsonable_encoder([serialize_model(item) for item in interviews]),
        "matches": jsonable_encoder([serialize_model(item) for item in matches]),
        "conversations": jsonable_encoder([serialize_model(item) for item in conversations]),
        "messages": jsonable_encoder([serialize_model(item) for item in messages]),
    }


@router.delete("/candidates/{candidate_id}", response_model=CandidateOut)
def delete_candidate_data(candidate_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    candidate = db.get(Candidate, candidate_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    candidate.name = f"Deleted Candidate {candidate.id}"
    candidate.phone = None
    candidate.email = None
    candidate.platform_id = None
    candidate.notes = "Personal data deleted by authorized request."
    candidate.tags = ["deleted"]
    candidate.skills = []
    candidate.licenses = []
    candidate.status = "Closed"
    candidate.expected_salary = None
    candidate.preferred_area = None
    candidate.preferred_shift = None
    resumes = db.execute(select(Resume).where(Resume.candidate_id == candidate_id)).scalars().all()
    for resume in resumes:
        resume.raw_text = "[deleted by authorized privacy request]"
        resume.parsed_json = {}
        resume.status = "Deleted"
    stored_documents = db.execute(select(StoredDocument).where(StoredDocument.candidate_id == candidate_id)).scalars().all()
    stored_document_ids = [document.id for document in stored_documents]
    purge_ids: list[int] = []
    for document in stored_documents:
        document.status = "Deletion Pending"
        document.deleted_at = None
        document.delete_audit = {"requested_by_user_id": user.id, "requested_at": now_iso(), "storage_path": document.storage_path, "status": "purge_queued"}
        purge = request_external_purge(db, provider="local_file", object_key=document.storage_path, purge_type="candidate_document", legal_hold=bool((document.retention_policy or {}).get("legal_hold")))
        purge_ids.append(purge.id)
        enqueue_task(
            db,
            QueueTask(
                queue_name="privacy",
                task_type="privacy.external_purge",
                payload={"purge_id": purge.id, "dry_run": False, "stored_document_id": document.id},
                idempotency_key=f"candidate-document-purge:{document.id}",
                max_retries=5,
            ),
        )
    conversations = db.execute(select(Conversation).where(Conversation.candidate_id == candidate_id)).scalars().all()
    conversation_ids = [conversation.id for conversation in conversations]
    if conversation_ids:
        messages = db.execute(select(Message).where(Message.conversation_id.in_(conversation_ids))).scalars().all()
        for message in messages:
            message.body = "[deleted by authorized privacy request]"
            message.metadata_json = {}
    maps = db.execute(select(PlatformIdentityMap).where(PlatformIdentityMap.candidate_id == candidate_id)).scalars().all()
    for item in maps:
        item.external_candidate_id = None
        item.external_conversation_id = None
        item.external_job_id = None
        item.metadata_json = {"deleted": True}
    applications = db.execute(select(Application).where(Application.candidate_id == candidate_id)).scalars().all()
    application_ids = [application.id for application in applications]
    for application in applications:
        application.ai_notes = "[deleted by authorized privacy request]"
        application.decision_event_key = None
    task_filters = [QueueTask.payload["candidate_id"].as_integer() == candidate_id]
    if stored_document_ids:
        task_filters.append(QueueTask.payload["document_id"].as_integer().in_(stored_document_ids))
        task_filters.append(QueueTask.payload["stored_document_id"].as_integer().in_(stored_document_ids))
    if application_ids:
        task_filters.append(QueueTask.payload["application_id"].as_integer().in_(application_ids))
    if conversation_ids:
        task_filters.append(QueueTask.payload["conversation_id"].as_integer().in_(conversation_ids))
    tasks = db.execute(select(QueueTask).where(or_(*task_filters), QueueTask.task_type != "privacy.external_purge")).scalars().all()
    for task in tasks:
        task.payload = {"deleted_candidate_id": candidate_id}
        if task.status in {"Queued", "Retry", "Running"}:
            task.status = "Cancelled"
            task.last_error = "Cancelled by authorized privacy deletion."
    requests = db.execute(select(AIRequest).where(AIRequest.candidate_id == candidate_id)).scalars().all()
    for request in requests:
        request.prompt_masked = "[deleted by authorized privacy request]"
        request.response_masked = "[deleted by authorized privacy request]"
    audit(
        db,
        user,
        "candidate.delete_data",
        "Candidate",
        str(candidate.id),
        {"mode": "anonymize_and_purge_documents", "resumes": len(resumes), "stored_documents": len(stored_documents), "purge_ids": purge_ids, "conversations": len(conversations), "identity_maps": len(maps), "queue_tasks": len(tasks), "ai_requests": len(requests)},
    )
    db.commit()
    db.refresh(candidate)
    return candidate


@router.get("/platforms")
async def platforms(_user: User = Depends(require_role("Viewer"))):
    data = []
    for key, label in SUPPORTED_PLATFORMS.items():
        status = await get_adapter(key).get_status()
        data.append({"platform": key, "adapter": label, "status": status.status, "message": status.message})
    return data


@router.get("/platform-accounts", response_model=list[PlatformAccountOut])
def list_platform_accounts(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    return db.execute(select(PlatformAccount).order_by(PlatformAccount.platform, PlatformAccount.display_name)).scalars().all()


@router.post("/platform-accounts", response_model=PlatformAccountOut)
def create_platform_account(payload: PlatformAccountCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    account = PlatformAccount(**payload.model_dump())
    db.add(account)
    db.flush()
    audit(db, user, "platform_account.create", "PlatformAccount", str(account.id), {"platform": account.platform, "status": account.status})
    db.commit()
    db.refresh(account)
    return account


@router.get("/platform-identity-maps", response_model=list[PlatformIdentityMapOut])
def list_platform_identity_maps(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(PlatformIdentityMap).order_by(PlatformIdentityMap.platform, PlatformIdentityMap.entity_type, PlatformIdentityMap.id.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/platform-identity-maps", response_model=PlatformIdentityMapOut)
def create_platform_identity_map(payload: PlatformIdentityMapCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    if payload.account_id and not db.get(PlatformAccount, payload.account_id):
        raise HTTPException(status_code=404, detail="Platform account not found")
    if payload.candidate_id and not db.get(Candidate, payload.candidate_id):
        raise HTTPException(status_code=404, detail="Candidate not found")
    if payload.conversation_id and not db.get(Conversation, payload.conversation_id):
        raise HTTPException(status_code=404, detail="Conversation not found")
    if payload.job_id and not db.get(Job, payload.job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    mapping = PlatformIdentityMap(**payload.model_dump())
    db.add(mapping)
    db.flush()
    audit(db, user, "platform_identity_map.create", "PlatformIdentityMap", str(mapping.id), {"platform": mapping.platform, "entity_type": mapping.entity_type})
    db.commit()
    db.refresh(mapping)
    return mapping


@router.get("/platform-groups", response_model=list[PlatformGroupOut])
def list_platform_groups(_user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    return db.execute(select(PlatformGroup).order_by(PlatformGroup.area, PlatformGroup.name)).scalars().all()


@router.post("/platform-groups", response_model=PlatformGroupOut)
def create_platform_group(payload: PlatformGroupCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    if not db.get(PlatformAccount, payload.platform_account_id):
        raise HTTPException(status_code=404, detail="Platform account not found")
    group = PlatformGroup(**payload.model_dump())
    db.add(group)
    db.flush()
    audit(db, user, "platform_group.create", "PlatformGroup", str(group.id), {"platform_account_id": group.platform_account_id, "area": group.area})
    db.commit()
    db.refresh(group)
    return group


@router.post("/platforms/{platform}/test")
async def test_platform(platform: str, _user: User = Depends(require_role("Manager"))):
    result = await get_adapter(platform).test_connection()
    return result.__dict__


@router.get("/posts", response_model=list[PostOut])
def list_posts(_user: User = Depends(require_role("Viewer")), db: Session = Depends(get_db)):
    return db.execute(select(Post).order_by(Post.created_at.desc())).scalars().all()


@router.post("/posts", response_model=PostOut)
def create_post(payload: PostCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    post = Post(**payload.model_dump())
    db.add(post)
    db.flush()
    task = enqueue_task(
        db,
        QueueTask(
            queue_name="posting",
            task_type="platform.create_post",
            payload={"post_id": post.id, "platform": post.platform, "job_id": post.job_id},
            idempotency_key=f"post:{post.platform}:{post.job_id}:{post.account_id}:{post.group_id}:{post.scheduled_at or 'now'}",
            scheduled_at=post.scheduled_at,
        )
    )
    audit(db, user, "post.queue", "Post", str(post.id), {"platform": post.platform, "queue_task_id": task.id, "status": "Queued"})
    db.commit()
    db.refresh(post)
    return post


@router.patch("/posts/{post_id}/metrics", response_model=PostOut)
def update_post_metrics(post_id: int, payload: PostMetricsUpdate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    post = db.get(Post, post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    verified_statuses = {"Delivered", "Published", "Confirmed", "Completed"}
    if payload.status in verified_statuses and not (payload.platform_post_id and payload.verified_at):
        raise HTTPException(status_code=409, detail="Verified/success status requires platform_post_id and verified_at evidence. Run the worker verification task instead.")
    if payload.status is not None:
        post.status = payload.status
    if payload.post_url is not None:
        post.post_url = payload.post_url
    if payload.platform_post_id is not None:
        post.platform_post_id = payload.platform_post_id
    if payload.verified_at is not None:
        post.verified_at = payload.verified_at
    if payload.error_reason is not None:
        post.error_reason = payload.error_reason
    post.metrics = {**(post.metrics or {}), **payload.metrics}
    audit(db, user, "post.metrics", "Post", str(post.id), {"status": post.status, "metrics": payload.metrics})
    db.commit()
    db.refresh(post)
    return post


@router.get("/ai/providers", response_model=list[AIProviderOut])
def list_ai_providers(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    return db.execute(select(AIProvider).order_by(AIProvider.name)).scalars().all()


@router.post("/ai/providers", response_model=AIProviderOut)
def create_ai_provider(payload: AIProviderCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Super Admin"))):
    provider = AIProvider(**payload.model_dump(exclude={"api_key"}), api_key_encrypted=encrypt_secret(payload.api_key))
    db.add(provider)
    db.flush()
    audit(db, user, "ai_provider.create", "AIProvider", str(provider.id), {"name": provider.name, "api_key": "***" if payload.api_key else None})
    db.commit()
    db.refresh(provider)
    return provider


@router.post("/ai/providers/{provider_id}/test", response_model=AIProviderOut)
async def test_ai_provider(provider_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    provider = db.get(AIProvider, provider_id)
    if not provider:
        raise HTTPException(status_code=404, detail="Provider not found")
    status, latency, message, http_status = await test_provider(provider)
    provider.last_status = status
    provider.last_latency_ms = latency
    provider.last_tested_at = now_iso()
    audit(
        db,
        user,
        "ai_provider.test",
        "AIProvider",
        str(provider.id),
        {"status": status, "http_status": http_status, "message": message},
    )
    db.commit()
    db.refresh(provider)
    result = AIProviderOut.model_validate(provider).model_dump()
    result.update(test_http_status=http_status, test_message=message)
    return result


@router.get("/usage", response_model=UsageSummary)
def usage(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    return usage_summary(db)


@router.get("/ai/routes", response_model=list[TaskModelRouteOut])
def list_ai_routes(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    return db.execute(select(TaskModelRoute).order_by(TaskModelRoute.task_name)).scalars().all()


def _required_ai_capability(task_name: str, budget_policy: dict | None) -> str:
    name = task_name.strip().lower()
    policy = budget_policy or {}
    if policy.get("requires_vision") or "vision" in name or "visual" in name or "screenshot" in name:
        return "supports_vision"
    if policy.get("requires_tools"):
        return "supports_tools"
    if policy.get("expected_json_schema") or policy.get("requires_json_schema"):
        return "supports_json_schema"
    return "supports_text"


def _ensure_route_providers_have_capability(db: Session, payload: TaskModelRouteCreate) -> None:
    required = _required_ai_capability(payload.task_name, payload.budget_policy)
    provider_ids = [item for item in [payload.primary_provider_id, *(payload.fallback_provider_ids or [])] if item]
    if not provider_ids:
        return
    providers = {provider.id: provider for provider in db.execute(select(AIProvider).where(AIProvider.id.in_(provider_ids))).scalars().all()}
    missing = [str(provider_id) for provider_id in provider_ids if provider_id not in providers]
    if missing:
        raise HTTPException(status_code=404, detail=f"AI provider(s) not found: {', '.join(missing)}")
    unsupported = [provider.name for provider in providers.values() if not getattr(provider, required)]
    if unsupported:
        raise HTTPException(status_code=409, detail=f"Route requires {required}; unsupported provider(s): {', '.join(unsupported)}")


@router.post("/ai/routes", response_model=TaskModelRouteOut)
def upsert_ai_route(payload: TaskModelRouteCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Super Admin"))):
    _ensure_route_providers_have_capability(db, payload)
    route = db.scalar(select(TaskModelRoute).where(TaskModelRoute.task_name == payload.task_name))
    if route:
        route.primary_provider_id = payload.primary_provider_id
        route.fallback_provider_ids = payload.fallback_provider_ids
        route.budget_policy = payload.budget_policy
        action = "ai_route.update"
    else:
        route = TaskModelRoute(**payload.model_dump())
        db.add(route)
        action = "ai_route.create"
    db.flush()
    audit(db, user, action, "TaskModelRoute", str(route.id), {"task_name": route.task_name})
    db.commit()
    db.refresh(route)
    return route


@router.post("/ai/run", response_model=AIRunResponse)
async def run_ai(payload: AIRunRequest, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    result = await run_task_with_router(db, payload.task_name, payload.prompt, payload.candidate_id, payload.job_id)
    audit(db, user, "ai.run", "TaskModelRoute", payload.task_name, {"status": result["status"], "provider": result["provider"]})
    db.commit()
    return result


@router.get("/prompts", response_model=list[PromptOut])
def list_prompts(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    return db.execute(select(Prompt).order_by(Prompt.name)).scalars().all()


@router.post("/prompts", response_model=PromptOut)
def create_prompt(payload: PromptCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    prompt = db.scalar(select(Prompt).where(Prompt.name == payload.name))
    if prompt:
        next_version = prompt.active_version + 1
        prompt.purpose = payload.purpose
        prompt.active_version = next_version
        action = "prompt.version"
    else:
        prompt = Prompt(name=payload.name, purpose=payload.purpose, active_version=1)
        db.add(prompt)
        db.flush()
        next_version = 1
        action = "prompt.create"
    db.add(
        PromptVersion(
            prompt_id=prompt.id,
            version=next_version,
            system_prompt=payload.system_prompt,
            template=payload.template,
            model_hint=payload.model_hint,
            temperature=payload.temperature,
            created_by=user.id,
        )
    )
    audit(db, user, action, "Prompt", str(prompt.id), {"name": prompt.name, "version": next_version})
    db.commit()
    db.refresh(prompt)
    return prompt


@router.get("/prompts/{prompt_id}/versions", response_model=list[PromptVersionOut])
def list_prompt_versions(prompt_id: int, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    return db.execute(select(PromptVersion).where(PromptVersion.prompt_id == prompt_id).order_by(PromptVersion.version.desc())).scalars().all()


@router.get("/system/errors", response_model=list[SystemErrorOut])
def list_system_errors(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(SystemError).order_by(SystemError.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.get("/queue/tasks", response_model=list[QueueTaskOut])
def list_queue_tasks(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(QueueTask).order_by(QueueTask.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.get("/queue/outbox")
def list_queue_outbox(limit: int = 100, offset: int = 0, status: str | None = None, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    query = select(QueueOutbox).order_by(QueueOutbox.created_at.desc()).offset(offset).limit(limit)
    if status:
        query = query.where(QueueOutbox.status == status)
    return [serialize_model(row) for row in db.execute(query).scalars().all()]


@router.get("/queue/dlq")
def list_queue_dlq(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    base = select(QueueDlqRecord)
    total = db.scalar(select(func.count()).select_from(base.subquery())) or 0
    records = db.execute(
        base
        .order_by(QueueDlqRecord.created_at.desc())
        .offset(offset)
        .limit(limit)
    ).scalars().all()
    return {"total": total, "items": [QueueDlqRecordOut.model_validate(record).model_dump() for record in records]}


@router.post("/queue/dlq/{record_id}/replay", response_model=QueueDlqRecordOut)
def replay_queue_dlq_record(record_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    try:
        record = replay_dlq_record(db, record_id)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 409
        raise HTTPException(status_code=status_code, detail=detail) from exc
    audit(db, user, "queue.dlq_replay", "QueueDlqRecord", str(record.id), {"queue_task_id": record.queue_task_id, "replayed_task_id": record.replayed_task_id})
    db.commit()
    db.refresh(record)
    return record


@router.get("/queue/redis/metrics")
def queue_redis_metrics(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    queue_status = {
        status: db.scalar(select(func.count(QueueTask.id)).where(QueueTask.status == status)) or 0
        for status in ["Queued", "Retry", "Running", "Manual Review Required", "Completed", "Cancelled"]
    }
    outbox_status = {
        status: db.scalar(select(func.count(QueueOutbox.id)).where(QueueOutbox.status == status)) or 0
        for status in ["Pending", "Retry", "Published"]
    }
    dlq_status = {
        status: db.scalar(select(func.count(QueueDlqRecord.id)).where(QueueDlqRecord.status == status)) or 0
        for status in ["Open", "Moving", "Ack Pending", "Redis Failed", "Replay In Progress", "Replay Failed", "Replay Blocked", "Replayed"]
    }
    dlq_total = sum(dlq_status.values())
    dlq_warn_threshold = int(get_settings().redis_dlq_warn_threshold or 1000)
    due_backlog = db.scalar(
        select(func.count(QueueTask.id))
        .where(QueueTask.status.in_(["Queued", "Retry"]))
        .where(or_(QueueTask.scheduled_at.is_(None), QueueTask.scheduled_at <= now_iso()))
    ) or 0
    metrics = {
        "backend": get_settings().queue_backend,
        "configured_streams": configured_streams(db),
        "queue_by_status": queue_status,
        "outbox_by_status": outbox_status,
        "dlq_by_status": dlq_status,
        "due_backlog": due_backlog,
        "dlq_total": dlq_total,
        "dlq_capacity": {"warning_threshold": dlq_warn_threshold, "alert": dlq_total >= dlq_warn_threshold},
        "manual_review_count": queue_status["Manual Review Required"],
        "redis": {"status": "Unavailable", "streams": {}},
    }
    try:
        client = redis_client()
        client.ping()
        metrics["redis"]["status"] = "Healthy"
        for stream in configured_streams(db):
            try:
                info = client.xinfo_stream(stream)
                groups = client.xinfo_groups(stream)
                metrics["redis"]["streams"][stream] = {
                    "length": info.get("length"),
                    "first_entry": info.get("first-entry"),
                    "last_entry": info.get("last-entry"),
                    "groups": [
                        {
                            "name": group.get("name"),
                            "consumers": group.get("consumers"),
                            "pending": group.get("pending"),
                            "lag": group.get("lag"),
                        }
                        for group in groups
                    ],
                }
            except Exception as exc:
                metrics["redis"]["streams"][stream] = {"error": str(exc)}
    except Exception as exc:
        metrics["redis"]["error"] = str(exc)
    return metrics


@router.get("/flow/states")
def list_flow_states(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return [
        serialize_model(item)
        for item in db.execute(select(BusinessFlowState).order_by(BusinessFlowState.updated_at.desc()).offset(offset).limit(limit)).scalars().all()
    ]


@router.get("/operations/metrics")
def operations_metrics(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    queue_status = {}
    for status in ["Queued", "Retry", "Running", "Manual Review Required", "Completed", "Cancelled"]:
        queue_status[status] = db.scalar(select(__import__("sqlalchemy").func.count(QueueTask.id)).where(QueueTask.status == status)) or 0
    oldest = db.scalar(select(QueueTask).where(QueueTask.status.in_(["Queued", "Retry", "Running"])).order_by(QueueTask.created_at).limit(1))
    platform_errors = {}
    for feature in ["facebook", "line", "browser", "calendar", "phone", "ocr"]:
        platform_errors[feature] = db.scalar(select(__import__("sqlalchemy").func.count(SystemError.id)).where(SystemError.feature.ilike(f"%{feature}%"), SystemError.status == "Open")) or 0
    return {
        "queue_depth": int(queue_status["Queued"] + queue_status["Retry"] + queue_status["Running"]),
        "queue_by_status": queue_status,
        "oldest_active_task_created_at": oldest.created_at.isoformat() if oldest else None,
        "manual_review_count": int(queue_status["Manual Review Required"]),
        "open_error_counts": platform_errors,
        "retention_backlog_runs": db.scalar(select(__import__("sqlalchemy").func.count(DataRetentionRun.id)).where(DataRetentionRun.status != "Completed")) or 0,
        "external_purge_pending": db.scalar(select(__import__("sqlalchemy").func.count(ExternalRetentionPurge.id)).where(ExternalRetentionPurge.status.in_(["Pending", "Manual Review Required"]))) or 0,
    }


@router.post("/privacy/external-purges", response_model=QueueTaskOut)
def create_external_purge(payload: ExternalPurgeRequest, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    purge = request_external_purge(db, provider=payload.provider, object_key=payload.object_key, purge_type=payload.purge_type, legal_hold=payload.legal_hold)
    task = enqueue_task(
        db,
        QueueTask(
            queue_name="privacy",
            task_type="privacy.external_purge",
            payload={"purge_id": purge.id, "dry_run": payload.dry_run},
            idempotency_key=f"external-purge:{purge.id}:{payload.dry_run}",
            max_retries=3,
        ),
    )
    audit(db, user, "privacy.external_purge.request", "ExternalRetentionPurge", str(purge.id), {"queue_task_id": task.id, "dry_run": payload.dry_run})
    db.commit()
    db.refresh(task)
    return task


@router.get("/automation/runs", response_model=list[PlatformAutomationRunOut])
def list_automation_runs(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(PlatformAutomationRun).order_by(PlatformAutomationRun.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.get("/automation/actions", response_model=list[ExternalActionLogOut])
def list_external_actions(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(ExternalActionLog).order_by(ExternalActionLog.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.get("/phone/calls", response_model=list[PhoneCallLogOut])
def list_phone_calls(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(PhoneCallLog).order_by(PhoneCallLog.created_at.desc()).offset(offset).limit(limit)).scalars().all()


def _verify_phone_webhook_signature(request: Request, body: bytes) -> None:
    settings = get_settings()
    if not settings.phone_webhook_secret:
        raise HTTPException(status_code=503, detail="Phone webhook secret is not configured")
    timestamp = request.headers.get("x-phone-timestamp")
    nonce = request.headers.get("x-phone-nonce")
    signature = request.headers.get("x-phone-signature")
    if not timestamp or not nonce or not signature:
        raise HTTPException(status_code=401, detail="Missing phone webhook signature headers")
    try:
        ts = int(timestamp)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid phone webhook timestamp") from exc
    now = int(time.time())
    if abs(now - ts) > settings.webhook_timestamp_tolerance_seconds:
        raise HTTPException(status_code=401, detail="Phone webhook timestamp outside tolerance")
    expected = hmac.new(settings.phone_webhook_secret.encode("utf-8"), f"{timestamp}.{nonce}.".encode("utf-8") + body, hashlib.sha256).hexdigest()
    supplied = signature.removeprefix("sha256=").strip()
    if not hmac.compare_digest(expected, supplied):
        raise HTTPException(status_code=401, detail="Invalid phone webhook signature")


def _webhook_account(db: Session, platform: str, account_id: int | None = None) -> PlatformAccount | None:
    if account_id:
        return db.get(PlatformAccount, account_id)
    return db.scalar(
        select(PlatformAccount)
        .where(PlatformAccount.platform == platform)
        .where(PlatformAccount.status.in_(["Configured", "Connected"]))
        .order_by(PlatformAccount.id.desc())
        .limit(1)
    )


def _record_platform_webhook_event(
    db: Session,
    *,
    platform: str,
    account: PlatformAccount | None,
    event_id: str,
    body: bytes,
    canonical_event: dict | None = None,
    external_conversation_id: str | None = None,
    external_message_id: str | None = None,
    evidence: dict | None = None,
) -> PlatformWebhookEvent:
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_hash = hashlib.sha256(json.dumps(jsonable_encoder(canonical_event or evidence or {}), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    existing = db.scalar(
        select(PlatformWebhookEvent).where(
            PlatformWebhookEvent.platform == platform,
            PlatformWebhookEvent.account_id == (account.id if account else None),
            PlatformWebhookEvent.event_id == event_id,
        )
    )
    if existing:
        existing_hash = existing.canonical_event_hash or existing.payload_hash
        incoming_hash = canonical_hash if existing.canonical_event_hash else payload_hash
        if existing_hash != incoming_hash:
            raise HTTPException(status_code=409, detail=f"{platform} webhook event id collision detected")
        existing.status = "Duplicate"
        existing.evidence = {**(existing.evidence or {}), "duplicate_replayed_at": now_iso()}
        return existing
    event = PlatformWebhookEvent(
        platform=platform,
        account_id=account.id if account else None,
        event_id=event_id,
        external_conversation_id=external_conversation_id,
        external_message_id=external_message_id,
        received_at=now_iso(),
        payload_hash=payload_hash,
        canonical_event_hash=canonical_hash,
        raw_body_b64=base64.b64encode(body).decode("ascii"),
        evidence={**(evidence or {}), "raw_body_sha256": payload_hash},
    )
    db.add(event)
    try:
        db.flush()
    except IntegrityError as exc:
        db.rollback()
        existing = db.scalar(
            select(PlatformWebhookEvent).where(
                PlatformWebhookEvent.platform == platform,
                PlatformWebhookEvent.account_id == (account.id if account else None),
                PlatformWebhookEvent.event_id == event_id,
            )
        )
        if existing and (existing.canonical_event_hash or existing.payload_hash) in {canonical_hash, payload_hash}:
            existing.status = "Duplicate"
            return existing
        raise HTTPException(status_code=409, detail=f"{platform} webhook event collision detected") from exc
    return event


def _stable_event_id(prefix: str, event: dict) -> str:
    encoded = jsonable_encoder(event)
    return f"{prefix}:hash:{hashlib.sha256(repr(encoded).encode('utf-8')).hexdigest()}"


def _canonical_platform_event(platform: str, event: dict) -> dict:
    ignored = {"deliveryContext", "timestamp", "replyToken", "webhookEventId", "isRedelivery", "redelivery", "batch_id", "batchId"}
    def scrub(value):
        if isinstance(value, dict):
            return {key: scrub(item) for key, item in sorted(value.items()) if key not in ignored}
        if isinstance(value, list):
            return [scrub(item) for item in value]
        return value

    return {"platform": platform, "event": scrub(event)}


def _line_external_keys(event: dict) -> tuple[str | None, str | None, str | None, str | None]:
    source = event.get("source") or {}
    user_id = source.get("userId")
    group_id = source.get("groupId")
    room_id = source.get("roomId")
    destination = event.get("destination")
    container_id = group_id or room_id or user_id
    if not container_id:
        return None, None, destination, None
    if user_id and (group_id or room_id):
        conversation_id = f"{destination or 'unknown-destination'}:{container_id}:{user_id}"
    else:
        conversation_id = f"{destination or 'unknown-destination'}:{container_id}"
    return conversation_id, user_id, destination, container_id


def _facebook_page_id(account: PlatformAccount | None, entry: dict) -> str | None:
    configured = (account.config or {}).get("page_id") if account else None
    return str(configured or entry.get("id") or "") or None


def _facebook_external_keys(entry: dict, event: dict, account: PlatformAccount | None) -> tuple[str | None, str | None, str | None]:
    page_id = _facebook_page_id(account, entry) or "unknown-page"
    if "message" in event:
        sender_id = (event.get("sender") or {}).get("id")
        return (f"{page_id}:messenger:{sender_id}" if sender_id else None), sender_id, None
    value = event.get("value") or {}
    sender_id = (value.get("from") or {}).get("id")
    post_id = value.get("post_id") or value.get("parent_id") or value.get("id")
    comment_id = value.get("comment_id") or value.get("id")
    if not sender_id or not post_id:
        return None, sender_id, comment_id
    return f"{page_id}:post:{post_id}:commenter:{sender_id}", sender_id, comment_id


def _job_id_for_external_post(db: Session, platform: str, account: PlatformAccount | None, external_post_id: str | None) -> int | None:
    if not external_post_id:
        return None
    mapping = db.scalar(
        select(PlatformIdentityMap).where(
            PlatformIdentityMap.platform == platform,
            PlatformIdentityMap.account_id == (account.id if account else None),
            PlatformIdentityMap.entity_type == "post",
            PlatformIdentityMap.external_id == external_post_id,
        )
    )
    if mapping and mapping.job_id:
        return mapping.job_id
    post = db.scalar(select(Post).where(Post.platform == platform, Post.account_id == (account.id if account else None), Post.platform_post_id == external_post_id))
    return post.job_id if post else None


def _find_or_create_inbound_conversation(
    db: Session,
    *,
    platform: str,
    account: PlatformAccount | None,
    external_conversation_id: str,
    external_candidate_id: str | None,
    job_id: int | None = None,
    metadata_json: dict | None = None,
) -> Conversation:
    mapping = db.scalar(
        select(PlatformIdentityMap).where(
            PlatformIdentityMap.platform == platform,
            PlatformIdentityMap.account_id == (account.id if account else None),
            PlatformIdentityMap.entity_type == "conversation",
            PlatformIdentityMap.external_id == external_conversation_id,
        )
    )
    if mapping and mapping.conversation_id:
        conversation = db.get(Conversation, mapping.conversation_id)
        if conversation:
            if job_id and not conversation.job_id:
                conversation.job_id = job_id
            return conversation
    candidate = None
    candidate_mapping = None
    if external_candidate_id:
        candidate_mapping = db.scalar(
            select(PlatformIdentityMap).where(
                PlatformIdentityMap.platform == platform,
                PlatformIdentityMap.account_id == (account.id if account else None),
                PlatformIdentityMap.entity_type == "candidate",
                PlatformIdentityMap.external_id == external_candidate_id,
            )
        )
        if candidate_mapping and candidate_mapping.candidate_id:
            candidate = db.get(Candidate, candidate_mapping.candidate_id)
    if not candidate:
        candidate = Candidate(name=f"{platform} candidate {external_candidate_id or external_conversation_id}", source_platform=platform, platform_id=external_candidate_id)
        db.add(candidate)
        db.flush()
    resolved_job_id = job_id or (candidate_mapping.job_id if candidate_mapping else None)
    if candidate_mapping and candidate_mapping.conversation_id and resolved_job_id:
        linked_conversation = db.get(Conversation, candidate_mapping.conversation_id)
        if linked_conversation and linked_conversation.status == "open" and (linked_conversation.job_id == resolved_job_id or linked_conversation.job_id is None):
            if not linked_conversation.job_id:
                linked_conversation.job_id = resolved_job_id
            db.add(
                PlatformIdentityMap(
                    platform=platform,
                    account_id=account.id if account else None,
                    entity_type="conversation",
                    external_id=external_conversation_id,
                    candidate_id=candidate.id,
                    conversation_id=linked_conversation.id,
                    job_id=linked_conversation.job_id,
                    external_candidate_id=external_candidate_id,
                    external_conversation_id=external_conversation_id,
                    metadata_json={"source": f"{platform}.webhook", "resolved_from_external_candidate": True, **(metadata_json or {})},
                )
            )
            return linked_conversation
    if not resolved_job_id and candidate:
        open_conversations = db.execute(
            select(Conversation).where(
                Conversation.channel == platform,
                Conversation.candidate_id == candidate.id,
                Conversation.status == "open",
                Conversation.job_id.is_not(None),
            )
        ).scalars().all()
        if len(open_conversations) == 1:
            resolved_job_id = open_conversations[0].job_id
            conversation = open_conversations[0]
            db.add(
                PlatformIdentityMap(
                    platform=platform,
                    account_id=account.id if account else None,
                    entity_type="conversation",
                    external_id=external_conversation_id,
                    candidate_id=candidate.id,
                    conversation_id=conversation.id,
                    job_id=conversation.job_id,
                    external_candidate_id=external_candidate_id,
                    external_conversation_id=external_conversation_id,
                    metadata_json={"source": f"{platform}.webhook", "resolved_from_external_candidate": True, **(metadata_json or {})},
                )
            )
            return conversation
        if len(open_conversations) > 1:
            db.add(SystemError(service="Webhook", feature=f"{platform}.identity_mapping", severity="Warning", message="Inbound private conversation matched multiple open jobs.", impact="Message stored in manual review; no question or candidate-specific outbound action will be automated."))
            db.add(
                QueueTask(
                    queue_name="manual-exception",
                    task_type=f"manual.{platform}_identity_ambiguous",
                    payload={"external_conversation_id": external_conversation_id, "external_candidate_id": external_candidate_id, "candidate_id": candidate.id},
                    idempotency_key=f"manual-identity-ambiguous:{platform}:{external_conversation_id}",
                    status="Manual Review Required",
                    max_retries=0,
                )
            )
            conversation = Conversation(channel=platform, candidate_id=candidate.id, job_id=None, ai_mode="suggest", status="manual_review")
            db.add(conversation)
            db.flush()
            return conversation
    conversation = Conversation(channel=platform, candidate_id=candidate.id, job_id=resolved_job_id, ai_mode="suggest", status="open")
    db.add(conversation)
    db.flush()
    if external_candidate_id and not candidate_mapping:
        db.add(
            PlatformIdentityMap(
                platform=platform,
                account_id=account.id if account else None,
                entity_type="candidate",
                external_id=external_candidate_id,
                candidate_id=candidate.id,
                external_candidate_id=external_candidate_id,
                job_id=resolved_job_id,
                conversation_id=conversation.id,
                metadata_json={"source": f"{platform}.webhook", **(metadata_json or {})},
            )
        )
    db.add(
        PlatformIdentityMap(
            platform=platform,
            account_id=account.id if account else None,
            entity_type="conversation",
            external_id=external_conversation_id,
            candidate_id=candidate.id,
            conversation_id=conversation.id,
            job_id=resolved_job_id,
            external_candidate_id=external_candidate_id,
            external_conversation_id=external_conversation_id,
            metadata_json={"source": f"{platform}.webhook", **(metadata_json or {})},
        )
    )
    return conversation


def _store_inbound_platform_message(
    db: Session,
    *,
    platform: str,
    account: PlatformAccount | None,
    external_conversation_id: str,
    external_candidate_id: str | None,
    external_message_id: str,
    body: str,
    raw_event: dict,
    job_id: int | None = None,
    metadata_json: dict | None = None,
) -> Message:
    existing_messages = db.execute(select(Message).where(Message.platform_message_id == external_message_id)).scalars().all()
    for existing in existing_messages:
        if (existing.metadata_json or {}).get("source") == f"{platform}.webhook":
            return existing
    conversation = _find_or_create_inbound_conversation(
        db,
        platform=platform,
        account=account,
        external_conversation_id=external_conversation_id,
        external_candidate_id=external_candidate_id,
        job_id=job_id,
        metadata_json=metadata_json,
    )
    message = Message(
        conversation_id=conversation.id,
        sender_type="candidate",
        body=body,
        platform_message_id=external_message_id,
        delivery_status="Delivered",
        metadata_json={"source": f"{platform}.webhook", "raw_event": raw_event, **(metadata_json or {})},
    )
    db.add(message)
    db.flush()
    enqueue_task(
        db,
        QueueTask(
            queue_name="flow",
            task_type="flow.process_inbound_message",
            payload={"message_id": message.id, "conversation_id": conversation.id, "platform": platform},
            idempotency_key=f"inbound-ai:{platform}:{external_message_id}",
            max_retries=3,
        ),
    )
    return message


def _verify_line_signature(account: PlatformAccount | None, body: bytes, signature: str | None) -> None:
    secret = (account.config or {}).get("channel_secret") if account else None
    if not secret:
        raise HTTPException(status_code=503, detail="LINE channel_secret is not configured")
    expected = base64.b64encode(hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()).decode("ascii")
    if not signature or not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="Invalid LINE webhook signature")


@router.post("/webhooks/line")
async def line_webhook(request: Request, account_id: int | None = None, db: Session = Depends(get_db)):
    body = await request.body()
    account = _webhook_account(db, "line", account_id)
    _verify_line_signature(account, body, request.headers.get("x-line-signature"))
    payload = await request.json()
    created = 0
    for event in payload.get("events") or []:
        event_id = event.get("webhookEventId") or event.get("replyToken") or _stable_event_id("line", event)
        external_conversation_id, external_candidate_id, destination, container_id = _line_external_keys(event)
        source_type = (event.get("source") or {}).get("type")
        message = event.get("message") or {}
        external_message_id = message.get("id") or event_id
        text = message.get("text") if message.get("type") == "text" else None
        recorded = _record_platform_webhook_event(
            db,
            platform="line",
            account=account,
            event_id=event_id,
            body=body,
            canonical_event=_canonical_platform_event("line", event),
            external_conversation_id=external_conversation_id,
            external_message_id=external_message_id,
            evidence={"type": event.get("type"), "message_type": message.get("type"), "destination": destination, "container_id": container_id, "user_id": external_candidate_id},
        )
        if recorded.status == "Duplicate":
            continue
        if event.get("type") == "message" and external_conversation_id and external_candidate_id and text:
            _store_inbound_platform_message(
                db,
                platform="line",
                account=account,
                external_conversation_id=external_conversation_id,
                external_candidate_id=external_candidate_id,
                external_message_id=external_message_id,
                body=text,
                raw_event=event,
                metadata_json={"line_source_type": source_type, "destination": destination, "container_id": container_id, "private_recipient_verified": source_type == "user"},
            )
            created += 1
    db.commit()
    return {"ok": True, "messages": created}


def _verify_facebook_signature(account: PlatformAccount | None, body: bytes, signature: str | None) -> None:
    secret = (account.config or {}).get("app_secret") if account else None
    if not secret:
        raise HTTPException(status_code=503, detail="Facebook app_secret is not configured")
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    supplied = (signature or "").removeprefix("sha256=")
    if not signature or not hmac.compare_digest(expected, supplied):
        raise HTTPException(status_code=401, detail="Invalid Facebook webhook signature")


@router.get("/webhooks/facebook")
async def facebook_webhook_verify(request: Request, account_id: int | None = None, db: Session = Depends(get_db)):
    account = _webhook_account(db, "facebook", account_id)
    verify_token = (account.config or {}).get("verify_token") if account else None
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and verify_token and params.get("hub.verify_token") == verify_token:
        return Response(content=params.get("hub.challenge") or "", media_type="text/plain")
    raise HTTPException(status_code=403, detail="Facebook webhook verification failed")


@router.post("/webhooks/facebook")
async def facebook_webhook(request: Request, account_id: int | None = None, db: Session = Depends(get_db)):
    body = await request.body()
    account = _webhook_account(db, "facebook", account_id)
    _verify_facebook_signature(account, body, request.headers.get("x-hub-signature-256"))
    payload = await request.json()
    created = 0
    for entry in payload.get("entry") or []:
        for event in entry.get("messaging") or []:
            sender = event.get("sender") or {}
            message = event.get("message") or {}
            if message.get("is_echo") or event.get("delivery") or event.get("read") or event.get("postback"):
                continue
            external_message_id = message.get("mid") or _stable_event_id("facebook:messaging", event)
            external_conversation_id, external_candidate_id, _comment_id = _facebook_external_keys(entry, event, account)
            event_id = f"messaging:{external_message_id}"
            recorded = _record_platform_webhook_event(db, platform="facebook", account=account, event_id=event_id, body=body, canonical_event=_canonical_platform_event("facebook", event), external_conversation_id=external_conversation_id, external_message_id=external_message_id, evidence={"type": "messaging", "sender_id": external_candidate_id})
            if recorded.status == "Duplicate":
                continue
            text = message.get("text") if message.get("text") and not message.get("attachments") else None
            if external_conversation_id and external_message_id and text and external_candidate_id:
                inbound_message = _store_inbound_platform_message(
                    db,
                    platform="facebook",
                    account=account,
                    external_conversation_id=external_conversation_id,
                    external_candidate_id=external_candidate_id,
                    external_message_id=external_message_id,
                    body=text,
                    raw_event=event,
                    metadata_json={"facebook_source_type": "messenger", "private_recipient_verified": True},
                )
                created += 1
        for change in entry.get("changes") or []:
            value = change.get("value") or {}
            text = value.get("message") or ""
            external_conversation_id, sender_id, comment_id = _facebook_external_keys(entry, change, account)
            if value.get("item") not in {None, "comment"} or value.get("verb") in {"remove", "hide"}:
                continue
            if sender_id and sender_id == _facebook_page_id(account, entry):
                continue
            if not comment_id:
                continue
            event_id = f"comment:{comment_id}"
            recorded = _record_platform_webhook_event(db, platform="facebook", account=account, event_id=event_id, body=body, canonical_event=_canonical_platform_event("facebook", change), external_conversation_id=external_conversation_id, external_message_id=comment_id, evidence={"type": "comment", "sender_id": sender_id, "post_id": value.get("post_id")})
            if recorded.status == "Duplicate":
                continue
            if text and external_conversation_id and sender_id:
                post_id = value.get("post_id") or value.get("parent_id")
                job_id = _job_id_for_external_post(db, "facebook", account, post_id)
                inbound_message = _store_inbound_platform_message(
                    db,
                    platform="facebook",
                    account=account,
                    external_conversation_id=external_conversation_id,
                    external_candidate_id=sender_id,
                    external_message_id=comment_id,
                    body=text,
                    raw_event=change,
                    job_id=job_id,
                    metadata_json={"facebook_source_type": "comment", "post_id": post_id, "comment_id": comment_id, "private_recipient_verified": False},
                )
                private_reply = Message(
                    conversation_id=inbound_message.conversation_id,
                    sender_type="system",
                    body="您好，為保護您的個人資料，請查看我們傳送給您的私人訊息並在私人對話中回覆。",
                    delivery_status="Pending Send",
                    metadata_json={"flow": "facebook_private_reply_invitation", "comment_id": comment_id, "approved_template": True},
                )
                db.add(private_reply)
                db.flush()
                enqueue_task(
                    db,
                    QueueTask(
                        queue_name="messaging",
                        task_type="message.private_reply",
                        payload={"message_id": private_reply.id, "comment_id": comment_id, "platform": "facebook", "account_id": account.id if account else None},
                        idempotency_key=f"facebook-private-reply:{comment_id}",
                        max_retries=1,
                    ),
                )
                created += 1
    db.commit()
    return {"ok": True, "messages": created}


@router.post("/webhooks/phone")
async def phone_webhook(request: Request, db: Session = Depends(get_db)):
    body = await request.body()
    _verify_phone_webhook_signature(request, body)
    payload = await request.json()
    call_id = payload.get("call_id") or payload.get("id")
    if not call_id:
        raise HTTPException(status_code=400, detail="call_id is required")
    provider = str(payload.get("provider") or request.headers.get("x-phone-provider") or "default")
    event_id = str(payload.get("event_id") or payload.get("callback_id") or request.headers.get("x-phone-event-id") or request.headers.get("x-phone-nonce"))
    nonce = request.headers.get("x-phone-nonce")
    event = PhoneWebhookEvent(
        provider=provider,
        event_id=event_id,
        nonce=nonce,
        external_call_id=str(call_id),
        received_at=now_iso(),
        payload_hash=hashlib.sha256(body).hexdigest(),
    )
    db.add(event)
    try:
        db.flush()
    except IntegrityError as exc:
        db.rollback()
        existing = db.scalar(select(PhoneWebhookEvent).where(PhoneWebhookEvent.provider == provider, PhoneWebhookEvent.event_id == event_id))
        if existing and existing.payload_hash == hashlib.sha256(body).hexdigest():
            return {"ok": True, "duplicate": True}
        raise HTTPException(status_code=409, detail="Phone webhook event collision detected") from exc
    call = db.scalar(select(PhoneCallLog).where(PhoneCallLog.external_call_id == str(call_id)))
    if not call:
        event.status = "Rejected"
        db.commit()
        raise HTTPException(status_code=404, detail="Call not found")

    status_rank = {"Accepted": 1, "Ringing": 2, "Connected": 3, "No Answer": 4, "no_answer": 4, "Rejected": 4, "rejected": 4, "Failed": 4, "failed": 4, "Completed": 5, "completed": 5}
    incoming_status = payload.get("dial_status") or payload.get("status") or call.dial_status
    if status_rank.get(str(incoming_status), 0) >= status_rank.get(str(call.dial_status), 0):
        call.dial_status = incoming_status
    call.call_result = payload.get("call_result") or payload.get("result") or call.call_result
    call.transcript = payload.get("transcript") or call.transcript
    call.recording_url = payload.get("recording_url") or call.recording_url
    from backend.app.services.queue import scrub_evidence

    disclosure_confirmed = disclosure_was_confirmed(payload)
    decision = classify_voice_transcript(call.transcript)
    state = voice_agent_state(provider_accepted=True, disclosure_confirmed=disclosure_confirmed, transcript=call.transcript, decision=decision)
    call.evidence = {
        **(call.evidence or {}),
        "callback": scrub_evidence(payload),
        "voice_agent_decision": decision.__dict__,
        "disclosure_played": bool(disclosure_confirmed or (call.evidence or {}).get("disclosure_played") is True),
        "voice_agent_state": str(state),
    }
    if str(call.dial_status) in SUCCESS_STATUSES and not call.evidence["disclosure_played"]:
        call.dial_status = "Human Review Required"
        call.call_result = "Phone callback did not prove AI disclosure playback/completion; no automated success decision was made."
    if "dnc_request" in decision.risk_flags:
        active_dnc = db.scalar(select(DoNotContact).where(DoNotContact.candidate_id == call.candidate_id, DoNotContact.status == "Active"))
        if not active_dnc:
            db.add(DoNotContact(candidate_id=call.candidate_id, reason="Candidate requested no further contact during phone callback.", evidence=str({"phone_call_id": call.external_call_id, "event_id": event_id})))
        call.dial_status = "Human Review Required"
        call.call_result = "Candidate requested no further contact; DNC recorded and automation stopped."
    if str(call.dial_status) in SUCCESS_STATUSES and (decision.confidence < 0.8 or decision.risk_flags):
        call.dial_status = "Human Review Required"
        call.call_result = "Voice callback was low-confidence or risky; no automated success decision was made."
    call.finished_at = payload.get("finished_at") or now_iso()

    if call.dial_status in {"No Answer", "no_answer", "Rejected", "rejected", "Failed", "failed"} and call.conversation_id:
        active_dnc = db.scalar(select(DoNotContact).where(DoNotContact.candidate_id == call.candidate_id, DoNotContact.status == "Active"))
        if not active_dnc:
            conversation = db.get(Conversation, call.conversation_id)
            fallback_platform = payload.get("fallback_platform") or (conversation.channel if conversation else "line")
            mapping = None
            if conversation:
                mapping = db.scalar(
                    select(PlatformIdentityMap).where(
                        PlatformIdentityMap.platform == fallback_platform,
                        PlatformIdentityMap.conversation_id == conversation.id,
                        PlatformIdentityMap.entity_type == "conversation",
                    )
                )
            fallback_account_id = payload.get("fallback_account_id") or (mapping.account_id if mapping else None)
            message = Message(
                conversation_id=call.conversation_id,
                sender_type="system",
                body=payload.get("fallback_body") or "We could not reach you by phone. Please reply here with a convenient time or whether you want to continue the interview process.",
                delivery_status="Pending Send",
                metadata_json={"flow": "phone_callback_fallback", "phone_call_id": call.external_call_id, "phone_webhook_event_id": event_id},
            )
            db.add(message)
            db.flush()
            task = enqueue_task(
                db,
                QueueTask(
                    queue_name="messaging",
                    task_type="message.send",
                    payload={"message_id": message.id, "conversation_id": call.conversation_id, "platform": fallback_platform, "account_id": fallback_account_id},
                    idempotency_key=f"phone-callback-fallback:{provider}:{event_id}",
                    max_retries=5,
                ),
            )
            event.fallback_task_id = task.id
    audit(db, None, "phone.callback", "PhoneCallLog", str(call.id), {"dial_status": call.dial_status, "call_result": call.call_result})
    db.commit()
    return {"ok": True}

@router.post("/queue/tasks", response_model=QueueTaskOut)
def create_queue_task(payload: QueueTaskCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    task = enqueue_task(db, QueueTask(**payload.model_dump()))
    audit(db, user, "queue.create", "QueueTask", str(task.id), {"task_type": task.task_type})
    db.commit()
    db.refresh(task)
    return task


@router.post("/queue/tasks/{task_id}/run", response_model=QueueTaskOut)
async def run_queue_task(task_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    task = db.get(QueueTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    claimed = claim_task(db, task, worker_id=f"manual:{user.id}")
    if not claimed:
        raise HTTPException(status_code=409, detail=f"Task is {task.status}")
    audit(db, user, "queue.run", "QueueTask", str(claimed.id), {"task_type": claimed.task_type, "fencing_token": claimed.fencing_token})
    db.commit()
    return await process_task(db, claimed)


@router.post("/queue/tasks/{task_id}/replay", response_model=QueueTaskOut)
def replay_queue_task(task_id: int, payload: QueueReplayRequest, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    try:
        task = replay_task(
            db,
            task_id,
            expected_delivery_revision=payload.expected_delivery_revision,
            replay_request_key=payload.replay_request_key,
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 409
        raise HTTPException(status_code=status_code, detail=detail) from exc
    audit(
        db,
        user,
        "queue.replay",
        "QueueTask",
        str(task.id),
        {"task_type": task.task_type, "delivery_revision": task.delivery_revision, "replay_request_key": payload.replay_request_key},
    )
    db.commit()
    db.refresh(task)
    return task


@router.get("/conversations", response_model=list[ConversationOut])
def list_conversations(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(Conversation).order_by(Conversation.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/conversations", response_model=ConversationOut)
def create_conversation(payload: ConversationCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    conversation = Conversation(**payload.model_dump())
    db.add(conversation)
    db.flush()
    audit(db, user, "conversation.create", "Conversation", str(conversation.id), {"channel": conversation.channel})
    db.commit()
    db.refresh(conversation)
    return conversation


@router.get("/messages", response_model=list[MessageOut])
def list_messages(conversation_id: int, limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(Message).where(Message.conversation_id == conversation_id).order_by(Message.created_at).offset(offset).limit(limit)).scalars().all()


@router.post("/messages", response_model=MessageOut)
def create_message(payload: MessageCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    conversation = db.get(Conversation, payload.conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if payload.sender_type in {"hr", "system", "ai"} and conversation.candidate_id:
        dnc = db.scalar(select(DoNotContact).where(DoNotContact.candidate_id == conversation.candidate_id, DoNotContact.status == "Active"))
        if dnc:
            raise HTTPException(status_code=409, detail="Candidate is on the do-not-contact list.")
    data = payload.model_dump(exclude={"send_external"})
    delivery_status = "Pending Send" if payload.send_external and payload.sender_type in {"hr", "system", "ai"} else "Internal Record"
    message = Message(**data, delivery_status=delivery_status)
    db.add(message)
    db.flush()
    if payload.sender_type == "candidate":
        process_inbound_candidate_message(db, message)
    queue_task_id = None
    if payload.send_external and payload.sender_type in {"hr", "system", "ai"}:
        task = enqueue_task(
            db,
            QueueTask(
                queue_name="messaging",
                task_type="message.send",
                payload={"message_id": message.id, "conversation_id": conversation.id, "platform": conversation.channel},
                idempotency_key=f"message:{conversation.channel}:{conversation.id}:{message.id}",
            ),
        )
        queue_task_id = task.id
    audit(db, user, "message.create", "Message", str(message.id), {"conversation_id": message.conversation_id, "delivery_status": message.delivery_status, "queue_task_id": queue_task_id})
    db.commit()
    db.refresh(message)
    return message


@router.get("/question-answers", response_model=list[CandidateQuestionAnswerOut])
def list_question_answers(application_id: int | None = None, limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    query = select(CandidateQuestionAnswer).order_by(CandidateQuestionAnswer.created_at.desc())
    if application_id:
        query = query.where(CandidateQuestionAnswer.application_id == application_id)
    return db.execute(query.offset(offset).limit(limit)).scalars().all()


@router.post("/applications/{application_id}/ask-next-question", response_model=QueueTaskOut)
def ask_next_question(application_id: int, conversation_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    application = db.get(Application, application_id)
    conversation = db.get(Conversation, conversation_id)
    if not application or not conversation:
        raise HTTPException(status_code=404, detail="Application or conversation not found")
    if conversation.candidate_id != application.candidate_id or conversation.job_id != application.job_id:
        raise HTTPException(status_code=409, detail="Conversation does not match application candidate/job.")
    task = enqueue_next_question(db, application, conversation)
    if not task:
        raise HTTPException(status_code=409, detail="No unanswered required question remains.")
    audit(db, user, "question.ask_next", "Application", str(application.id), {"conversation_id": conversation.id, "queue_task_id": task.id})
    db.commit()
    db.refresh(task)
    return task


@router.get("/matches", response_model=list[MatchOut])
def list_matches(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(MatchRecord).order_by(MatchRecord.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/matches/suggest", response_model=MatchOut)
def suggest_match(payload: MatchRequest, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    candidate = db.get(Candidate, payload.candidate_id)
    job = db.get(Job, payload.job_id)
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    match = build_match(db, candidate, job)
    db.flush()
    audit(db, user, "match.suggest", "MatchRecord", str(match.id), {"candidate_id": candidate.id, "job_id": job.id, "score": match.score})
    db.commit()
    db.refresh(match)
    return match


@router.get("/interviews", response_model=list[InterviewOut])
def list_interviews(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(Interview).order_by(Interview.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/interviews", response_model=InterviewOut)
def create_interview(payload: InterviewCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    if not db.get(Candidate, payload.candidate_id):
        raise HTTPException(status_code=404, detail="Candidate not found")
    if not db.get(Job, payload.job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    interview = Interview(**payload.model_dump(), hr_user_id=user.id)
    db.add(interview)
    db.flush()
    schedule_interview_tasks(db, interview)
    audit(db, user, "interview.schedule", "Interview", str(interview.id), {"candidate_id": interview.candidate_id, "job_id": interview.job_id})
    db.commit()
    db.refresh(interview)
    return interview


@router.post("/interviews/{interview_id}/attendance", response_model=InterviewOut)
def record_interview_attendance(
    interview_id: int,
    payload: dict = Body(default_factory=dict),
    db: Session = Depends(get_db),
    user: User = Depends(require_role("Recruiter / HR")),
):
    interview = db.get(Interview, interview_id)
    if not interview:
        raise HTTPException(status_code=404, detail="Interview not found")
    status = str(payload.get("attendance_status") or payload.get("status") or "").strip()
    if status not in {"Arrived", "No Show"}:
        raise HTTPException(status_code=422, detail="attendance_status must be Arrived or No Show")
    interview.attendance_status = status
    interview.attended_at = now_iso() if status == "Arrived" else None
    interview.attendance_evidence = {"recorded_by_user_id": user.id, "recorded_at": now_iso(), "source": payload.get("source") or "hr_verified", "note": payload.get("note")}
    if status == "Arrived":
        from backend.app.services.flow import cancel_pending_interview_tasks

        cancel_pending_interview_tasks(db, interview.id, "Cancelled because HR verified candidate arrival.", {"flow.interview_no_show_check"})
    audit(db, user, "interview.attendance", "Interview", str(interview.id), {"attendance_status": status})
    db.commit()
    db.refresh(interview)
    return interview


@router.post("/calendar/book-interview", response_model=QueueTaskOut)
def book_interview(payload: CalendarBookRequest, db: Session = Depends(get_db), user: User = Depends(require_role("Recruiter / HR"))):
    interview = db.get(Interview, payload.interview_id)
    if not interview:
        raise HTTPException(status_code=404, detail="Interview not found")
    forbidden = {"access_token", "oauth_token", "refresh_token", "client_secret", "token", "secret"}
    if forbidden.intersection(set((payload.calendar_config or {}).keys())):
        raise HTTPException(status_code=400, detail="Calendar credentials must be stored on PlatformAccount config, not QueueTask payload.")
    account_id = payload.calendar_config.get("account_id") or payload.calendar_config.get("calendar_account_id")
    if account_id:
        account = db.get(PlatformAccount, account_id)
        if not account or account.platform != "calendar":
            raise HTTPException(status_code=404, detail="Calendar PlatformAccount not found")
    key = f"calendar-book-request:{interview.id}:{hashlib.sha256(str(payload.calendar_config).encode('utf-8')).hexdigest()}"
    task = enqueue_task(
        db,
        QueueTask(
            queue_name="calendar",
            task_type="calendar.book_interview",
            payload={"interview_id": interview.id, "calendar_config": payload.calendar_config},
            idempotency_key=key,
            max_retries=3,
        ),
    )
    audit(db, user, "calendar.book_interview.request", "Interview", str(interview.id), {"queue_task_id": task.id})
    db.commit()
    db.refresh(task)
    return task


@router.get("/analytics/summary", response_model=AnalyticsSummary)
def analytics(_user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    return analytics_summary(db)


@router.get("/do-not-contact", response_model=list[DoNotContactOut])
def list_do_not_contact(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(DoNotContact).order_by(DoNotContact.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/do-not-contact", response_model=DoNotContactOut)
def create_do_not_contact(payload: DoNotContactCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    if not db.get(Candidate, payload.candidate_id):
        raise HTTPException(status_code=404, detail="Candidate not found")
    entry = DoNotContact(**payload.model_dump(), created_by=user.id)
    db.add(entry)
    db.flush()
    audit(db, user, "do_not_contact.create", "DoNotContact", str(entry.id), {"candidate_id": entry.candidate_id})
    db.commit()
    db.refresh(entry)
    return entry


@router.post("/do-not-contact/{entry_id}/lift", response_model=DoNotContactOut)
def lift_do_not_contact(entry_id: int, payload: DoNotContactLift, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    entry = db.get(DoNotContact, entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Do-not-contact entry not found")
    if not entry.can_lift:
        raise HTTPException(status_code=409, detail="This entry cannot be lifted")
    entry.status = "Lifted"
    audit(db, user, "do_not_contact.lift", "DoNotContact", str(entry.id), {"reason": payload.reason})
    db.commit()
    db.refresh(entry)
    return entry


@router.get("/recruitment-blacklist", response_model=list[RecruitmentBlacklistOut])
def list_recruitment_blacklist(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Recruiter / HR")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(RecruitmentBlacklist).order_by(RecruitmentBlacklist.created_at.desc()).offset(offset).limit(limit)).scalars().all()


@router.post("/recruitment-blacklist", response_model=RecruitmentBlacklistOut)
def create_recruitment_blacklist(payload: RecruitmentBlacklistCreate, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    if payload.candidate_id and not db.get(Candidate, payload.candidate_id):
        raise HTTPException(status_code=404, detail="Candidate not found")
    if not payload.candidate_id and not payload.phone and not payload.email:
        raise HTTPException(status_code=400, detail="candidate_id, phone, or email is required")
    if payload.scope_type == "job":
        if not payload.scope_id:
            raise HTTPException(status_code=400, detail="scope_id is required when scope_type=job")
        try:
            job_id = int(payload.scope_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="scope_id must be a numeric job id when scope_type=job") from exc
        if not db.get(Job, job_id):
            raise HTTPException(status_code=404, detail="Scoped job not found")
        payload.scope_id = str(job_id)
    elif payload.scope_type in {"candidate", "global"} and payload.scope_id:
        raise HTTPException(status_code=400, detail="scope_id is only accepted when scope_type=job or legacy")
    entry = RecruitmentBlacklist(**payload.model_dump(), created_by=user.id)
    db.add(entry)
    db.flush()
    audit(db, user, "recruitment_blacklist.create", "RecruitmentBlacklist", str(entry.id), {"candidate_id": entry.candidate_id, "rule_code": entry.rule_code})
    db.commit()
    db.refresh(entry)
    return entry


def _validate_blacklist_scope(db: Session, *, scope_type: str, scope_id: str | None) -> str | None:
    if scope_type == "job":
        if not scope_id:
            raise HTTPException(status_code=400, detail="scope_id is required when scope_type=job")
        try:
            job_id = int(scope_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="scope_id must be a numeric job id when scope_type=job") from exc
        if not db.get(Job, job_id):
            raise HTTPException(status_code=404, detail="Scoped job not found")
        return str(job_id)
    if scope_type in {"candidate", "global"} and scope_id:
        raise HTTPException(status_code=400, detail="scope_id is only accepted when scope_type=job or legacy")
    return scope_id


@router.patch("/recruitment-blacklist/{entry_id}", response_model=RecruitmentBlacklistOut)
def update_recruitment_blacklist(entry_id: int, payload: dict = Body(default_factory=dict), db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    entry = db.get(RecruitmentBlacklist, entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Recruitment blacklist entry not found")
    for field in ["candidate_id", "phone", "email", "reason", "rule_code", "evidence"]:
        if field in payload:
            setattr(entry, field, payload[field])
    if "scope_type" in payload or "scope_id" in payload:
        scope_type = RecruitmentBlacklistCreate.model_validate({**serialize_model(entry), **payload, "reason": payload.get("reason") or entry.reason}).scope_type
        scope_id = RecruitmentBlacklistCreate.model_validate({**serialize_model(entry), **payload, "reason": payload.get("reason") or entry.reason}).scope_id
        entry.scope_type = scope_type
        entry.scope_id = _validate_blacklist_scope(db, scope_type=scope_type, scope_id=scope_id)
    audit(db, user, "recruitment_blacklist.update", "RecruitmentBlacklist", str(entry.id), {"fields": sorted(payload.keys())})
    db.commit()
    db.refresh(entry)
    return entry


@router.post("/recruitment-blacklist/{entry_id}/confirm-scope", response_model=RecruitmentBlacklistOut)
def confirm_recruitment_blacklist_scope(entry_id: int, payload: dict = Body(default_factory=dict), db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    entry = db.get(RecruitmentBlacklist, entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Recruitment blacklist entry not found")
    scope_type = RecruitmentBlacklistCreate.model_validate({**serialize_model(entry), **payload, "reason": payload.get("reason") or entry.reason}).scope_type
    scope_id = RecruitmentBlacklistCreate.model_validate({**serialize_model(entry), **payload, "reason": payload.get("reason") or entry.reason}).scope_id
    entry.scope_type = scope_type
    entry.scope_id = _validate_blacklist_scope(db, scope_type=scope_type, scope_id=scope_id)
    entry.status = "Active"
    entry.evidence = f"{entry.evidence or ''}\nScope confirmed by user {user.id}: {payload.get('note') or payload.get('reason') or ''}".strip()
    audit(db, user, "recruitment_blacklist.confirm_scope", "RecruitmentBlacklist", str(entry.id), {"scope_type": entry.scope_type, "scope_id": entry.scope_id})
    db.commit()
    db.refresh(entry)
    return entry


@router.post("/recruitment-blacklist/{entry_id}/activate", response_model=RecruitmentBlacklistOut)
def activate_recruitment_blacklist(entry_id: int, db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    entry = db.get(RecruitmentBlacklist, entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Recruitment blacklist entry not found")
    entry.scope_id = _validate_blacklist_scope(db, scope_type=entry.scope_type, scope_id=entry.scope_id)
    entry.status = "Active"
    audit(db, user, "recruitment_blacklist.activate", "RecruitmentBlacklist", str(entry.id), {})
    db.commit()
    db.refresh(entry)
    return entry


@router.post("/recruitment-blacklist/{entry_id}/lift", response_model=RecruitmentBlacklistOut)
def lift_recruitment_blacklist(entry_id: int, payload: dict = Body(default_factory=dict), db: Session = Depends(get_db), user: User = Depends(require_role("Manager"))):
    entry = db.get(RecruitmentBlacklist, entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Recruitment blacklist entry not found")
    entry.status = "Lifted"
    entry.evidence = f"{entry.evidence or ''}\nLifted by user {user.id}: {payload.get('reason') or ''}".strip()
    audit(db, user, "recruitment_blacklist.lift", "RecruitmentBlacklist", str(entry.id), {"reason": payload.get("reason")})
    db.commit()
    db.refresh(entry)
    return entry


@router.get("/audit-logs", response_model=list[AuditLogOut])
def list_audit_logs(limit: int = 100, offset: int = 0, _user: User = Depends(require_role("Manager")), db: Session = Depends(get_db)):
    limit, offset = bounded_page(limit, offset)
    return db.execute(select(AuditLog).order_by(AuditLog.created_at.desc()).offset(offset).limit(limit)).scalars().all()
    DoNotContactCreate,
    DoNotContactLift,
    DoNotContactOut,

