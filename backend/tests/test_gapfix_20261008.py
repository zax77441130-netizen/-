import base64
import hashlib
import hmac
import json
import tempfile
import uuid

import os

db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
db_file.close()
os.environ["DATABASE_URL"] = f"sqlite:///{db_file.name}"
os.environ["DEMO_MODE"] = "true"
os.environ["DEMO_ADMIN_PASSWORD"] = "admin1234"
os.environ["SECRET_KEY"] = "test-secret-key-that-is-not-default"
os.environ["AI_PRICE_JSON"] = '{"Local Template:rules-plus-template":{"input_per_million":1.0,"output_per_million":2.0}}'

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from backend.app.core.db import SessionLocal
from backend.app.main import app
from backend.app.models.domain import (
    Application,
    BusinessEvent,
    Candidate,
    Conversation,
    Job,
    Message,
    PlatformWebhookEvent,
    QueueTask,
    RecruitmentBlacklist,
    SystemError,
    Base,
    Company,
    User,
)
from backend.app.api.routes import create_job
from backend.app.schemas.dto import JobCreate
from backend.app.services.security import hash_password
from backend.app.services.flow import set_application_status
from backend.app.services.redis_queue import cleanup_stale_dlq_indexes, configured_streams


client = TestClient(app)
csrf_headers = {}


def auth_headers() -> dict[str, str]:
    global csrf_headers
    if csrf_headers.get("x-csrf-token"):
        return csrf_headers
    response = client.post("/api/auth/login", json={"email": "admin@example.com", "password": "admin1234"})
    assert response.status_code == 200
    csrf_headers = {"x-csrf-token": client.cookies.get("recruit_csrf")}
    return csrf_headers


def test_line_webhook_canonical_replay_ignores_batch_wrappers():
    headers = auth_headers()
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE canonical", "status": "Configured", "config": {"channel_secret": "line-canonical-secret"}, "capabilities": ["webhook"]},
        headers=headers,
    ).json()
    event = {
        "type": "message",
        "webhookEventId": "line-canonical-event-1",
        "timestamp": 1,
        "source": {"type": "user", "userId": "line-canonical-user"},
        "message": {"id": "line-canonical-message-1", "type": "text", "text": "hello"},
    }
    first_body = json.dumps({"events": [event], "batch_id": "a"}, separators=(",", ":")).encode("utf-8")
    second_event = {**event, "timestamp": 2, "isRedelivery": True}
    second_body = json.dumps({"events": [second_event], "batch_id": "b"}, separators=(",", ":")).encode("utf-8")
    for body in [first_body, second_body]:
        signature = base64.b64encode(hmac.new(b"line-canonical-secret", body, hashlib.sha256).digest()).decode("ascii")
        response = client.post(f"/api/webhooks/line?account_id={account['id']}", content=body, headers={"content-type": "application/json", "x-line-signature": signature})
        assert response.status_code == 200
    with SessionLocal() as db:
        events = db.execute(select(PlatformWebhookEvent).where(PlatformWebhookEvent.event_id == "line-canonical-event-1")).scalars().all()
        messages = db.execute(select(Message).where(Message.platform_message_id == "line-canonical-message-1")).scalars().all()
        assert len(events) == 1
        assert events[0].status == "Duplicate"
        assert events[0].raw_body_b64
        assert events[0].canonical_event_hash
        assert len(messages) == 1


def test_pending_scope_confirmation_pauses_automation_until_confirmed():
    headers = auth_headers()
    candidate = client.post("/api/candidates", json={"name": "Pending Scope Candidate", "phone": "0911555000", "source_platform": "manual"}, headers=headers).json()
    job = client.get("/api/jobs", headers=headers).json()[0]
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    with SessionLocal() as db:
        db.add(RecruitmentBlacklist(candidate_id=candidate["id"], reason="needs scope confirmation", rule_code="legacy", scope_type="legacy", scope_id=None, status="Pending Scope Confirmation"))
        db.commit()
    message = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "system", "body": "hello", "send_external": True}, headers=headers).json()
    task = next(task for task in client.get("/api/queue/tasks", headers=headers).json() if task["payload"].get("message_id") == message["id"])
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Manual Review Required"
    assert "Pending Scope Confirmation" in ran.json()["last_error"]
    with SessionLocal() as db:
        assert db.get(Conversation, conversation["id"]).status == "manual_review"
        manual = db.scalar(select(QueueTask).where(QueueTask.task_type == "manual.blacklist_scope_confirmation"))
        assert manual is not None
    entry = client.get("/api/recruitment-blacklist", headers=headers).json()[0]
    confirmed = client.post(f"/api/recruitment-blacklist/{entry['id']}/confirm-scope", json={"scope_type": "job", "scope_id": f"0{job['id']}", "note": "confirmed"}, headers=headers)
    assert confirmed.status_code == 200
    assert confirmed.json()["status"] == "Active"
    assert confirmed.json()["scope_id"] == str(job["id"])
    lifted = client.post(f"/api/recruitment-blacklist/{entry['id']}/lift", json={"reason": "test release"}, headers=headers)
    assert lifted.status_code == 200
    assert lifted.json()["status"] == "Lifted"


def test_business_event_log_blocks_stale_non_manual_status_regression():
    with SessionLocal() as db:
        candidate = Candidate(name="Business Event Candidate", source_platform="manual")
        job = Job(
            title="Business Event Job",
            location="Taipei",
            area="Taipei",
            job_type="Full-time",
            shift="Day",
            hours="9-5",
            salary_text="market",
            description="job",
            requirements="req",
            contact_name="HR",
            contact_method="email",
            company_id=1,
        )
        db.add_all([candidate, job])
        db.flush()
        app_row = Application(candidate_id=candidate.id, job_id=job.id, status="Interview")
        db.add(app_row)
        db.flush()
        set_application_status(db, app_row, to_status="Contacting", source="webhook", reason="old event", event_key="event-regression-a")
        db.commit()
        assert app_row.status == "Interview"
        event = db.scalar(select(BusinessEvent).where(BusinessEvent.event_key == "event-regression-a"))
        assert event is not None
        assert event.status == "Ignored Stale Transition"


def test_redis_stream_warning_is_throttled_and_cleanup_is_bounded():
    prefix = f"gapfix-stream-{uuid.uuid4().hex}"
    with SessionLocal() as db:
        for idx in range(505):
            db.add(
                QueueTask(
                    queue_name=f"{prefix}-{idx}",
                    task_type="message.verify",
                    payload={"message_id": 999999, "platform": "line"},
                    idempotency_key=f"{prefix}-{idx}",
                    status="Queued",
                )
            )
        db.commit()
        first = configured_streams(db)
        second = configured_streams(db)
        assert f"recruitment:{prefix}-504" in first
        assert first == second
        warnings = db.execute(select(SystemError).where(SystemError.service == "RedisQueue", SystemError.feature == "configured_streams")).scalars().all()
        assert len(warnings) == 1
        assert warnings[0].retry_count >= 1

    class FakeRedis:
        def __init__(self):
            self.data = {f"recruitment:dlq:index:{idx}": "__pending__" for idx in range(5)}
            self.deleted = []

        def scan_iter(self, match=None, count=None):
            return iter(list(self.data.keys()))

        def get(self, key):
            return self.data.get(key)

        def delete(self, key):
            self.deleted.append(key)
            self.data.pop(key, None)

    fake = FakeRedis()
    assert cleanup_stale_dlq_indexes(fake, limit=3) == 3
    assert len(fake.deleted) == 3


def test_create_job_without_seeded_company_persists(tmp_path):
    database_path = tmp_path / "empty-company.db"
    engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(bind=engine)
    with TestSession() as db:
        user = User(
            email="isolated-admin@example.com",
            name="Isolated Admin",
            role="Super Admin",
            password_hash=hash_password("admin1234"),
            active=True,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        job = create_job(
            JobCreate(
                title="Persistence Test Job",
                location="Taipei",
                area="Taipei",
                shift="Day",
                hours="09:00-18:00",
                salary_text="NTD 40,000",
                description="Regression test",
                requirements="Reliable",
                contact_name="HR",
                contact_method="email",
            ),
            db,
            user,
        )
        job_id = job.id

    with TestSession() as db:
        persisted = db.get(Job, job_id)
        assert persisted is not None
        assert persisted.title == "Persistence Test Job"
        assert db.get(Company, persisted.company_id).name == "預設公司"
