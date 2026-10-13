import asyncio
import os
import json
import shutil
import subprocess
import tempfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
db_file.close()
os.environ["DATABASE_URL"] = f"sqlite:///{db_file.name}"
os.environ["DEMO_MODE"] = "true"
os.environ["DEMO_ADMIN_PASSWORD"] = "admin1234"
os.environ["SECRET_KEY"] = "test-secret-key-that-is-not-default"
os.environ["AI_PRICE_JSON"] = '{"Local Template:rules-plus-template":{"input_per_million":1.0,"output_per_million":2.0}}'

from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.orm import sessionmaker

from backend.app.core.config import get_settings
from backend.app.core.db import SessionLocal
from backend.app.main import app
from backend.app.models.domain import AIProvider, AIRequest, Application, BusinessFlowState, CalendarBooking, Candidate, CandidateQuestionAnswer, Conversation, DataRetentionRun, DoNotContact, ExternalActionLog, ExternalRetentionPurge, InboundEmail, Interview, Message, PhoneCallLog, PlatformAccount, PlatformIdentityMap, PlatformRateLimitState, PlatformWebhookEvent, Post, QueueDlqRecord, QueueOutbox, QueueRecoveryCursor, QueueReplayRecord, QueueTask, RecruitmentBlacklist, Resume, StorageObject, SystemError, StoredDocument, User
from backend.app.services.queue import claim_task, has_active_recruitment_blacklist, process_task, reserve_next_task
from backend.app.services import redis_queue as redis_queue_service
from backend.app.services.redis_queue import _heartbeat_task, _renew_once, configured_streams, consume_redis_once, reconcile_dlq_records, publish_pending_outbox, reclaim_pending_messages, repair_lost_published_outbox, replay_dlq_record, replay_task
from backend.app.services.email_worker import acknowledge_imap_messages, poll_imap
from backend.app.services.voice import classify_voice_transcript, disclosure_was_confirmed
from backend.app.services.ai import _validate_json_schema, call_provider_sync, test_provider as run_provider_test
from backend.app.workers.scheduler import ensure_periodic_email_poll_tasks, ensure_periodic_retention_cleanup_tasks
from backend.app.services.security import decrypt_secret, encrypt_secret, hash_password
from backend.app.services.calendar import _read_back_matches
from backend.app.services.flow import schedule_interview_tasks
from backend.app.services.queue_utils import utc_now


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


def test_health_and_csrf_cookie():
    response = client.get("/api/health")
    assert response.status_code == 200
    headers = auth_headers()
    assert headers["x-csrf-token"]
    blocked = client.post("/api/posts", json={"job_id": 1, "platform": "facebook", "content": "blocked"})
    assert blocked.status_code == 403


def test_openapi_schema_has_localized_human_text():
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert "charset" in response.headers["content-type"]
    schema = response.json()
    assert schema["info"]["title"] == "招募營運 API"
    assert schema["paths"]["/api/health"]["get"]["summary"] == "系統健康狀態"
    assert schema["paths"]["/api/health"]["get"]["responses"]["200"]["description"] == "成功回應"


def test_browser_direct_json_pages_are_readable_html():
    health = client.get("/api/health", headers={"accept": "text/html"})
    assert health.status_code == 200
    assert "text/html" in health.headers["content-type"]
    assert '<meta charset="utf-8"' in health.text
    assert "系統健康狀態" in health.text
    assert '"狀態"' in health.text
    openapi = client.get("/openapi.json", headers={"accept": "text/html"})
    assert openapi.status_code == 200
    assert "text/html" in openapi.headers["content-type"]
    assert '<meta charset="utf-8"' in openapi.text
    assert "OpenAPI 規格" in openapi.text
    assert "招募營運 API" in openapi.text


def test_health_reports_worker_runtime_settings_from_pydantic(monkeypatch):
    monkeypatch.setenv("QUEUE_BACKEND", "redis")
    monkeypatch.setenv("WORKER_QUEUES", "email,ocr")
    monkeypatch.setenv("REDIS_CONSUMER_GROUP", "env-group")
    monkeypatch.setenv("REDIS_PENDING_IDLE_MS", "12345")
    monkeypatch.setenv("OCR_ENGINE", "tesseract")
    monkeypatch.setenv("OCR_LANGUAGES", "chi_tra")
    get_settings.cache_clear()
    try:
        response = client.get("/api/health")
        assert response.status_code == 200
        config = response.json()["工作器設定"]
        assert config["佇列後端"] == "Redis"
        assert config["工作佇列"] == ["email", "ocr"]
        assert config["Redis 消費群組"] == "env-group"
        assert config["Redis 待處理閒置毫秒"] == 12345
        assert config["OCR 引擎"] == "tesseract"
        assert config["OCR 語言"] == "chi_tra"
    finally:
        get_settings.cache_clear()


def test_csrf_must_match_persistent_session_token():
    headers = auth_headers()
    original = client.cookies.get("recruit_csrf")
    client.cookies.set("recruit_csrf", "forged-csrf")
    forged = client.post("/api/posts", json={"job_id": 1, "platform": "facebook", "content": "forged"}, headers={"x-csrf-token": "forged-csrf"})
    assert forged.status_code == 403
    client.cookies.set("recruit_csrf", original)
    assert client.get("/api/jobs", headers=headers).status_code == 200


def test_jobs_seeded_copywriting_usage_and_cost():
    headers = auth_headers()
    jobs = client.get("/api/jobs").json()
    assert len(jobs) >= 4
    copy = client.post("/api/ai/copywriting", json={"job_id": jobs[0]["id"]}, headers=headers)
    assert copy.status_code == 200
    assert "內建模板" in copy.json()["ai_notice"]
    usage = client.get("/api/usage").json()
    assert usage["today_tokens"] > 0
    assert usage["month_tokens"] >= usage["today_tokens"]
    assert usage["estimated_cost"] > 0
    assert usage["usage_source"]["Estimated"] > 0


def test_queue_scheduling_retry_and_manual_review():
    headers = auth_headers()
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
    due = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
    future_task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "posting", "task_type": "platform.create_post", "payload": {"post_id": 999999}, "scheduled_at": future},
        headers=headers,
    )
    assert future_task.status_code == 200
    with SessionLocal() as db:
        assert reserve_next_task(db) is None

    due_task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "posting", "task_type": "platform.create_post", "payload": {"post_id": 999999}, "scheduled_at": due, "max_retries": 1},
        headers=headers,
    )
    assert due_task.status_code == 200
    ran = client.post(f"/api/queue/tasks/{due_task.json()['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Manual Review Required"


def test_ai_router_template_fallback_not_success():
    headers = auth_headers()
    route = client.post(
        "/api/ai/routes",
        json={"task_name": "MessageClassification", "primary_provider_id": None, "fallback_provider_ids": [], "budget_policy": {"monthly_cost_limit": 100}},
        headers=headers,
    )
    assert route.status_code == 200
    run = client.post("/api/ai/run", json={"task_name": "MessageClassification", "prompt": "薪資多少?"}, headers=headers)
    assert run.status_code == 200
    assert run.json()["status"] == "template_fallback"
    assert "salary_question" in run.json()["output"]
    errors = client.get("/api/system/errors")
    assert any(error["service"] == "AI Router" for error in errors.json())


def test_ai_json_schema_rejects_invalid_root_types_numbers_and_enums():
    schema = {
        "type": "object",
        "required": ["decision", "confidence", "risk_flags"],
        "properties": {
            "decision": {"type": "string", "enum": ["confirm", "decline", "uncertain"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "risk_flags": {"type": "array", "items": {"type": "string"}},
            "needs_followup": {"type": "boolean"},
        },
    }
    invalid_outputs = [
        "[]",
        json.dumps({"decision": "confirm", "confidence": "0.9", "risk_flags": []}),
        json.dumps({"decision": "confirm", "confidence": 2, "risk_flags": []}),
        json.dumps({"decision": "book_now", "confidence": 0.8, "risk_flags": []}),
        json.dumps({"decision": "confirm", "confidence": 0.8, "risk_flags": [1]}),
        json.dumps({"decision": "confirm", "confidence": 0.8, "risk_flags": [], "needs_followup": "false"}),
    ]
    for output in invalid_outputs:
        try:
            _validate_json_schema(output, schema)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Invalid AI output passed schema validation: {output}")
    _validate_json_schema(json.dumps({"decision": "confirm", "confidence": 0.8, "risk_flags": [], "needs_followup": False}), schema)


def test_api_key_encryption_has_integrity():
    encrypted = encrypt_secret("secret-token")
    assert encrypted != "secret-token"
    assert decrypt_secret(encrypted) == "secret-token"
    tampered = encrypted[:-2] + "aa"
    try:
        decrypt_secret(tampered)
    except ValueError:
        pass
    else:
        raise AssertionError("tampered secret should fail integrity validation")


def test_application_match_interview_analytics_and_dnc():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[0]
    job = client.get("/api/jobs").json()[0]
    application = client.post("/api/applications", json={"candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers)
    assert application.status_code == 200
    match = client.post("/api/matches/suggest", json={"candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers)
    assert match.status_code == 200
    assert "advisory" in match.json()["recruiter_notes"]
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2026-09-26", "interview_time": "10:00"},
        headers=headers,
    )
    assert interview.status_code == 200
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers)
    assert conversation.status_code == 200
    dnc = client.post("/api/do-not-contact", json={"candidate_id": candidate["id"], "reason": "Requested no contact"}, headers=headers)
    assert dnc.status_code == 200
    blocked = client.post(
        "/api/messages",
        json={"conversation_id": conversation.json()["id"], "sender_type": "hr", "body": "follow up"},
        headers=headers,
    )
    assert blocked.status_code == 409
    analytics = client.get("/api/analytics/summary")
    assert analytics.json()["applications"] >= 1
    assert analytics.json()["interviews"] >= 1


def test_resume_dedup_bulk_import_and_privacy_delete():
    headers = auth_headers()
    resume_text = """
姓名: 王小明
電話: 0988123456
Email: resume-test@example.com
居住: 桃園中壢
希望日班，可做包裝、檢驗，有普通重型機車
最快到職: 2026-10-01
"""
    parsed = client.post("/api/resumes/parse", json={"raw_text": resume_text, "source": "email", "file_name": "resume.txt"}, headers=headers)
    assert parsed.status_code == 200
    data = parsed.json()
    assert data["candidate_id"] is not None
    assert data["parsed_json"]["phone"] == "0988123456"
    parsed_again = client.post("/api/resumes/parse", json={"raw_text": resume_text, "source": "email"}, headers=headers)
    assert parsed_again.json()["candidate_id"] == data["candidate_id"]

    bulk = client.post(
        "/api/imports/candidates",
        json={"candidates": [{"name": "Bulk A", "phone": "0977000001", "email": "bulk-a@example.com"}, {"name": "Bulk A Duplicate", "phone": "0977000001", "email": "bulk-a@example.com"}]},
        headers=headers,
    )
    assert bulk.status_code == 200
    assert bulk.json()["created"] == 1
    assert bulk.json()["deduplicated"] == 1

    conversation = client.post("/api/conversations", json={"channel": "email", "candidate_id": data["candidate_id"]}, headers=headers)
    client.post("/api/messages", json={"conversation_id": conversation.json()["id"], "sender_type": "candidate", "body": "contains private data"}, headers=headers)
    deleted = client.delete(f"/api/candidates/{data['candidate_id']}", headers=headers)
    assert deleted.status_code == 200
    exported = client.get(f"/api/candidates/{data['candidate_id']}/export")
    assert exported.json()["resumes"][0]["raw_text"] == "[deleted by authorized privacy request]"
    assert exported.json()["messages"][0]["body"] == "[deleted by authorized privacy request]"


def test_email_inbound_complete_resume_and_notification_link():
    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]
    complete = client.post(
        "/api/inbound-emails/import",
        json={
            "message_id": "<complete-resume@example.test>",
            "sender": "notify@518.example",
            "received_at": "2026-09-27T06:00:00Z",
            "subject": "518 application with resume",
            "source_platform": "518",
            "job_id": job["id"],
            "raw_text": "姓名: Test Worker\nPhone: 0988123999\nEmail: worker518@example.com\n地點 桃園\n班別 日班\n技能 warehouse",
            "attachment_names": ["resume.txt"],
        },
        headers=headers,
    )
    assert complete.status_code == 200
    assert complete.json()["candidate_id"] is not None
    assert complete.json()["application_id"] is not None
    assert complete.json()["import_status"] == "Parsed"
    duplicate = client.post(
        "/api/inbound-emails/import",
        json={
            "message_id": "<complete-resume@example.test>",
            "sender": "notify@518.example",
            "received_at": "2026-09-27T06:00:00Z",
            "subject": "duplicate",
            "raw_text": "duplicate",
        },
        headers=headers,
    )
    assert duplicate.json()["id"] == complete.json()["id"]

    linked = client.post(
        "/api/inbound-emails/import",
        json={
            "message_id": "<link-only@example.test>",
            "sender": "notify@518.example",
            "received_at": "2026-09-27T06:05:00Z",
            "subject": "518 application notification",
            "source_platform": "518",
            "job_id": job["id"],
            "raw_text": "New applicant. View resume: https://www.518.com.tw/applicant/abc123",
        },
        headers=headers,
    )
    assert linked.status_code == 200
    assert linked.json()["candidate_id"] is None
    assert linked.json()["import_status"] == "Pending Resume Fetch"
    tasks = client.get("/api/queue/tasks", headers=headers).json()
    assert any(task["task_type"] == "platform.fetch_resume" and task["idempotency_key"] == "email-fetch-resume:<link-only@example.test>" for task in tasks)


def test_external_message_queues_and_does_not_fake_delivery():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[1]
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"]}, headers=headers)
    message = client.post(
        "/api/messages",
        json={"conversation_id": conversation.json()["id"], "sender_type": "hr", "body": "Please confirm phone availability", "send_external": True},
        headers=headers,
    )
    assert message.status_code == 200
    assert message.json()["delivery_status"] == "Pending Send"
    tasks = client.get("/api/queue/tasks", headers=headers).json()
    task = next(task for task in tasks if task["payload"].get("message_id") == message.json()["id"])
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.status_code == 200
    refreshed = client.get(f"/api/messages?conversation_id={conversation.json()['id']}", headers=headers).json()[0]
    assert refreshed["delivery_status"] == "Pending Send"
    assert "Missing external conversation mapping" in ran.json()["last_error"]
    runs = client.get("/api/automation/runs", headers=headers).json()
    assert any(run["queue_task_id"] == task["id"] and run["status"] == "Failed" for run in runs)


def test_paused_conversation_blocks_automatic_outbound_send(tmp_path):
    headers = auth_headers()
    fixture = tmp_path / "line-paused-fixture.json"
    fixture.write_text(json.dumps({"send_message": {"ok": True, "status": "Sent", "message": "sent", "data": {"platform_message_id": "paused-should-not-send"}}}), encoding="utf-8")
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE paused fixture", "status": "Configured", "config": {"fixture_path": str(fixture)}, "capabilities": ["send_message"]},
        headers=headers,
    ).json()
    candidate = client.get("/api/candidates").json()[1]
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"]}, headers=headers).json()
    with SessionLocal() as db:
        row = db.get(Conversation, conversation["id"])
        row.status = "paused"
        db.commit()
    client.post(
        "/api/platform-identity-maps",
        json={"platform": "line", "account_id": account["id"], "entity_type": "conversation", "external_id": "paused-line-conv", "candidate_id": candidate["id"], "conversation_id": conversation["id"], "external_conversation_id": "paused-line-conv", "external_candidate_id": "paused-line-user", "metadata_json": {"private_recipient_verified": True, "line_source_type": "user"}},
        headers=headers,
    )
    message = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "system", "body": "do not send", "send_external": True}, headers=headers).json()
    task = next(task for task in client.get("/api/queue/tasks", headers=headers).json() if task["payload"].get("message_id") == message["id"])
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.json()["status"] == "Manual Review Required"
    with SessionLocal() as db:
        row = db.get(Message, message["id"])
        assert row.platform_message_id is None
        assert row.delivery_status == "Manual Review Required"


def test_queue_idempotency_key_returns_existing_task():
    headers = auth_headers()
    payload = {"queue_name": "posting", "task_type": "platform.create_post", "payload": {"post_id": 999999}, "idempotency_key": "idem-test-1"}
    first = client.post("/api/queue/tasks", json=payload, headers=headers)
    second = client.post("/api/queue/tasks", json=payload, headers=headers)
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]


def test_platform_account_config_is_redacted_and_metrics_cannot_fake_success(tmp_path):
    headers = auth_headers()
    account = client.post(
        "/api/platform-accounts",
        json={
            "platform": "line",
            "display_name": "LINE official test",
            "status": "Configured",
            "capabilities": ["send_message"],
            "config": {"base_url": "https://line.example.test", "api_token": "secret-token", "session_state_path": "C:/secret/state.json"},
        },
        headers=headers,
    )
    assert account.status_code == 200
    assert account.json()["config"]["api_token"] == "***configured***"
    assert account.json()["config"]["session_state_path"] == "***configured***"
    job = client.get("/api/jobs").json()[0]
    post = client.post("/api/posts", json={"job_id": job["id"], "platform": "line", "content": "hello"}, headers=headers)
    assert post.status_code == 200
    blocked = client.patch(f"/api/posts/{post.json()['id']}/metrics", json={"status": "Published", "metrics": {"reach": 100}}, headers=headers)
    assert blocked.status_code == 409


def test_fetch_resume_worker_uses_fixture_and_updates_link_only_email(tmp_path):
    headers = auth_headers()
    fixture = tmp_path / "518-fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "fetch_resume": {
                    "ok": True,
                    "status": "Delivered",
                    "message": "Resume fixture fetched.",
                    "data": {
                        "raw_text": "Name: Fixture Applicant\nPhone: 0988000111\nEmail: fixture518@example.com\nRegion: Taoyuan\nSkills: warehouse, packing",
                        "file_name": "fixture-resume.txt",
                        "platform_candidate_id": "518-applicant-1",
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "518", "display_name": "518 browser fixture", "status": "Configured", "config": {"fixture_path": str(fixture)}, "capabilities": ["fetch_resume"]},
        headers=headers,
    )
    linked = client.post(
        "/api/inbound-emails/import",
        json={
            "message_id": "<fixture-link-only@example.test>",
            "sender": "notify@518.example",
            "received_at": "2026-09-27T06:30:00Z",
            "subject": "518 link only",
            "source_platform": "518",
            "raw_text": "Open resume https://www.518.com.tw/applicant/fixture-1",
        },
        headers=headers,
    )
    task = client.post(
        "/api/queue/tasks",
        json={
            "queue_name": "platform-worker",
            "task_type": "platform.fetch_resume",
            "payload": {"platform": "518", "platform_link": linked.json()["platform_link"], "inbound_email_id": linked.json()["id"], "account_id": account.json()["id"]},
            "idempotency_key": "fixture-fetch-resume",
            "max_retries": 1,
        },
        headers=headers,
    )
    ran = client.post(f"/api/queue/tasks/{task.json()['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Completed"
    inbound = next(item for item in client.get("/api/inbound-emails", headers=headers).json() if item["id"] == linked.json()["id"])
    assert inbound["import_status"] == "Parsed"
    assert inbound["candidate_id"] is not None
    actions = client.get("/api/automation/actions", headers=headers).json()
    assert any(item["action_type"] == "platform.fetch_resume" and item["status"] == "Delivered" for item in actions)


def test_fixture_message_delivery_verification_and_required_question_flow(tmp_path, monkeypatch):
    headers = auth_headers()
    def fake_question_ai(provider, prompt):
        return (
            json.dumps({"decision": "answer_current_question", "question_key": "preferred_area", "answer_text": "不知道，日班或夜班都可以", "confidence": 0.94, "risk_flags": []}, ensure_ascii=False),
            {"input_tokens": 10, "output_tokens": 8, "actual": True},
        )

    monkeypatch.setattr("backend.app.services.ai.call_provider_sync", fake_question_ai)
    provider = client.post(
        "/api/ai/providers",
        json={"name": "question-ai-test", "provider_type": "openai_compatible", "base_url": "https://ai.test", "model": "question-model", "supports_text": True, "supports_json_schema": True},
        headers=headers,
    )
    client.post(
        "/api/ai/routes",
        json={
            "task_name": "RequiredQuestionAnswerExtraction",
            "primary_provider_id": provider.json()["id"],
            "fallback_provider_ids": [],
            "budget_policy": {
                "expected_json_schema": {
                    "type": "object",
                    "required": ["decision", "question_key", "answer_text", "confidence", "risk_flags"],
                    "properties": {"risk_flags": {"type": "array"}, "decision": {"type": "string"}, "question_key": {"type": "string"}, "answer_text": {"type": "string"}},
                }
            },
        },
        headers=headers,
    )
    fixture = tmp_path / "line-fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "send_message": {"ok": True, "status": "Delivery Unconfirmed", "message": "Message accepted.", "data": {"platform_message_id": "line-msg-1"}},
                "verify_message": {"ok": True, "status": "Delivered", "message": "Message is visible on platform.", "data": {"platform_message_id": "line-msg-1", "seen": True}},
            }
        ),
        encoding="utf-8",
    )
    account = client.post(
        "/api/platform-accounts",
        json={
            "platform": "line",
            "display_name": "LINE fixture",
            "status": "Configured",
            "config": {"base_url": "https://line.example.test", "fixture_path": str(fixture)},
            "capabilities": ["send_message", "verify_message"],
        },
        headers=headers,
    )
    candidate = client.get("/api/candidates").json()[2]
    job = client.get("/api/jobs").json()[0]
    application = client.post("/api/applications", json={"candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers)
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers)
    mapping = client.post(
        "/api/platform-identity-maps",
        json={
            "platform": "line",
            "account_id": account.json()["id"],
            "entity_type": "conversation",
            "external_id": "line-conv-1",
            "external_conversation_id": "line-conv-1",
            "external_candidate_id": "line-candidate-1",
            "conversation_id": conversation.json()["id"],
            "candidate_id": candidate["id"],
            "job_id": job["id"],
        },
        headers=headers,
    )
    assert mapping.status_code == 200
    ask = client.post(f"/api/applications/{application.json()['id']}/ask-next-question?conversation_id={conversation.json()['id']}", headers=headers)
    assert ask.status_code == 200
    task_payload = ask.json()["payload"]
    task_payload["account_id"] = account.json()["id"]
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "messaging", "task_type": "message.send", "payload": task_payload, "idempotency_key": "fixture-question-send"},
        headers=headers,
    )
    ran = client.post(f"/api/queue/tasks/{task.json()['id']}/run", headers=headers)
    assert ran.status_code == 200
    verify_task = next(item for item in client.get("/api/queue/tasks", headers=headers).json() if item["task_type"] == "message.verify" and item["payload"].get("platform_message_id") == "line-msg-1")
    verified = client.post(f"/api/queue/tasks/{verify_task['id']}/run", headers=headers)
    assert verified.status_code == 200
    candidate_reply = client.post("/api/messages", json={"conversation_id": conversation.json()["id"], "sender_type": "candidate", "body": "不知道，日班或夜班都可以"}, headers=headers)
    assert candidate_reply.status_code == 200
    answers = client.get(f"/api/question-answers?application_id={application.json()['id']}", headers=headers).json()
    assert any(answer["answer_status"] in {"Needs Clarification", "Multi Value"} for answer in answers)
    with SessionLocal() as db:
        followups = db.execute(select(QueueTask).where(QueueTask.task_type == "flow.follow_up_inactive", QueueTask.payload["conversation_id"].as_integer() == conversation.json()["id"])).scalars().all()
        assert followups
        assert all(task.status == "Cancelled" for task in followups)
        request = db.execute(select(AIRequest).where(AIRequest.feature == "RequiredQuestionAnswerExtraction").order_by(AIRequest.id.desc())).scalars().first()
        assert request is not None
        assert request.provider == "question-ai-test"


def test_answer_without_asked_question_does_not_record_required_answer():
    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]
    with SessionLocal() as db:
        candidate_row = Candidate(name="No Asked Question Candidate", phone="0919000001", source_platform="test")
        db.add(candidate_row)
        db.commit()
        candidate = {"id": candidate_row.id}
    application = client.post("/api/applications", json={"candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers)
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers)
    assert application.status_code == 200
    reply = client.post("/api/messages", json={"conversation_id": conversation.json()["id"], "sender_type": "candidate", "body": "I can work nights"}, headers=headers)
    assert reply.status_code == 200
    with SessionLocal() as db:
        answers = db.execute(select(CandidateQuestionAnswer).where(CandidateQuestionAnswer.application_id == application.json()["id"])).scalars().all()
        assert answers
        assert all(answer.answer_status == "Unanswered" for answer in answers)
        assert db.scalar(select(CandidateQuestionAnswer).where(CandidateQuestionAnswer.evidence_message_id == reply.json()["id"])) is None


def test_interview_creation_schedules_reminders_and_no_show_check():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[5]
    job = client.get("/api/jobs").json()[0]
    future = datetime.now(UTC) + timedelta(days=2)
    interview = client.post(
        "/api/interviews",
        json={
            "candidate_id": candidate["id"],
            "job_id": job["id"],
            "interview_date": future.strftime("%Y-%m-%d"),
            "interview_time": future.strftime("%H:%M"),
            "reminder_policy": ["24h", "1h"],
        },
        headers=headers,
    )
    assert interview.status_code == 200
    with SessionLocal() as db:
        tasks = db.execute(select(QueueTask).where(QueueTask.payload["interview_id"].as_integer() == interview.json()["id"])).scalars().all()
        task_types = {task.task_type for task in tasks}
        assert "flow.interview_reminder" in task_types
        assert "flow.interview_no_show_check" in task_types
        assert len([task for task in tasks if task.task_type == "flow.interview_reminder"]) == 2


def test_interview_confirm_reply_keeps_no_show_until_attendance_and_negation_goes_manual():
    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]
    candidate_confirm = client.get("/api/candidates").json()[6]
    future = datetime.now(UTC) + timedelta(days=1)
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate_confirm["id"], "job_id": job["id"]}, headers=headers).json()
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate_confirm["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "reminder_policy": ["24h"]},
        headers=headers,
    ).json()
    reply = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "candidate", "body": "我確認會準時到場"}, headers=headers)
    assert reply.status_code == 200
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        assert row.status == "Confirmed"
        no_show = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_no_show_check", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        assert no_show.status == "Queued"
        assert no_show.payload.get("booking_version")

    candidate_negated = client.get("/api/candidates").json()[7]
    conversation_negated = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate_negated["id"], "job_id": job["id"]}, headers=headers).json()
    interview_negated = client.post(
        "/api/interviews",
        json={"candidate_id": candidate_negated["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "reminder_policy": ["24h"]},
        headers=headers,
    ).json()
    negated_reply = client.post("/api/messages", json={"conversation_id": conversation_negated["id"], "sender_type": "candidate", "body": "我不確認，明天不會去"}, headers=headers)
    assert negated_reply.status_code == 200
    with SessionLocal() as db:
        row = db.get(Interview, interview_negated["id"])
        assert row.status == "Manual Review Required"
        manual = db.scalar(select(QueueTask).where(QueueTask.task_type == "manual.interview_reply_classification", QueueTask.payload["message_id"].as_integer() == negated_reply.json()["id"]))
        assert manual is not None


def test_interview_no_show_rechecks_latest_confirm_before_sending():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[8]
    job = client.get("/api/jobs").json()[0]
    future = datetime.now(UTC) + timedelta(hours=3)
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "reminder_policy": ["1h"]},
        headers=headers,
    ).json()
    client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "candidate", "body": "OK 會到"}, headers=headers)
    with SessionLocal() as db:
        no_show = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_no_show_check", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        no_show.status = "Queued"
        no_show.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        db.commit()
        no_show_id = no_show.id
    ran = client.post(f"/api/queue/tasks/{no_show_id}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        assert row.status == "No Response Follow-up"
        assert db.scalar(select(Message).where(Message.metadata_json["flow"].as_string() == "interview_no_show_30m", Message.conversation_id == conversation["id"]))


def test_interview_timezone_and_policy_validation():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[6]
    job = client.get("/api/jobs").json()[0]
    invalid_policy = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2026-10-05", "interview_time": "10:00", "reminder_policy": ["2x"]},
        headers=headers,
    )
    assert invalid_policy.status_code == 422
    invalid_timezone = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2026-10-05", "interview_time": "10:00", "timezone": "Mars/Base"},
        headers=headers,
    )
    assert invalid_timezone.status_code == 422
    future = datetime.now(UTC) + timedelta(days=3)
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "timezone": "Asia/Taipei", "reminder_policy": ["24h"]},
        headers=headers,
    )
    assert interview.status_code == 200
    assert interview.json()["timezone"] == "Asia/Taipei"
    with SessionLocal() as db:
        reminder = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_reminder", QueueTask.payload["interview_id"].as_integer() == interview.json()["id"]))
        assert reminder is not None
        assert reminder.scheduled_at is not None


def test_518_rejects_untrusted_resume_link_and_phone_node_is_not_configured():
    headers = auth_headers()
    unsafe = client.post(
        "/api/inbound-emails/import",
        json={
            "message_id": "<unsafe-link@example.test>",
            "sender": "notify@518.example",
            "received_at": "2026-09-30T05:00:00Z",
            "subject": "518 notification",
            "source_platform": "518",
            "raw_text": "Open resume https://evil.example/resume/1",
        },
        headers=headers,
    )
    assert unsafe.status_code == 200
    assert unsafe.json()["import_status"] == "Parse Failed"
    assert "outside the allowed 518 domains" in unsafe.json()["parse_error"]

    candidate = client.get("/api/candidates").json()[0]
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "phone", "task_type": "phone.dial", "payload": {"candidate_id": candidate["id"], "script": "請確認是否方便面試。", "consent_disclosed": True}, "max_retries": 1},
        headers=headers,
    )
    ran = client.post(f"/api/queue/tasks/{task.json()['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Manual Review Required"
    calls = client.get("/api/phone/calls", headers=headers).json()
    assert any(call["candidate_id"] == candidate["id"] and call["dial_status"] == "Not Configured" for call in calls)


def test_provider_adapters_use_native_payloads(monkeypatch):
    calls = []

    class DummyResponse:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self._data

    class DummyClient:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def post(self, url, json=None, headers=None, params=None):
            calls.append({"url": url, "json": json, "headers": headers, "params": params})
            if "messages" in url:
                return DummyResponse({"content": [{"type": "text", "text": "anthropic ok"}], "usage": {"input_tokens": 3, "output_tokens": 2}})
            if "generateContent" in url:
                return DummyResponse({"candidates": [{"content": {"parts": [{"text": "gemini ok"}]}}], "usageMetadata": {"promptTokenCount": 4, "candidatesTokenCount": 2}})
            return DummyResponse({"message": {"content": "ollama ok"}, "prompt_eval_count": 5, "eval_count": 2})

    monkeypatch.setattr("backend.app.services.ai.httpx.Client", DummyClient)
    anthropic = AIProvider(name="anthropic", provider_type="anthropic", base_url="https://api.anthropic.test", endpoint="/v1/messages", model="claude-test")
    gemini = AIProvider(name="gemini", provider_type="gemini", base_url="https://generativelanguage.googleapis.test", endpoint="/v1beta/models/gemini-test:generateContent", model="gemini-test")
    ollama = AIProvider(name="ollama", provider_type="ollama", base_url="http://localhost:11434", model="llama3")
    assert call_provider_sync(anthropic, "hello")[0] == "anthropic ok"
    assert calls[-1]["json"]["messages"][0]["content"] == "hello"
    assert call_provider_sync(gemini, "hello")[0] == "gemini ok"
    assert calls[-1]["json"]["contents"][0]["parts"][0]["text"] == "hello"
    assert call_provider_sync(ollama, "hello")[0] == "ollama ok"
    assert calls[-1]["json"]["stream"] is False


def test_provider_test_returns_sanitized_http_error(monkeypatch):
    secret = "sk-test-secret-value-1234567890"
    provider = AIProvider(
        name="failing-openai",
        provider_type="openai",
        base_url="https://api.openai.test/v1",
        endpoint="/chat/completions",
        model="gpt-test",
        api_key_encrypted=encrypt_secret(secret),
        enabled=True,
    )

    async def fail_call(_provider, _prompt):
        request = httpx.Request("POST", "https://api.openai.test/v1/chat/completions")
        response = httpx.Response(
            429,
            request=request,
            json={"error": {"message": f"quota exhausted for {secret}", "code": "insufficient_quota"}},
        )
        raise httpx.HTTPStatusError("quota", request=request, response=response)

    monkeypatch.setattr("backend.app.services.ai.call_provider", fail_call)
    status, latency, message, http_status = asyncio.run(run_provider_test(provider))
    assert status == "Error"
    assert latency is not None
    assert http_status == 429
    assert secret not in message
    assert "[REDACTED_API_KEY]" in message
    assert "insufficient_quota" in message


def test_ai_route_invalid_json_schema_falls_back_to_template(monkeypatch):
    headers = auth_headers()

    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "not json"}}], "usage": {"prompt_tokens": 2, "completion_tokens": 2}}

    class DummyClient:
        def __init__(self, timeout):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def post(self, *args, **kwargs):
            return DummyResponse()

    monkeypatch.setattr("backend.app.services.ai.httpx.Client", DummyClient)
    provider = client.post(
        "/api/ai/providers",
        json={"name": "schema-test", "provider_type": "openai_compatible", "base_url": "https://provider.test", "model": "model", "supports_json_schema": True},
        headers=headers,
    )
    route = client.post(
        "/api/ai/routes",
        json={
            "task_name": "SchemaRoute",
            "primary_provider_id": provider.json()["id"],
            "fallback_provider_ids": [],
            "budget_policy": {"expected_json_schema": {"type": "object", "required": ["label"]}},
        },
        headers=headers,
    )
    assert route.status_code == 200
    run = client.post("/api/ai/run", json={"task_name": "SchemaRoute", "prompt": "classify"}, headers=headers)
    assert run.status_code == 200
    assert run.json()["status"] == "template_fallback"
    assert run.json()["attempts"][0]["status"] == "error"


def test_ai_request_masks_phone_and_email_in_audit_text():
    headers = auth_headers()
    run = client.post("/api/ai/run", json={"task_name": "MessageClassification", "prompt": "Phone: 0988123456 Email: secret@example.com wants interview"}, headers=headers)
    assert run.status_code == 200
    with SessionLocal() as db:
        request = db.execute(select(AIRequest).order_by(AIRequest.id.desc())).scalars().first()
        assert "0988123456" not in request.prompt_masked
        assert "secret@example.com" not in request.prompt_masked
        assert "[phone]" in request.prompt_masked or "[redacted]" in request.prompt_masked


def test_ai_route_rejects_provider_without_required_capability():
    headers = auth_headers()
    provider = client.post(
        "/api/ai/providers",
        json={"name": "text-only-route-test", "provider_type": "local_template", "base_url": "local", "model": "template", "supports_text": True, "supports_vision": False},
        headers=headers,
    )
    assert provider.status_code == 200
    route = client.post(
        "/api/ai/routes",
        json={"task_name": "WebVisionRecognition", "primary_provider_id": provider.json()["id"], "fallback_provider_ids": [], "budget_policy": {"requires_vision": True}},
        headers=headers,
    )
    assert route.status_code == 409
    assert "supports_vision" in route.json()["detail"]


def test_signed_phone_callback_and_replay(monkeypatch):
    import hashlib
    import hmac
    import time

    monkeypatch.setenv("PHONE_WEBHOOK_SECRET", "phone-test-secret")
    from backend.app.core.config import get_settings

    get_settings.cache_clear()
    with SessionLocal() as db:
        call = PhoneCallLog(candidate_id=1, external_call_id="call-signed-1", dial_status="Accepted")
        db.add(call)
        db.commit()
    body = b'{"call_id":"call-signed-1","status":"completed","transcript":"ok"}'
    timestamp = str(int(time.time()))
    nonce = "nonce-1"
    signature = hmac.new(b"phone-test-secret", f"{timestamp}.{nonce}.".encode("utf-8") + body, hashlib.sha256).hexdigest()
    response = client.post(
        "/api/webhooks/phone",
        content=body,
        headers={"content-type": "application/json", "x-phone-timestamp": timestamp, "x-phone-nonce": nonce, "x-phone-signature": f"sha256={signature}"},
    )
    assert response.status_code == 200
    replay = client.post(
        "/api/webhooks/phone",
        content=body,
        headers={"content-type": "application/json", "x-phone-timestamp": timestamp, "x-phone-nonce": nonce, "x-phone-signature": f"sha256={signature}"},
    )
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    monkeypatch.delenv("PHONE_WEBHOOK_SECRET", raising=False)
    get_settings.cache_clear()


def test_phone_no_answer_callback_enqueues_one_fallback_message(monkeypatch):
    import hashlib
    import hmac
    import time

    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[1]
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"]}, headers=headers)
    monkeypatch.setenv("PHONE_WEBHOOK_SECRET", "phone-test-secret")
    from backend.app.core.config import get_settings

    get_settings.cache_clear()
    with SessionLocal() as db:
        call = PhoneCallLog(candidate_id=candidate["id"], conversation_id=conversation.json()["id"], external_call_id="call-no-answer-1", dial_status="Accepted")
        db.add(call)
        db.commit()
    body = b'{"event_id":"evt-no-answer-1","provider":"mocktel","call_id":"call-no-answer-1","status":"No Answer"}'
    timestamp = str(int(time.time()))
    nonce = "nonce-no-answer-1"
    signature = hmac.new(b"phone-test-secret", f"{timestamp}.{nonce}.".encode("utf-8") + body, hashlib.sha256).hexdigest()
    response = client.post(
        "/api/webhooks/phone",
        content=body,
        headers={"content-type": "application/json", "x-phone-timestamp": timestamp, "x-phone-nonce": nonce, "x-phone-signature": f"sha256={signature}"},
    )
    assert response.status_code == 200
    with SessionLocal() as db:
        fallback_tasks = db.execute(select(QueueTask).where(QueueTask.idempotency_key == "phone-callback-fallback:mocktel:evt-no-answer-1")).scalars().all()
        assert len(fallback_tasks) == 1
        assert fallback_tasks[0].payload["conversation_id"] == conversation.json()["id"]
    replay = client.post(
        "/api/webhooks/phone",
        content=body,
        headers={"content-type": "application/json", "x-phone-timestamp": timestamp, "x-phone-nonce": nonce, "x-phone-signature": f"sha256={signature}"},
    )
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    with SessionLocal() as db:
        fallback_tasks = db.execute(select(QueueTask).where(QueueTask.idempotency_key == "phone-callback-fallback:mocktel:evt-no-answer-1")).scalars().all()
        assert len(fallback_tasks) == 1
    monkeypatch.delenv("PHONE_WEBHOOK_SECRET", raising=False)
    get_settings.cache_clear()


def test_line_webhook_signature_dedupes_and_records_inbound_message():
    import base64
    import hashlib
    import hmac

    headers = auth_headers()
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE webhook", "status": "Configured", "config": {"channel_secret": "line-secret"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    body = json.dumps(
        {
            "events": [
                {
                    "type": "message",
                    "webhookEventId": "line-event-1",
                    "source": {"type": "user", "userId": "line-user-1"},
                    "message": {"id": "line-message-1", "type": "text", "text": "I want to apply"},
                }
            ]
        },
        separators=(",", ":"),
    ).encode("utf-8")
    signature = base64.b64encode(hmac.new(b"line-secret", body, hashlib.sha256).digest()).decode("ascii")
    response = client.post(f"/api/webhooks/line?account_id={account.json()['id']}", content=body, headers={"content-type": "application/json", "x-line-signature": signature})
    assert response.status_code == 200
    replay = client.post(f"/api/webhooks/line?account_id={account.json()['id']}", content=body, headers={"content-type": "application/json", "x-line-signature": signature})
    assert replay.status_code == 200
    with SessionLocal() as db:
        event = db.scalar(select(PlatformWebhookEvent).where(PlatformWebhookEvent.event_id == "line-event-1"))
        message = db.scalar(select(Message).where(Message.platform_message_id == "line-message-1"))
        assert event is not None
        assert event.status == "Duplicate"
        assert message is not None
        assert message.delivery_status == "Delivered"


def test_line_group_members_are_isolated_by_user_and_destination():
    import base64
    import hashlib
    import hmac

    headers = auth_headers()
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE group isolation", "status": "Configured", "config": {"channel_secret": "line-group-secret"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    body = json.dumps(
        {
            "destination": "line-bot-1",
            "events": [
                {
                    "type": "message",
                    "webhookEventId": "line-group-event-1",
                    "destination": "line-bot-1",
                    "source": {"type": "group", "groupId": "line-group-1", "userId": "line-user-a"},
                    "message": {"id": "line-group-message-1", "type": "text", "text": "A applies"},
                },
                {
                    "type": "message",
                    "webhookEventId": "line-group-event-2",
                    "destination": "line-bot-1",
                    "source": {"type": "group", "groupId": "line-group-1", "userId": "line-user-b"},
                    "message": {"id": "line-group-message-2", "type": "text", "text": "B applies"},
                },
            ],
        },
        separators=(",", ":"),
    ).encode("utf-8")
    signature = base64.b64encode(hmac.new(b"line-group-secret", body, hashlib.sha256).digest()).decode("ascii")
    response = client.post(f"/api/webhooks/line?account_id={account.json()['id']}", content=body, headers={"content-type": "application/json", "x-line-signature": signature})
    assert response.status_code == 200
    assert response.json()["messages"] == 2
    with SessionLocal() as db:
        message_a = db.scalar(select(Message).where(Message.platform_message_id == "line-group-message-1"))
        message_b = db.scalar(select(Message).where(Message.platform_message_id == "line-group-message-2"))
        assert message_a.conversation_id != message_b.conversation_id
        conv_a = db.get(Conversation, message_a.conversation_id)
        conv_b = db.get(Conversation, message_b.conversation_id)
        assert conv_a.candidate_id != conv_b.candidate_id
        keys = {
            row.external_id
            for row in db.execute(select(PlatformIdentityMap).where(PlatformIdentityMap.platform == "line", PlatformIdentityMap.entity_type == "conversation")).scalars().all()
        }
        assert "line-bot-1:line-group-1:line-user-a" in keys
        assert "line-bot-1:line-group-1:line-user-b" in keys


def test_facebook_commenters_are_isolated_and_echoes_are_ignored():
    import hashlib
    import hmac

    headers = auth_headers()
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "facebook", "display_name": "FB webhook isolation", "status": "Configured", "config": {"app_secret": "fb-secret", "page_id": "page-1"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    body = json.dumps(
        {
            "entry": [
                {
                    "id": "page-1",
                    "messaging": [
                        {"sender": {"id": "fb-user-echo"}, "recipient": {"id": "page-1"}, "message": {"mid": "fb-echo-1", "is_echo": True, "text": "our own reply"}},
                        {"sender": {"id": "fb-user-delivery"}, "recipient": {"id": "page-1"}, "delivery": {"mids": ["m1"]}},
                    ],
                    "changes": [
                        {"field": "feed", "value": {"item": "comment", "post_id": "post-1", "comment_id": "comment-1", "from": {"id": "fb-user-a"}, "message": "A comment"}},
                        {"field": "feed", "value": {"item": "comment", "post_id": "post-1", "comment_id": "comment-2", "from": {"id": "fb-user-b"}, "message": "B comment"}},
                        {"field": "feed", "value": {"item": "comment", "post_id": "post-1", "comment_id": "comment-page", "from": {"id": "page-1"}, "message": "page self comment"}},
                    ],
                }
            ]
        },
        separators=(",", ":"),
    ).encode("utf-8")
    signature = "sha256=" + hmac.new(b"fb-secret", body, hashlib.sha256).hexdigest()
    response = client.post(f"/api/webhooks/facebook?account_id={account.json()['id']}", content=body, headers={"content-type": "application/json", "x-hub-signature-256": signature})
    assert response.status_code == 200
    assert response.json()["messages"] == 2
    with SessionLocal() as db:
        message_a = db.scalar(select(Message).where(Message.platform_message_id == "comment-1"))
        message_b = db.scalar(select(Message).where(Message.platform_message_id == "comment-2"))
        assert message_a is not None
        assert message_b is not None
        assert message_a.conversation_id != message_b.conversation_id
        assert db.scalar(select(Message).where(Message.platform_message_id == "fb-echo-1")) is None
        keys = {
            row.external_id
            for row in db.execute(select(PlatformIdentityMap).where(PlatformIdentityMap.platform == "facebook", PlatformIdentityMap.entity_type == "conversation")).scalars().all()
        }
        assert "page-1:post:post-1:commenter:fb-user-a" in keys
        assert "page-1:post:post-1:commenter:fb-user-b" in keys


def test_facebook_new_comment_maps_post_to_job_and_runs_inbound_ai_queue(monkeypatch):
    import hashlib
    import hmac

    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]

    def fake_initial_ai(provider, prompt):
        return (
            json.dumps({"intent": "apply_to_job", "job_id": str(job["id"]), "confidence": 0.93, "risk_flags": []}),
            {"input_tokens": 9, "output_tokens": 8, "actual": True},
        )

    monkeypatch.setattr("backend.app.services.ai.call_provider_sync", fake_initial_ai)
    provider = client.post(
        "/api/ai/providers",
        json={"name": "initial-ai-test", "provider_type": "openai_compatible", "base_url": "https://ai.test", "model": "initial-model", "supports_text": True, "supports_json_schema": True},
        headers=headers,
    )
    client.post(
        "/api/ai/routes",
        json={
            "task_name": "InitialInquiryClassification",
            "primary_provider_id": provider.json()["id"],
            "fallback_provider_ids": [],
            "budget_policy": {"expected_json_schema": {"type": "object", "required": ["intent", "job_id", "confidence", "risk_flags"], "properties": {"intent": {"type": "string"}, "job_id": {"type": "string"}, "risk_flags": {"type": "array"}}}},
        },
        headers=headers,
    )
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "facebook", "display_name": "FB inbound AI", "status": "Configured", "config": {"app_secret": "fb-inbound-secret", "page_id": "page-ai"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    with SessionLocal() as db:
        db.add(Post(job_id=job["id"], platform="facebook", account_id=account.json()["id"], content="job post", status="Verified", platform_post_id="post-ai-1"))
        db.commit()
    body = json.dumps(
        {"entry": [{"id": "page-ai", "changes": [{"field": "feed", "value": {"item": "comment", "post_id": "post-ai-1", "comment_id": "comment-ai-1", "from": {"id": "fb-new-candidate"}, "message": "我想應徵"}}]}]},
        separators=(",", ":"),
    ).encode("utf-8")
    signature = "sha256=" + hmac.new(b"fb-inbound-secret", body, hashlib.sha256).hexdigest()
    response = client.post(f"/api/webhooks/facebook?account_id={account.json()['id']}", content=body, headers={"content-type": "application/json", "x-hub-signature-256": signature})
    assert response.status_code == 200
    with SessionLocal() as db:
        message = db.scalar(select(Message).where(Message.platform_message_id == "comment-ai-1"))
        assert message is not None
        conversation = db.get(Conversation, message.conversation_id)
        assert conversation.job_id == job["id"]
        task = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.process_inbound_message", QueueTask.payload["message_id"].as_integer() == message.id))
        assert task is not None
    ran = client.post(f"/api/queue/tasks/{task.id}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        application = db.scalar(select(Application).where(Application.job_id == job["id"]))
        assert application is not None
        ai_request = db.scalar(select(AIRequest).where(AIRequest.feature == "InitialInquiryClassification").order_by(AIRequest.id.desc()))
        assert ai_request is not None
        question_task = db.scalar(select(QueueTask).where(QueueTask.task_type == "message.send", QueueTask.idempotency_key.like("question:%")))
        assert question_task is not None


def test_facebook_comment_then_messenger_reuses_candidate_job_conversation():
    import hashlib
    import hmac

    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "facebook", "display_name": "FB continuity", "status": "Configured", "config": {"app_secret": "fb-continuity-secret", "page_id": "page-cont"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    with SessionLocal() as db:
        db.add(Post(job_id=job["id"], platform="facebook", account_id=account.json()["id"], content="job post", status="Verified", platform_post_id="post-cont-1"))
        db.commit()

    comment_body = json.dumps(
        {"entry": [{"id": "page-cont", "changes": [{"field": "feed", "value": {"item": "comment", "post_id": "post-cont-1", "comment_id": "comment-cont-1", "from": {"id": "fb-cont-user"}, "message": "interested"}}]}]},
        separators=(",", ":"),
    ).encode("utf-8")
    comment_sig = "sha256=" + hmac.new(b"fb-continuity-secret", comment_body, hashlib.sha256).hexdigest()
    assert client.post(f"/api/webhooks/facebook?account_id={account.json()['id']}", content=comment_body, headers={"content-type": "application/json", "x-hub-signature-256": comment_sig}).status_code == 200
    with SessionLocal() as db:
        comment = db.scalar(select(Message).where(Message.platform_message_id == "comment-cont-1"))
        comment_conversation = db.get(Conversation, comment.conversation_id)
        assert comment_conversation.job_id == job["id"]

    messenger_body = json.dumps(
        {"entry": [{"id": "page-cont", "messaging": [{"sender": {"id": "fb-cont-user"}, "recipient": {"id": "page-cont"}, "message": {"mid": "messenger-cont-1", "text": "my answer"}}]}]},
        separators=(",", ":"),
    ).encode("utf-8")
    messenger_sig = "sha256=" + hmac.new(b"fb-continuity-secret", messenger_body, hashlib.sha256).hexdigest()
    assert client.post(f"/api/webhooks/facebook?account_id={account.json()['id']}", content=messenger_body, headers={"content-type": "application/json", "x-hub-signature-256": messenger_sig}).status_code == 200
    with SessionLocal() as db:
        comment = db.scalar(select(Message).where(Message.platform_message_id == "comment-cont-1"))
        messenger = db.scalar(select(Message).where(Message.platform_message_id == "messenger-cont-1"))
        conversation = db.get(Conversation, messenger.conversation_id)
        assert messenger.conversation_id == comment.conversation_id
        assert conversation.job_id == job["id"]
        private_map = db.scalar(select(PlatformIdentityMap).where(PlatformIdentityMap.external_id == "page-cont:messenger:fb-cont-user"))
        assert private_map.conversation_id == conversation.id
        assert private_map.metadata_json["private_recipient_verified"] is True


def test_facebook_private_reply_uses_comment_id_and_messenger_send_uses_psid(tmp_path):
    import hashlib
    import hmac

    headers = auth_headers()
    fixture = tmp_path / "fb-fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "private_reply": {"ok": True, "status": "Sent", "message": "private reply sent", "data": {"platform_message_id": "fb-private-reply-1"}},
                "send_message": {"ok": True, "status": "Sent", "message": "messenger sent", "data": {"platform_message_id": "fb-messenger-out-1"}},
            }
        ),
        encoding="utf-8",
    )
    job = client.get("/api/jobs").json()[0]
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "facebook", "display_name": "FB payload fixture", "status": "Configured", "config": {"app_secret": "fb-payload-secret", "page_id": "page-payload", "fixture_path": str(fixture)}, "capabilities": ["webhook", "send_message"]},
        headers=headers,
    )
    with SessionLocal() as db:
        db.add(Post(job_id=job["id"], platform="facebook", account_id=account.json()["id"], content="job post", status="Verified", platform_post_id="post-payload-1"))
        db.commit()
    comment_body = json.dumps(
        {"entry": [{"id": "page-payload", "changes": [{"field": "feed", "value": {"item": "comment", "post_id": "post-payload-1", "comment_id": "comment-payload-1", "from": {"id": "fb-user-payload"}, "message": "interested"}}]}]},
        separators=(",", ":"),
    ).encode("utf-8")
    signature = "sha256=" + hmac.new(b"fb-payload-secret", comment_body, hashlib.sha256).hexdigest()
    assert client.post(f"/api/webhooks/facebook?account_id={account.json()['id']}", content=comment_body, headers={"content-type": "application/json", "x-hub-signature-256": signature}).status_code == 200
    with SessionLocal() as db:
        private_reply_task = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == "facebook-private-reply:comment-payload-1"))
        assert private_reply_task is not None
        private_reply_task_id = private_reply_task.id
    assert client.post(f"/api/queue/tasks/{private_reply_task_id}/run", headers=headers).json()["status"] == "Completed"
    with SessionLocal() as db:
        private_log = db.scalar(select(ExternalActionLog).where(ExternalActionLog.action_type == "message.private_reply").order_by(ExternalActionLog.id.desc()))
        assert private_log.evidence["request_payload"]["comment_id"] == "comment-payload-1"

        inbound = db.scalar(select(Message).where(Message.platform_message_id == "comment-payload-1"))
        conversation = db.get(Conversation, inbound.conversation_id)
        db.add(
            PlatformIdentityMap(
                platform="facebook",
                account_id=account.json()["id"],
                entity_type="conversation",
                external_id="page-payload:messenger:fb-user-payload",
                external_conversation_id="page-payload:messenger:fb-user-payload",
                external_candidate_id="fb-user-payload",
                conversation_id=conversation.id,
                candidate_id=conversation.candidate_id,
                job_id=conversation.job_id,
                metadata_json={"private_recipient_verified": True, "facebook_source_type": "messenger"},
            )
        )
        outbound = Message(conversation_id=conversation.id, sender_type="system", body="private question", delivery_status="Pending Send")
        db.add(outbound)
        db.flush()
        send_task = QueueTask(queue_name="messaging", task_type="message.send", payload={"message_id": outbound.id, "conversation_id": conversation.id, "platform": "facebook", "account_id": account.json()["id"]}, idempotency_key="fb-psid-send-test", max_retries=1)
        db.add(send_task)
        db.commit()
        send_task_id = send_task.id
    assert client.post(f"/api/queue/tasks/{send_task_id}/run", headers=headers).json()["status"] == "Completed"
    with SessionLocal() as db:
        send_log = db.scalar(select(ExternalActionLog).where(ExternalActionLog.action_type == "message.send", ExternalActionLog.platform == "facebook").order_by(ExternalActionLog.id.desc()))
        assert send_log.evidence["request_payload"]["recipient_id"] == "fb-user-payload"
        assert "page-payload:messenger" not in send_log.evidence["request_payload"]["recipient_id"]


def test_line_push_payload_uses_verified_user_id_not_group_or_destination(tmp_path):
    headers = auth_headers()
    fixture = tmp_path / "line-payload-fixture.json"
    fixture.write_text(json.dumps({"send_message": {"ok": True, "status": "Sent", "message": "sent", "data": {"platform_message_id": "line-payload-out-1"}}}), encoding="utf-8")
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE payload fixture", "status": "Configured", "config": {"base_url": "https://line.example.test", "fixture_path": str(fixture)}, "capabilities": ["send_message"]},
        headers=headers,
    )
    candidate = client.get("/api/candidates").json()[2]
    job = client.get("/api/jobs").json()[0]
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    client.post(
        "/api/platform-identity-maps",
        json={
            "platform": "line",
            "account_id": account.json()["id"],
            "entity_type": "conversation",
            "external_id": "line-bot:line-user-payload",
            "external_conversation_id": "line-bot:line-user-payload",
            "external_candidate_id": "line-user-payload",
            "conversation_id": conversation["id"],
            "candidate_id": candidate["id"],
            "job_id": job["id"],
            "metadata_json": {"private_recipient_verified": True, "line_source_type": "user", "destination": "line-bot"},
        },
        headers=headers,
    )
    message = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "system", "body": "private line question", "send_external": True}, headers=headers).json()
    with SessionLocal() as db:
        task = db.scalar(select(QueueTask).where(QueueTask.payload["message_id"].as_integer() == message["id"]))
        task.payload = {**task.payload, "account_id": account.json()["id"]}
        db.commit()
        task_id = task.id
    assert client.post(f"/api/queue/tasks/{task_id}/run", headers=headers).json()["status"] == "Completed"
    with SessionLocal() as db:
        send_log = db.scalar(select(ExternalActionLog).where(ExternalActionLog.action_type == "message.send", ExternalActionLog.platform == "line").order_by(ExternalActionLog.id.desc()))
        assert send_log.evidence["request_payload"]["to"] == "line-user-payload"
        assert send_log.evidence["request_payload"]["to"] != "line-bot"
        assert "group" not in send_log.evidence["request_payload"]["to"].lower()


def test_line_group_then_one_to_one_reuses_resolved_conversation():
    import base64
    import hashlib
    import hmac

    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE continuity", "status": "Configured", "config": {"channel_secret": "line-cont-secret"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    group_body = json.dumps(
        {
            "destination": "line-bot-cont",
            "events": [
                {
                    "type": "message",
                    "webhookEventId": "line-cont-group-event",
                    "destination": "line-bot-cont",
                    "source": {"type": "group", "groupId": "line-group-cont", "userId": "line-user-cont"},
                    "message": {"id": "line-cont-group-message", "type": "text", "text": "apply"},
                }
            ],
        },
        separators=(",", ":"),
    ).encode("utf-8")
    group_sig = base64.b64encode(hmac.new(b"line-cont-secret", group_body, hashlib.sha256).digest()).decode("ascii")
    assert client.post(f"/api/webhooks/line?account_id={account.json()['id']}", content=group_body, headers={"content-type": "application/json", "x-line-signature": group_sig}).status_code == 200
    with SessionLocal() as db:
        group_message = db.scalar(select(Message).where(Message.platform_message_id == "line-cont-group-message"))
        conversation = db.get(Conversation, group_message.conversation_id)
        conversation.job_id = job["id"]
        candidate_map = db.scalar(select(PlatformIdentityMap).where(PlatformIdentityMap.platform == "line", PlatformIdentityMap.entity_type == "candidate", PlatformIdentityMap.external_id == "line-user-cont"))
        candidate_map.job_id = job["id"]
        db.commit()

    one_to_one_body = json.dumps(
        {
            "destination": "line-bot-cont",
            "events": [
                {
                    "type": "message",
                    "webhookEventId": "line-cont-user-event",
                    "destination": "line-bot-cont",
                    "source": {"type": "user", "userId": "line-user-cont"},
                    "message": {"id": "line-cont-user-message", "type": "text", "text": "one to one reply"},
                }
            ],
        },
        separators=(",", ":"),
    ).encode("utf-8")
    user_sig = base64.b64encode(hmac.new(b"line-cont-secret", one_to_one_body, hashlib.sha256).digest()).decode("ascii")
    assert client.post(f"/api/webhooks/line?account_id={account.json()['id']}", content=one_to_one_body, headers={"content-type": "application/json", "x-line-signature": user_sig}).status_code == 200
    with SessionLocal() as db:
        group_message = db.scalar(select(Message).where(Message.platform_message_id == "line-cont-group-message"))
        user_message = db.scalar(select(Message).where(Message.platform_message_id == "line-cont-user-message"))
        conversation = db.get(Conversation, user_message.conversation_id)
        assert user_message.conversation_id == group_message.conversation_id
        assert conversation.job_id == job["id"]
        private_map = db.scalar(select(PlatformIdentityMap).where(PlatformIdentityMap.external_id == "line-bot-cont:line-user-cont"))
        assert private_map.metadata_json["private_recipient_verified"] is True


def test_candidate_specific_send_refuses_public_facebook_comment_mapping():
    import hashlib
    import hmac

    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "facebook", "display_name": "FB public block", "status": "Configured", "config": {"app_secret": "fb-public-secret", "page_id": "page-public"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    with SessionLocal() as db:
        db.add(Post(job_id=job["id"], platform="facebook", account_id=account.json()["id"], content="job post", status="Verified", platform_post_id="post-public-1"))
        db.commit()
    body = json.dumps(
        {"entry": [{"id": "page-public", "changes": [{"field": "feed", "value": {"item": "comment", "post_id": "post-public-1", "comment_id": "comment-public-1", "from": {"id": "fb-public-user"}, "message": "interested"}}]}]},
        separators=(",", ":"),
    ).encode("utf-8")
    sig = "sha256=" + hmac.new(b"fb-public-secret", body, hashlib.sha256).hexdigest()
    client.post(f"/api/webhooks/facebook?account_id={account.json()['id']}", content=body, headers={"content-type": "application/json", "x-hub-signature-256": sig})
    with SessionLocal() as db:
        inbound = db.scalar(select(Message).where(Message.platform_message_id == "comment-public-1"))
        outbound = Message(conversation_id=inbound.conversation_id, sender_type="system", body="private question", delivery_status="Pending Send")
        db.add(outbound)
        db.flush()
        task = QueueTask(queue_name="messaging", task_type="message.send", payload={"message_id": outbound.id, "conversation_id": inbound.conversation_id, "platform": "facebook", "account_id": account.json()["id"]}, idempotency_key="fb-public-send-block", max_retries=1)
        db.add(task)
        db.commit()
        task_id = task.id
    ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Manual Review Required"
    assert "verified private recipient" in ran.json()["last_error"]


def test_unsuitable_three_day_rejection_is_distinct_and_idempotent():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[4]
    job = client.get("/api/jobs").json()[0]
    application = client.post("/api/applications", json={"candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    client.post(
        "/api/platform-identity-maps",
        json={
            "platform": "line",
            "entity_type": "conversation",
            "external_id": "line-private-unsuitable",
            "external_conversation_id": "line-private-unsuitable",
            "external_candidate_id": "line-private-user-unsuitable",
            "conversation_id": conversation["id"],
            "candidate_id": candidate["id"],
            "job_id": job["id"],
            "metadata_json": {"private_recipient_verified": True, "line_source_type": "user"},
        },
        headers=headers,
    )
    updated = client.patch(f"/api/applications/{application['id']}/status", json={"status": "Unsuitable", "reason": "screening result"}, headers=headers)
    assert updated.status_code == 200
    with SessionLocal() as db:
        due = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == f"unsuitable-rejection-due:{application['id']}"))
        assert due is not None
        assert due.task_type == "flow.unsuitable_rejection_due"
        due.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        db.commit()
        task_id = due.id
    ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        app_row = db.get(Application, application["id"])
        assert app_row.status == "Closed"
        notices = db.execute(select(Message).where(Message.metadata_json["flow"].as_string() == "three_day_unsuitable_rejection")).scalars().all()
        assert len([item for item in notices if item.conversation_id == conversation["id"]]) == 1
        send_task = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == f"unsuitable-rejection-message:{application['id']}"))
        assert send_task is not None


def test_retention_cleanup_masks_live_tables_and_records_run():
    headers = auth_headers()
    old = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=400)
    with SessionLocal() as db:
        inbound = InboundEmail(message_id="<retention@example.test>", sender="sender@example.test", received_at="2025-01-01T00:00:00Z", subject="old", raw_text="private raw", attachment_names=["old.pdf"])
        inbound.created_at = old
        db.add(inbound)
        ai = AIRequest(provider="p", model="m", feature="f", status="success", prompt_masked="private prompt", response_masked="private response")
        ai.created_at = old
        db.add(ai)
        db.commit()
    task = client.post(
        "/api/queue/tasks",
        json={
            "queue_name": "privacy",
            "task_type": "privacy.retention_cleanup",
            "payload": {"policies": [{"name": "test_email", "model": "InboundEmail", "days": 365, "strategy": "mask_raw_text"}, {"name": "test_ai", "model": "AIRequest", "days": 365, "strategy": "mask_prompt_output"}]},
            "idempotency_key": "retention-test-run",
            "max_retries": 1,
        },
        headers=headers,
    )
    ran = client.post(f"/api/queue/tasks/{task.json()['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        inbound = db.scalar(select(InboundEmail).where(InboundEmail.message_id == "<retention@example.test>"))
        assert inbound.raw_text == "[retained import metadata only]"
        ai = db.scalar(select(AIRequest).where(AIRequest.feature == "f").order_by(AIRequest.id.desc()))
        assert ai.prompt_masked == "[retained audit metadata only]"
        assert db.scalar(select(DataRetentionRun).where(DataRetentionRun.policy_name == "test_email")) is not None


def test_bad_webhook_signature_creates_no_platform_event():
    headers = auth_headers()
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE bad signature", "status": "Configured", "config": {"channel_secret": "bad-sig-secret"}, "capabilities": ["webhook"]},
        headers=headers,
    )
    body = b'{"events":[{"type":"message","webhookEventId":"bad-sig-event","source":{"type":"user","userId":"u"},"message":{"id":"bad-sig-message","type":"text","text":"hello"}}]}'
    response = client.post(f"/api/webhooks/line?account_id={account.json()['id']}", content=body, headers={"content-type": "application/json", "x-line-signature": "invalid"})
    assert response.status_code == 401
    with SessionLocal() as db:
        assert db.scalar(select(PlatformWebhookEvent).where(PlatformWebhookEvent.event_id == "bad-sig-event")) is None
        assert db.scalar(select(Message).where(Message.platform_message_id == "bad-sig-message")) is None


def test_platform_rate_policy_suppresses_duplicate_candidate_message(tmp_path):
    headers = auth_headers()
    fixture = tmp_path / "line-rate-fixture.json"
    fixture.write_text(json.dumps({"send_message": {"ok": True, "status": "Sent", "message": "accepted", "data": {"platform_message_id": "line-rate-msg"}}}), encoding="utf-8")
    account = client.post(
        "/api/platform-accounts",
        json={
            "platform": "line",
            "display_name": "LINE rate policy",
            "status": "Configured",
            "config": {"base_url": "https://line.example.test", "fixture_path": str(fixture), "rate_policy": {"suppress_duplicate_messages": True, "candidate_cooldown_seconds": 0}},
            "capabilities": ["send_message"],
        },
        headers=headers,
    )
    candidate = client.get("/api/candidates").json()[3]
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"]}, headers=headers)
    client.post(
        "/api/platform-identity-maps",
        json={
            "platform": "line",
            "account_id": account.json()["id"],
            "entity_type": "conversation",
            "external_id": "line-rate-conv",
            "external_conversation_id": "line-rate-conv",
            "external_candidate_id": "line-rate-user",
            "conversation_id": conversation.json()["id"],
            "candidate_id": candidate["id"],
            "metadata_json": {"private_recipient_verified": True, "line_source_type": "user"},
        },
        headers=headers,
    )
    first_message = client.post("/api/messages", json={"conversation_id": conversation.json()["id"], "sender_type": "hr", "body": "duplicate body", "send_external": True}, headers=headers)
    first_task = next(task for task in client.get("/api/queue/tasks", headers=headers).json() if task["payload"].get("message_id") == first_message.json()["id"])
    first_task["payload"]["account_id"] = account.json()["id"]
    explicit = client.post("/api/queue/tasks", json={"queue_name": "messaging", "task_type": "message.send", "payload": first_task["payload"], "idempotency_key": "rate-first"}, headers=headers)
    assert client.post(f"/api/queue/tasks/{explicit.json()['id']}/run", headers=headers).json()["status"] == "Completed"
    second_message = client.post("/api/messages", json={"conversation_id": conversation.json()["id"], "sender_type": "hr", "body": "duplicate body", "send_external": True}, headers=headers)
    second_task_payload = {"message_id": second_message.json()["id"], "conversation_id": conversation.json()["id"], "platform": "line", "account_id": account.json()["id"]}
    second_task = client.post("/api/queue/tasks", json={"queue_name": "messaging", "task_type": "message.send", "payload": second_task_payload, "idempotency_key": "rate-second", "max_retries": 1}, headers=headers)
    ran = client.post(f"/api/queue/tasks/{second_task.json()['id']}/run", headers=headers)
    assert ran.json()["status"] == "Manual Review Required"
    assert "duplicate content suppressed" in ran.json()["last_error"]
    with SessionLocal() as db:
        state = db.scalar(select(PlatformRateLimitState).where(PlatformRateLimitState.scope_type == "candidate", PlatformRateLimitState.scope_id == str(candidate["id"])))
        assert state is not None
        assert state.last_content_hash


def test_line_retry_key_is_uuid_and_409_accepted(monkeypatch):
    from backend.app.connectors.adapters import LineAdapter

    captured = {}

    class FakeResponse:
        status_code = 409
        text = '{"message":"accepted"}'
        headers = {"x-line-accepted-request-id": "line-request-accepted"}

    class FakeAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json, headers):
            captured["headers"] = headers
            captured["json"] = json
            return FakeResponse()

    import backend.app.connectors.adapters as adapters

    monkeypatch.setattr(adapters.httpx, "AsyncClient", FakeAsyncClient)
    retry_key = "123e4567-e89b-12d3-a456-426614174000"
    result = asyncio.run(LineAdapter({"channel_access_token": "token", "line_retry_key": retry_key}).send_message("U123", "hello"))
    assert result.ok is True
    assert result.status == "Accepted"
    assert captured["headers"]["X-Line-Retry-Key"] == retry_key
    assert len(captured["headers"]["X-Line-Retry-Key"]) == 36
    assert result.data["accepted_duplicate"] is True


def test_line_send_persists_retry_key_and_unknown_verify_preserves_accepted(tmp_path):
    headers = auth_headers()
    fixture = tmp_path / "line-unknown-fixture.json"
    fixture.write_text(json.dumps({"send_message": {"ok": True, "status": "Accepted", "message": "accepted", "data": {}}, "verify_message": {"ok": True, "status": "Unknown", "message": "unknown", "data": {"candidate_read_status": "Unknown"}}}), encoding="utf-8")
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "line", "display_name": "LINE unknown verify", "status": "Configured", "config": {"base_url": "https://line.example.test", "fixture_path": str(fixture)}, "capabilities": ["send_message", "verify_message"]},
        headers=headers,
    ).json()
    candidate = client.post("/api/candidates", json={"name": "Line Retry Candidate", "source_platform": "line"}, headers=headers).json()
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"]}, headers=headers).json()
    client.post(
        "/api/platform-identity-maps",
        json={
            "platform": "line",
            "account_id": account["id"],
            "entity_type": "conversation",
            "external_id": "line-retry-conv",
            "candidate_id": candidate["id"],
            "conversation_id": conversation["id"],
            "external_conversation_id": "line-retry-conv",
            "external_candidate_id": "line-retry-user",
            "metadata_json": {"private_recipient_verified": True, "line_source_type": "user"},
        },
        headers=headers,
    )
    message = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "system", "body": "same text", "send_external": True}, headers=headers).json()
    send_task = next(task for task in client.get("/api/queue/tasks", headers=headers).json() if task["payload"].get("message_id") == message["id"] and task["task_type"] == "message.send")
    assert client.post(f"/api/queue/tasks/{send_task['id']}/run", headers=headers).json()["status"] == "Completed"
    with SessionLocal() as db:
        row = db.get(Message, message["id"])
        assert row.delivery_status == "Accepted"
        retry_key = (row.metadata_json or {}).get("line_retry_key")
        assert retry_key and len(retry_key) == 36
        row.platform_message_id = "line-accepted-request"
        verify_task = QueueTask(queue_name="verification", task_type="message.verify", payload={"message_id": row.id, "platform": "line", "platform_message_id": row.platform_message_id, "account_id": account["id"]}, idempotency_key=f"line-unknown-verify:{row.id}", max_retries=1)
        db.add(verify_task)
        db.commit()
        task_id = verify_task.id
    ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
    assert ran.status_code == 200
    with SessionLocal() as db:
        row = db.get(Message, message["id"])
        assert row.delivery_status == "Accepted"
        assert (row.metadata_json or {}).get("line_retry_key") == retry_key


def test_scheduler_creates_periodic_518_email_poll_task():
    with SessionLocal() as db:
        account = PlatformAccount(
            platform="email",
            display_name="518 mailbox",
            status="Configured",
            config={"auto_poll_518": True, "mailbox": "INBOX", "poll_interval_seconds": 60},
            capabilities=["read_messages"],
        )
        db.add(account)
        db.commit()
        ensure_periodic_email_poll_tasks(db)
        task = db.scalar(select(QueueTask).where(QueueTask.task_type == "email.poll_518", QueueTask.payload["account_id"].as_integer() == account.id))
        assert task is not None


def _email_bytes(message_id: str, subject: str = "Apply") -> bytes:
    return f"Message-ID: <{message_id}>\nFrom: worker@example.test\nDate: Mon, 1 Jan 2026 00:00:00 +0000\nSubject: {subject}\n\n姓名: Email Worker\nPhone: 0911222333\n".encode()


class FakeImap:
    uid_validity = "valid-1"
    stored = []

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def login(self, username, password):
        return "OK", [b"logged in"]

    def select(self, mailbox):
        return "OK", [b"25"]

    def response(self, name):
        return "OK", [self.uid_validity.encode()]

    def uid(self, command, *args):
        if command == "search":
            return "OK", [b" ".join(str(uid).encode() for uid in range(1, 26))]
        if command == "fetch":
            uid = int(args[0])
            return "OK", [(b"BODY[]", _email_bytes(f"msg-{uid}@example.test"))]
        if command == "store":
            self.stored.append(str(args[0].decode() if isinstance(args[0], bytes) else args[0]))
            return "OK", [b"stored"]
        raise AssertionError(command)


def test_imap_poll_reads_oldest_unprocessed_batch(monkeypatch):
    import imaplib

    monkeypatch.setattr(imaplib, "IMAP4_SSL", FakeImap)
    messages = poll_imap({"host": "imap.example.test", "username": "u", "password": "p", "mailbox": "INBOX"}, limit=20, min_uid=0)
    assert [item.imap_uid for item in messages[:5]] == ["1", "2", "3", "4", "5"]
    assert messages[-1].imap_uid == "20"


def test_imap_ack_rejects_uidvalidity_change(monkeypatch):
    import imaplib

    class ChangedValidityImap(FakeImap):
        uid_validity = "valid-2"

    monkeypatch.setattr(imaplib, "IMAP4_SSL", ChangedValidityImap)
    try:
        acknowledge_imap_messages({"host": "imap.example.test", "username": "u", "password": "p", "mailbox": "INBOX"}, ["1"], uid_validity="valid-1")
    except RuntimeError as exc:
        assert "UIDVALIDITY changed" in str(exc)
    else:
        raise AssertionError("ACK should reject changed UIDVALIDITY")
        assert task.payload["mailbox"] == "INBOX"


def test_scheduler_creates_periodic_retention_cleanup_task(monkeypatch):
    monkeypatch.setenv("RETENTION_CLEANUP_INTERVAL_SECONDS", "3600")
    monkeypatch.setenv("RETENTION_CLEANUP_BATCH_SIZE", "123")
    get_settings.cache_clear()
    with SessionLocal() as db:
        try:
            for task in db.execute(select(QueueTask).where(QueueTask.task_type == "privacy.retention_cleanup", QueueTask.status.in_(["Queued", "Retry", "Running"]))).scalars().all():
                task.status = "Cancelled"
            db.commit()
            ensure_periodic_retention_cleanup_tasks(db)
            first = db.scalar(select(QueueTask).where(QueueTask.task_type == "privacy.retention_cleanup", QueueTask.payload["source"].as_string() == "periodic_scheduler"))
            assert first is not None
            assert first.payload["batch_size"] == 123
            first_id = first.id
            ensure_periodic_retention_cleanup_tasks(db)
            tasks = db.execute(select(QueueTask).where(QueueTask.task_type == "privacy.retention_cleanup", QueueTask.payload["source"].as_string() == "periodic_scheduler")).scalars().all()
            assert [task.id for task in tasks].count(first_id) == 1
        finally:
            get_settings.cache_clear()


def _make_interview_reply_case(headers, *, body: str, entry: str, tmp_path=None):
    job = client.get("/api/jobs").json()[0]
    unique = f"{entry}-{datetime.now(UTC).timestamp()}".replace(".", "")
    with SessionLocal() as db:
        candidate_row = Candidate(name=f"Interview Reply {unique}", phone=f"09{unique[-8:]}", source_platform="test")
        db.add(candidate_row)
        db.commit()
        candidate = {"id": candidate_row.id}
    application = client.post("/api/applications", json={"candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    conversation = client.post("/api/conversations", json={"channel": "facebook", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2027-01-10", "interview_time": "10:00"},
        headers=headers,
    ).json()
    ask = client.post(f"/api/applications/{application['id']}/ask-next-question?conversation_id={conversation['id']}", headers=headers)
    assert ask.status_code == 200
    with SessionLocal() as db:
        question = db.scalar(select(CandidateQuestionAnswer).where(CandidateQuestionAnswer.application_id == application["id"]))
        prompt = db.scalar(select(Message).where(Message.metadata_json["question_answer_id"].as_integer() == question.id))
        prompt.delivery_status = "Delivered"
        db.commit()

    if entry == "api":
        response = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "candidate", "body": body}, headers=headers)
        assert response.status_code == 200
    elif entry == "queue":
        with SessionLocal() as db:
            message = Message(conversation_id=conversation["id"], sender_type="candidate", body=body, delivery_status="Delivered")
            db.add(message)
            db.flush()
            task = QueueTask(queue_name="flow", task_type="flow.process_inbound_message", payload={"message_id": message.id}, idempotency_key=f"test-inbound-entry:{entry}:{message.id}", max_retries=1)
            db.add(task)
            db.commit()
            task_id = task.id
        ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
        assert ran.status_code == 200
    elif entry == "sync":
        fixture = tmp_path / f"sync-{interview['id']}.json"
        fixture.write_text(
            json.dumps(
                {
                    "read_messages": {
                        "ok": True,
                        "status": "Delivered",
                        "data": {
                            "items": [
                                {
                                    "candidate_platform_id": f"sync-candidate-{interview['id']}",
                                    "conversation_platform_id": f"sync-conversation-{interview['id']}",
                                    "platform_message_id": f"sync-message-{interview['id']}",
                                    "job_id": job["id"],
                                    "body": body,
                                }
                            ]
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        account = client.post(
            "/api/platform-accounts",
            json={"platform": "facebook", "display_name": "FB sync entry fixture", "status": "Configured", "config": {"base_url": "https://graph.example.test", "fixture_path": str(fixture)}, "capabilities": ["read_messages"]},
            headers=headers,
        ).json()
        with SessionLocal() as db:
            candidate_row = db.get(Candidate, candidate["id"])
            candidate_row.source_platform = "facebook"
            candidate_row.platform_id = f"sync-candidate-{interview['id']}"
            db.commit()
        task = client.post(
            "/api/queue/tasks",
            json={"queue_name": "platform-worker", "task_type": "platform.sync_messages", "payload": {"platform": "facebook", "account_id": account["id"]}, "idempotency_key": f"sync-entry-{interview['id']}", "max_retries": 1},
            headers=headers,
        ).json()
        ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
        assert ran.status_code == 200
    else:
        raise AssertionError(entry)
    return application, interview, conversation


def test_interview_replies_are_consumed_before_question_answers_across_entries(tmp_path):
    headers = auth_headers()
    cases = [
        ("api", "confirmed"),
        ("queue", "I cannot attend the interview"),
        ("sync", "please cancel the interview"),
    ]
    for entry, body in cases:
        application, interview, conversation = _make_interview_reply_case(headers, body=body, entry=entry, tmp_path=tmp_path)
        with SessionLocal() as db:
            question = db.scalar(select(CandidateQuestionAnswer).where(CandidateQuestionAnswer.application_id == application["id"]))
            interview_row = db.get(Interview, interview["id"])
            assert question.evidence_message_id is None
            assert question.answer_text is None
            assert interview_row.status in {"Confirmed", "Cancelled"}
            assert not db.execute(select(QueueTask).where(QueueTask.task_type.like("manual.initial_inquiry%"), QueueTask.payload["conversation_id"].as_integer() == conversation["id"])).scalars().all()


def test_interview_reschedule_and_uncertain_pause_tasks_without_answering():
    headers = auth_headers()
    for body, expected_status in [("please reschedule the interview", "Reschedule Requested"), ("interview maybe??", "Manual Review Required")]:
        application, interview, _conversation = _make_interview_reply_case(headers, body=body, entry="api")
        with SessionLocal() as db:
            question = db.scalar(select(CandidateQuestionAnswer).where(CandidateQuestionAnswer.application_id == application["id"]))
            interview_row = db.get(Interview, interview["id"])
            pending_tasks = db.execute(select(QueueTask).where(QueueTask.payload["interview_id"].as_integer() == interview["id"], QueueTask.task_type.in_(["flow.interview_reminder", "flow.interview_no_show_check"]))).scalars().all()
            assert question.evidence_message_id is None
            assert interview_row.status == expected_status
            assert all(task.status == "Cancelled" for task in pending_tasks)


def test_unrelated_interview_reply_still_flows_to_question_answer(monkeypatch):
    headers = auth_headers()

    def fake_question_ai(provider, prompt):
        active_key = json.loads(prompt)["active_question"]["key"]
        return (
            json.dumps({"decision": "answer_current_question", "question_key": active_key, "answer_text": "Taoyuan", "confidence": 0.94, "risk_flags": []}),
            {"input_tokens": 10, "output_tokens": 8, "actual": True},
        )

    monkeypatch.setattr("backend.app.services.ai.call_provider_sync", fake_question_ai)
    provider = client.post(
        "/api/ai/providers",
        json={"name": "question-ai-unrelated-interview", "provider_type": "openai_compatible", "base_url": "https://ai.test", "model": "question-model", "supports_text": True, "supports_json_schema": True},
        headers=headers,
    )
    client.post(
        "/api/ai/routes",
        json={"task_name": "RequiredQuestionAnswerExtraction", "primary_provider_id": provider.json()["id"], "fallback_provider_ids": [], "budget_policy": {"expected_json_schema": {"type": "object", "required": ["decision", "question_key", "answer_text", "confidence", "risk_flags"]}}},
        headers=headers,
    )
    application, interview, _conversation = _make_interview_reply_case(headers, body="Taoyuan", entry="api")
    with SessionLocal() as db:
        question = db.scalar(select(CandidateQuestionAnswer).where(CandidateQuestionAnswer.application_id == application["id"]))
        interview_row = db.get(Interview, interview["id"])
        assert question.answer_text == "Taoyuan"
        assert question.evidence_message_id is not None
        assert interview_row.status == "Scheduled"


def test_no_show_low_confidence_reply_cancels_without_no_response_message():
    headers = auth_headers()
    application, interview, conversation = _make_interview_reply_case(headers, body="interview maybe??", entry="api")
    with SessionLocal() as db:
        no_show = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_no_show_check", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        no_show.status = "Queued"
        no_show.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        db.commit()
        task_id = no_show.id
    ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Cancelled"
    with SessionLocal() as db:
        assert db.get(Interview, interview["id"]).status == "Manual Review Required"
        assert db.scalar(select(Message).where(Message.conversation_id == conversation["id"], Message.metadata_json["flow"].as_string() == "interview_no_show_30m")) is None
        assert db.scalar(select(SystemError).where(SystemError.feature == "interview_reply_classification")) is not None


def test_retention_filters_before_limit_and_continues_backlog():
    headers = auth_headers()
    old = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=500)
    with SessionLocal() as db:
        for idx in range(3):
            running = QueueTask(queue_name="x", task_type="x", payload={"idx": idx}, status="Running", idempotency_key=f"retention-running-{idx}")
            running.created_at = old - timedelta(minutes=idx)
            db.add(running)
        for idx in range(3):
            completed = QueueTask(queue_name="x", task_type="x", payload={"idx": idx}, status="Completed", idempotency_key=f"retention-completed-{idx}")
            completed.created_at = old - timedelta(hours=idx)
            db.add(completed)
        open_error = SystemError(service="test", feature="retention-open", severity="Warning", message="open", status="Open")
        open_error.created_at = old
        closed_error = SystemError(service="test", feature="retention-closed", severity="Warning", message="closed", status="Closed")
        closed_error.created_at = old
        db.add_all([open_error, closed_error])
        db.commit()
    task = client.post(
        "/api/queue/tasks",
        json={
            "queue_name": "privacy",
            "task_type": "privacy.retention_cleanup",
            "payload": {
                "batch_size": 2,
                "policies": [
                    {"name": "queue_completed_test", "model": "QueueTask", "days": 90, "strategy": "delete_completed"},
                    {"name": "system_error_closed_test", "model": "SystemError", "days": 90, "strategy": "delete_closed"},
                ],
            },
            "idempotency_key": "retention-filter-continuation-test",
            "max_retries": 1,
        },
        headers=headers,
    ).json()
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        assert db.scalar(select(QueueTask).where(QueueTask.idempotency_key == "retention-running-0")) is not None
        assert db.scalar(select(SystemError).where(SystemError.feature == "retention-open")) is not None
        assert db.scalar(select(SystemError).where(SystemError.feature == "retention-closed")) is None
        continuation = db.scalar(select(QueueTask).where(QueueTask.idempotency_key.like("retention-continuation:%")))
        assert continuation is not None
        run = db.scalar(select(DataRetentionRun).where(DataRetentionRun.policy_name == "queue_completed_test").order_by(DataRetentionRun.id.desc()))
        assert run.rows_scanned == 2
        assert run.details["backlog_remaining_estimate"] >= 1


def test_no_show_ignores_old_candidate_reply_before_interview():
    headers = auth_headers()
    job = client.get("/api/jobs").json()[0]
    with SessionLocal() as db:
        candidate_row = Candidate(name="Old Reply Candidate", phone="0919000002", source_platform="test")
        db.add(candidate_row)
        db.flush()
        conversation = Conversation(channel="line", candidate_id=candidate_row.id, job_id=job["id"], status="open")
        db.add(conversation)
        db.flush()
        old_message = Message(conversation_id=conversation.id, sender_type="candidate", body="confirmed", delivery_status="Delivered")
        old_message.created_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=1)
        db.add(old_message)
        db.commit()
        candidate_id = candidate_row.id
        conversation_id = conversation.id
    future = datetime.now(UTC) + timedelta(minutes=1)
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate_id, "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "reminder_policy": ["24h"]},
        headers=headers,
    ).json()
    with SessionLocal() as db:
        no_show = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_no_show_check", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        no_show.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        db.commit()
        task_id = no_show.id
    ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        assert row.status == "No Response Follow-up"
        assert db.scalar(select(Message).where(Message.conversation_id == conversation_id, Message.metadata_json["flow"].as_string() == "interview_no_show_30m")) is not None


def test_calendar_booking_is_idempotent_and_requires_two_available_interviewers():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[0]
    job = client.get("/api/jobs").json()[0]
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2027-01-10", "interview_time": "10:00"},
        headers=headers,
    ).json()
    config = {"provider": "mock", "interviewer_ids": ["hr1", "hr2"], "required_interviewer_count": 2, "timezone": "Asia/Taipei", "workdays": [0, 1, 2, 3, 4]}
    first = client.post("/api/calendar/book-interview", json={"interview_id": interview["id"], "calendar_config": config}, headers=headers)
    second = client.post("/api/calendar/book-interview", json={"interview_id": interview["id"], "calendar_config": config}, headers=headers)
    assert first.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    ran = client.post(f"/api/queue/tasks/{first.json()['id']}/run", headers=headers)
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        booking = db.scalar(select(CalendarBooking).where(CalendarBooking.interview_id == interview["id"]))
        assert booking is not None
        assert booking.status == "Booked"
        assert booking.read_back["mock"] is True

    interview2 = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2027-01-11", "interview_time": "10:00"},
        headers=headers,
    ).json()
    blocked = client.post("/api/calendar/book-interview", json={"interview_id": interview2["id"], "calendar_config": {"provider": "mock", "interviewer_ids": ["hr1"], "required_interviewer_count": 2}}, headers=headers).json()
    blocked_run = client.post(f"/api/queue/tasks/{blocked['id']}/run", headers=headers)
    assert blocked_run.json()["status"] == "Manual Review Required"


def test_voice_callback_low_confidence_and_replay_are_safe():
    import hashlib
    import hmac
    from backend.app.core.config import get_settings

    get_settings().phone_webhook_secret = "test-secret-key-that-is-not-default"
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[1]
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "phone", "display_name": "Mock phone", "status": "Configured", "config": {"provider": "mock", "dial_url": "mock://dial"}, "capabilities": ["dial"]},
        headers=headers,
    ).json()
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "phone", "task_type": "phone.dial", "payload": {"candidate_id": candidate["id"], "account_id": account["id"], "consent_disclosed": True, "script": "Screening call"}, "idempotency_key": "voice-low-confidence-test"},
        headers=headers,
    ).json()
    assert client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers).json()["status"] == "Completed"
    with SessionLocal() as db:
        call = db.scalar(select(PhoneCallLog).where(PhoneCallLog.external_call_id == f"mock-call-{task['id']}"))
        call.dial_status = "Completed"
        db.commit()
    body = json.dumps({"provider": "mock", "event_id": "voice-low-event", "call_id": f"mock-call-{task['id']}", "status": "Completed", "transcript": "maybe not sure"})
    ts = str(int(datetime.now(UTC).timestamp()))
    nonce = "voice-low-nonce"
    sig = "sha256=" + hmac.new(b"test-secret-key-that-is-not-default", f"{ts}.{nonce}.".encode("utf-8") + body.encode("utf-8"), hashlib.sha256).hexdigest()
    response = client.post("/api/webhooks/phone", content=body, headers={"content-type": "application/json", "x-phone-timestamp": ts, "x-phone-nonce": nonce, "x-phone-signature": sig})
    assert response.status_code == 200
    replay = client.post("/api/webhooks/phone", content=body, headers={"content-type": "application/json", "x-phone-timestamp": ts, "x-phone-nonce": nonce, "x-phone-signature": sig})
    assert replay.status_code == 200
    assert replay.json()["duplicate"] is True
    with SessionLocal() as db:
        call = db.scalar(select(PhoneCallLog).where(PhoneCallLog.external_call_id == f"mock-call-{task['id']}"))
        assert call.dial_status == "Human Review Required"


def test_phone_provider_request_body_uses_disclosed_script_and_requires_playback(monkeypatch):
    sent = {}

    class DummyResponse:
        status_code = 200
        text = "{}"

        def json(self):
            return {"call_id": "live-call-1", "status": "Completed", "transcript": "yes available", "disclosure_played": False}

    class DummyAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None, headers=None):
            sent["url"] = url
            sent["json"] = json
            sent["headers"] = headers
            return DummyResponse()

    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", DummyAsyncClient)
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[2]
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "phone", "display_name": "Live phone", "status": "Configured", "config": {"provider": "acmevoice", "dial_url": "https://phone.example.test/dial", "api_token": "secret"}, "capabilities": ["dial"]},
        headers=headers,
    ).json()
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "phone", "task_type": "phone.dial", "payload": {"candidate_id": candidate["id"], "account_id": account["id"], "company_name": "ACME", "consent_disclosed": True, "script": "請確認是否方便面試。"}, "idempotency_key": "phone-live-body-test", "max_retries": 1},
        headers=headers,
    ).json()
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.json()["status"] == "Completed"
    assert sent["json"]["script"].startswith("This is an automated recruiting call from ACME.")
    assert "請確認是否方便面試。" in sent["json"]["script"]
    with SessionLocal() as db:
        call = db.scalar(select(PhoneCallLog).where(PhoneCallLog.external_call_id == "live-call-1"))
        assert call.script == sent["json"]["script"]
        assert call.evidence["outbound_request"]["script"] == sent["json"]["script"]
        assert call.evidence["disclosure_played"] is False
        assert call.dial_status == "Human Review Required"


def test_calendar_readback_missing_is_unverified_not_booked():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[0]
    job = client.get("/api/jobs").json()[0]
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2027-01-12", "interview_time": "10:00"},
        headers=headers,
    ).json()
    config = {"provider": "mock", "interviewer_ids": ["hr3", "hr4"], "required_interviewer_count": 2, "mock_read_back_missing": True}
    task = client.post("/api/calendar/book-interview", json={"interview_id": interview["id"], "calendar_config": config}, headers=headers).json()
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.json()["status"] == "Manual Review Required"
    with SessionLocal() as db:
        booking = db.scalar(select(CalendarBooking).where(CalendarBooking.interview_id == interview["id"]))
        assert booking.status == "Unverified"
        assert booking.external_event_id is not None
        assert booking.read_back["verified"] is False


def test_calendar_rejects_inline_tokens_and_invalid_slot_policy():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[0]
    job = client.get("/api/jobs").json()[0]
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": "2027-01-13", "interview_time": "10:00"},
        headers=headers,
    ).json()
    token_response = client.post("/api/calendar/book-interview", json={"interview_id": interview["id"], "calendar_config": {"provider": "google", "access_token": "secret"}}, headers=headers)
    assert token_response.status_code == 400
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "calendar", "task_type": "calendar.book_interview", "payload": {"interview_id": interview["id"], "calendar_config": {"provider": "mock", "interviewer_ids": ["hr5", "hr6"], "slot_minutes": 0}}, "idempotency_key": "calendar-invalid-slot", "max_retries": 1},
        headers=headers,
    ).json()
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.json()["status"] == "Manual Review Required"
    assert "slot_minutes" in ran.json()["last_error"]


def test_google_freebusy_calendar_errors_fail_closed(monkeypatch):
    from backend.app.services.calendar import GoogleCalendarAdapter

    class DummyResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"calendars": {"hr@example.com": {"errors": [{"reason": "forbidden"}], "busy": []}}}

    import backend.app.services.calendar as calendar_service

    monkeypatch.setattr(calendar_service.httpx, "post", lambda *args, **kwargs: DummyResponse())
    adapter = GoogleCalendarAdapter({"access_token": "token"})
    try:
        adapter.free_busy(["hr@example.com"], datetime.now(UTC), datetime.now(UTC) + timedelta(hours=1))
    except RuntimeError as exc:
        assert "calendar errors" in str(exc)
    else:
        raise AssertionError("free_busy should fail closed on per-calendar errors")


def test_microsoft_graph_freebusy_maps_busy_slots(monkeypatch):
    from backend.app.services.calendar import MicrosoftGraphCalendarAdapter

    class DummyResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"value": [{"scheduleId": "hr@example.com", "scheduleItems": [{"status": "busy", "start": {"dateTime": "2027-01-01T01:00:00", "timeZone": "Asia/Taipei"}, "end": {"dateTime": "2027-01-01T02:00:00", "timeZone": "Asia/Taipei"}}]}]}

    import backend.app.services.calendar as calendar_service

    monkeypatch.setattr(calendar_service.httpx, "post", lambda *args, **kwargs: DummyResponse())
    adapter = MicrosoftGraphCalendarAdapter({"access_token": "token"})
    busy = adapter.free_busy(["hr@example.com"], datetime.now(UTC), datetime.now(UTC) + timedelta(hours=1))
    assert busy["hr@example.com"][0]["start"] == {"dateTime": "2027-01-01T01:00:00", "timeZone": "Asia/Taipei"}


def test_delete_candidate_preserves_and_runs_document_purge_task(tmp_path):
    from pypdf import PdfWriter

    headers = auth_headers()
    candidate = client.post("/api/candidates", json={"name": "Delete File Candidate", "source_platform": "manual"}, headers=headers).json()
    pdf_path = tmp_path / "delete-me.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with pdf_path.open("wb") as handle:
        writer.write(handle)
    with pdf_path.open("rb") as handle:
        upload = client.post(f"/api/resumes/ocr?candidate_id={candidate['id']}", files={"file": ("delete-me.pdf", handle, "application/pdf")}, headers=headers)
    assert upload.status_code == 200
    with SessionLocal() as db:
        document = db.scalar(select(StoredDocument).where(StoredDocument.candidate_id == candidate["id"]))
        stored_path = Path(document.storage_path)
        assert stored_path.exists()
    delete = client.delete(f"/api/candidates/{candidate['id']}", headers=headers)
    assert delete.status_code == 200
    with SessionLocal() as db:
        document = db.scalar(select(StoredDocument).where(StoredDocument.candidate_id == candidate["id"]))
        assert document.status == "Deletion Pending"
        assert document.deleted_at is None
        purge_task = db.scalar(select(QueueTask).where(QueueTask.task_type == "privacy.external_purge", QueueTask.payload["stored_document_id"].as_integer() == document.id))
        assert purge_task is not None
        assert purge_task.status == "Queued"
        assert purge_task.payload["purge_id"]
        purge_task_id = purge_task.id
    ran = client.post(f"/api/queue/tasks/{purge_task_id}/run", headers=headers)
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        document = db.scalar(select(StoredDocument).where(StoredDocument.candidate_id == candidate["id"]))
        assert document.status == "Deleted"
        assert document.deleted_at is not None
        assert document.delete_audit["status"] == "deleted"
        assert not stored_path.exists()


def test_deleted_document_ocr_cannot_recreate_resume_data(tmp_path):
    from pypdf import PdfWriter

    headers = auth_headers()
    candidate = client.post("/api/candidates", json={"name": "OCR Tombstone Candidate", "source_platform": "manual"}, headers=headers).json()
    pdf_path = tmp_path / "tombstone.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with pdf_path.open("wb") as handle:
        writer.write(handle)
    with pdf_path.open("rb") as handle:
        upload = client.post(f"/api/resumes/ocr?candidate_id={candidate['id']}", files={"file": ("tombstone.pdf", handle, "application/pdf")}, headers=headers)
    assert upload.status_code == 200
    client.delete(f"/api/candidates/{candidate['id']}", headers=headers)
    with SessionLocal() as db:
        document = db.scalar(select(StoredDocument).where(StoredDocument.candidate_id == candidate["id"]))
        task = QueueTask(queue_name="ocr", task_type="resume.ocr_pdf", payload={"document_id": document.id}, idempotency_key=f"ocr-after-delete:{document.id}", max_retries=1)
        db.add(task)
        db.commit()
        task_id = task.id
        before = db.scalar(select(__import__("sqlalchemy").func.count(Resume.id)).where(Resume.candidate_id == candidate["id"]))
    ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
    assert ran.json()["status"] == "Cancelled"
    with SessionLocal() as db:
        after = db.scalar(select(__import__("sqlalchemy").func.count(Resume.id)).where(Resume.candidate_id == candidate["id"]))
        assert after == before


def test_duplicate_pdf_upload_creates_distinct_document_records(tmp_path):
    from pypdf import PdfWriter

    headers = auth_headers()
    first_candidate = client.post("/api/candidates", json={"name": "Duplicate PDF A", "source_platform": "manual"}, headers=headers).json()
    second_candidate = client.post("/api/candidates", json={"name": "Duplicate PDF B", "source_platform": "manual"}, headers=headers).json()
    pdf_path = tmp_path / "same.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with pdf_path.open("wb") as handle:
        writer.write(handle)
    with pdf_path.open("rb") as handle:
        first = client.post(f"/api/resumes/ocr?candidate_id={first_candidate['id']}", files={"file": ("same.pdf", handle, "application/pdf")}, headers=headers)
    with pdf_path.open("rb") as handle:
        second = client.post(f"/api/resumes/ocr?candidate_id={second_candidate['id']}", files={"file": ("same.pdf", handle, "application/pdf")}, headers=headers)
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] != second.json()["id"]
    with SessionLocal() as db:
        documents = db.execute(select(StoredDocument).where(StoredDocument.file_name == "same.pdf")).scalars().all()
        candidate_ids = {item.candidate_id for item in documents}
        assert {first_candidate["id"], second_candidate["id"]}.issubset(candidate_ids)
        assert len({item.id for item in documents if item.candidate_id in candidate_ids}) >= 2


def test_tesseract_ocr_rasterizes_pdf_and_uses_tsv_confidence(tmp_path):
    from backend.app.services.ocr import extract_scanned_pdf_with_tesseract

    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF")
    calls = []

    def runner(args, timeout):
        calls.append(args)
        if args[0] == "pdftoppm":
            image = Path(args[-1]).with_name(Path(args[-1]).name + "-1.png")
            image.write_bytes(b"png")
            return subprocess.CompletedProcess(args, 0, "", "")
        tsv = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n5\t1\t1\t1\t1\t1\t0\t0\t10\t10\t91\t履歷\n5\t1\t1\t1\t1\t2\t0\t0\t10\t10\t87\tEngineer\n"
        return subprocess.CompletedProcess(args, 0, tsv, "")

    result = extract_scanned_pdf_with_tesseract(pdf, runner=runner)
    assert calls[0][0] == "pdftoppm"
    assert calls[1][0] == "tesseract"
    assert calls[1][-1] == "tsv"
    assert result.text == "履歷 Engineer"
    assert result.confidence == 0.89


def test_voice_negative_uncertain_and_strict_disclosure_handling():
    from backend.app.services.voice import classify_voice_transcript, disclosure_was_confirmed

    for phrase in ["not available", "I cannot confirm", "不可以", "可能可以"]:
        decision = classify_voice_transcript(phrase)
        assert decision.next_state == "Human Review Required"
        assert decision.intent != "confirm_interest"
    assert disclosure_was_confirmed({"disclosure_played": "false"}) is False
    assert disclosure_was_confirmed({"disclosure_played": False}) is False
    assert disclosure_was_confirmed({"disclosure_played": True}) is True


def test_phone_callback_dnc_request_records_dnc_and_preserves_prior_disclosure(monkeypatch):
    import hashlib
    import hmac
    from backend.app.core.config import get_settings

    get_settings().phone_webhook_secret = "test-secret-key-that-is-not-default"
    headers = auth_headers()
    candidate = client.post("/api/candidates", json={"name": "Phone DNC Candidate", "source_platform": "manual"}, headers=headers).json()
    with SessionLocal() as db:
        call = PhoneCallLog(candidate_id=candidate["id"], external_call_id="call-dnc-1", dial_status="Completed", evidence={"disclosure_played": True})
        db.add(call)
        db.commit()
    body = json.dumps({"provider": "mock", "event_id": "voice-dnc-event", "call_id": "call-dnc-1", "status": "Completed", "transcript": "do not call me again", "disclosure_played": "false"})
    ts = str(int(datetime.now(UTC).timestamp()))
    nonce = "voice-dnc-nonce"
    sig = "sha256=" + hmac.new(b"test-secret-key-that-is-not-default", f"{ts}.{nonce}.".encode("utf-8") + body.encode("utf-8"), hashlib.sha256).hexdigest()
    response = client.post("/api/webhooks/phone", content=body, headers={"content-type": "application/json", "x-phone-timestamp": ts, "x-phone-nonce": nonce, "x-phone-signature": sig})
    assert response.status_code == 200
    with SessionLocal() as db:
        assert db.scalar(select(DoNotContact).where(DoNotContact.candidate_id == candidate["id"], DoNotContact.status == "Active")) is not None
        call = db.scalar(select(PhoneCallLog).where(PhoneCallLog.external_call_id == "call-dnc-1"))
        assert call.evidence["disclosure_played"] is True
        assert call.dial_status == "Human Review Required"


def test_running_task_cannot_be_run_again_via_api():
    headers = auth_headers()
    task = client.post("/api/queue/tasks", json={"queue_name": "x", "task_type": "redis.disabled", "payload": {}, "idempotency_key": "running-api-guard"}, headers=headers).json()
    with SessionLocal() as db:
        row = db.get(QueueTask, task["id"])
        row.status = "Running"
        db.commit()
    response = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert response.status_code == 409


def test_confirmed_interview_still_no_shows_until_arrival_verified():
    headers = auth_headers()
    candidate = client.post("/api/candidates", json={"name": "Confirmed No Show Candidate", "source_platform": "manual"}, headers=headers).json()
    job = client.get("/api/jobs").json()[0]
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    future = datetime.now(UTC) + timedelta(minutes=10)
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "timezone": "UTC", "reminder_policy": ["24h"]},
        headers=headers,
    ).json()
    message = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "candidate", "body": "yes confirm"}, headers=headers)
    assert message.status_code == 200
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        row.status = "Confirmed"
        no_show = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_no_show_check", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        if not no_show:
            no_show = QueueTask(queue_name="flow", task_type="flow.interview_no_show_check", payload={"interview_id": interview["id"], "offset": "30m"}, idempotency_key=f"confirmed-no-show:{interview['id']}", scheduled_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z"), max_retries=1)
            db.add(no_show)
            db.flush()
        else:
            no_show.status = "Queued"
            no_show.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        task_id = no_show.id
        db.commit()
    ran = client.post(f"/api/queue/tasks/{task_id}/run", headers=headers)
    assert ran.json()["status"] == "Completed"
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        assert row.status == "No Response Follow-up"
        assert row.attendance_status == "Not Arrived"


def test_ocr_blank_pdf_goes_manual_and_payload_excludes_resume_text(tmp_path):
    from pypdf import PdfWriter

    headers = auth_headers()
    pdf_path = tmp_path / "blank.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with pdf_path.open("wb") as handle:
        writer.write(handle)
    with pdf_path.open("rb") as handle:
        response = client.post("/api/resumes/ocr", files={"file": ("blank.pdf", handle, "application/pdf")}, headers=headers)
    assert response.status_code == 200
    task = response.json()
    assert "raw_text" not in task["payload"]
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.json()["status"] == "Manual Review Required"


def test_redis_queue_outbox_and_atomic_claim_are_enabled():
    headers = auth_headers()
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "redis", "task_type": "message.verify", "payload": {"message_id": 999999, "platform": "line"}, "idempotency_key": "redis-outbox-test", "max_retries": 1},
        headers=headers,
    ).json()
    with SessionLocal() as db:
        outbox = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == task["id"]))
        assert outbox is not None
        row = db.get(QueueTask, task["id"])
        claimed = reserve_next_task(db, queue_names={"redis"}, worker_id="test-worker")
        assert claimed.id == row.id
        assert claimed.status == "Running"
        assert claimed.locked_by == "test-worker"
        assert claimed.fencing_token == 1
    blocked = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert blocked.status_code == 409


def test_atomic_claim_blocks_two_sessions_that_read_same_queued_task():
    with SessionLocal() as setup:
        task = QueueTask(queue_name="race", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="claim-race", max_retries=1)
        setup.add(task)
        setup.commit()
        task_id = task.id
    session_a = SessionLocal()
    session_b = SessionLocal()
    try:
        task_a = session_a.get(QueueTask, task_id)
        task_b = session_b.get(QueueTask, task_id)
        claimed_a = claim_task(session_a, task_a, worker_id="worker-a")
        claimed_b = claim_task(session_b, task_b, worker_id="worker-b")
        assert claimed_a is not None
        assert claimed_b is None
        with SessionLocal() as verify:
            row = verify.get(QueueTask, task_id)
            assert row.locked_by == "worker-a"
            assert row.fencing_token == 1
    finally:
        session_a.close()
        session_b.close()


def test_direct_queue_task_insert_gets_transactional_outbox():
    with SessionLocal() as db:
        task = QueueTask(queue_name="internal", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="direct-outbox", max_retries=1)
        db.add(task)
        db.commit()
        outbox = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == task.id))
        assert outbox is not None
        assert outbox.delivery_revision == task.delivery_revision
        assert outbox.payload["delivery_revision"] == task.delivery_revision


class FakeRedisStreams:
    def __init__(self):
        self.streams = {}
        self.groups = set()
        self.acked = []
        self.pending = {}
        self.counter = 0
        self.fail_xack_once = False
        self.fail_xrange_once = False
        self.kv = {}

    def xadd(self, stream, fields):
        self.counter += 1
        message_id = f"{self.counter}-0"
        self.streams.setdefault(stream, []).append((message_id, dict(fields)))
        return message_id

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, nx=False):
        if nx and key in self.kv:
            return False
        self.kv[key] = value
        return True

    def eval(self, _script, numkeys, *keys_and_args):
        assert numkeys == 2
        dlq_stream, index_key, *argv = keys_and_args
        indexed = self.kv.get(index_key)
        if indexed and indexed != "__pending__" and self.xrange(dlq_stream, min=indexed, max=indexed, count=1):
            return indexed
        fields = {str(argv[index]): str(argv[index + 1]) for index in range(0, len(argv), 2)}
        entry_id = self.xadd(dlq_stream, fields)
        self.kv[index_key] = entry_id
        return entry_id

    def xgroup_create(self, stream, group, id="0", mkstream=True):
        if mkstream:
            self.streams.setdefault(stream, [])
        key = (stream, group)
        if key in self.groups:
            from redis.exceptions import RedisError

            raise RedisError("BUSYGROUP Consumer Group name already exists")
        self.groups.add(key)

    def xreadgroup(self, group, consumer, streams, count=10, block=1000):
        response = []
        for stream in streams:
            pending_ids = {message_id for message_id, _consumer, _deliveries, _idle in self.pending.get((stream, group), [])}
            messages = [(message_id, fields) for message_id, fields in self.streams.get(stream, []) if message_id not in pending_ids][:count]
            if messages:
                bucket = self.pending.setdefault((stream, group), [])
                for message_id, _fields in messages:
                    bucket.append((message_id, consumer, 1, 0))
                response.append((stream, messages))
        return response

    def xack(self, stream, group, message_id):
        if self.fail_xack_once:
            self.fail_xack_once = False
            from redis.exceptions import RedisError

            raise RedisError("simulated ack failure")
        self.acked.append((stream, group, message_id))
        key = (stream, group)
        self.pending[key] = [item for item in self.pending.get(key, []) if item[0] != message_id]
        return 1

    def xpending_range(self, stream, group, min="-", max="+", count=50):
        rows = []
        for message_id, consumer, deliveries, idle in self.pending.get((stream, group), []):
            if min.startswith("(") and message_id <= min[1:]:
                continue
            rows.append(
                {
                    "message_id": message_id,
                    "consumer": consumer,
                    "time_since_delivered": idle,
                    "times_delivered": deliveries,
                }
            )
        return rows[:count]

    def xrange(self, stream, min="-", max="+", count=None):
        if self.fail_xrange_once:
            self.fail_xrange_once = False
            from redis.exceptions import RedisError

            raise RedisError("simulated xrange failure")
        rows = []
        exclusive_min = min[1:] if isinstance(min, str) and min.startswith("(") else None
        stream_rows = self.streams.get(stream, [])
        start_index = 0
        if exclusive_min:
            positions = [index for index, (message_id, _fields) in enumerate(stream_rows) if message_id == exclusive_min]
            start_index = positions[0] + 1 if positions else 0
        for message_id, fields in stream_rows[start_index:]:
            if not exclusive_min and min not in {"-", message_id}:
                continue
            if max not in {"+", message_id}:
                continue
            rows.append((message_id, fields))
        return rows[:count] if count else rows

    def xclaim(self, stream, group, consumer, min_idle_time, message_ids):
        key = (stream, group)
        claimed = []
        updated = []
        for message_id, old_consumer, deliveries, idle in self.pending.get(key, []):
            if message_id in message_ids and idle >= min_idle_time:
                fields = dict(self.xrange(stream, min=message_id, max=message_id, count=1)[0][1])
                claimed.append((message_id, fields))
                updated.append((message_id, consumer, deliveries + 1, 0))
            else:
                updated.append((message_id, old_consumer, deliveries, idle))
        self.pending[key] = updated
        return claimed

    def set_pending_idle(self, stream, group, message_id, idle_ms, deliveries=1):
        key = (stream, group)
        self.pending[key] = [
            (item_id, consumer, deliveries if item_id == message_id else item_deliveries, idle_ms if item_id == message_id else idle)
            for item_id, consumer, item_deliveries, idle in self.pending.get(key, [])
        ]


def test_redis_stream_consumer_claims_processes_and_acks_message():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-consume", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-consume", max_retries=1)
        db.add(task)
        db.commit()
        assert publish_pending_outbox(db, limit=1000, client=fake) >= 1
        processed = asyncio.run(consume_redis_once(db, streams=["recruitment:redis-consume"], worker_id="test-worker", client=fake, block_ms=1))
        assert processed == 1
        db.refresh(task)
        assert task.status == "Manual Review Required"
        assert fake.acked
        assert fake.streams["recruitment:redis-consume"]
        assert not fake.pending[("recruitment:redis-consume", "recruitment-workers")]


def test_redis_outbox_does_not_publish_future_tasks_until_due():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        future = (datetime.now(UTC) + timedelta(hours=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        task = QueueTask(queue_name="redis-future", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-future", max_retries=1, scheduled_at=future)
        db.add(task)
        db.commit()
        assert publish_pending_outbox(db, limit=1000, client=fake) == 0
        assert "recruitment:redis-future" not in fake.streams
        outbox = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == task.id))
        assert outbox.status == "Pending"


def test_redis_published_outbox_repairs_when_stream_entry_is_missing():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-loss", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-loss", max_retries=1)
        db.add(task)
        db.commit()
        assert publish_pending_outbox(db, limit=1000, client=fake) >= 1
        outbox = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == task.id))
        assert outbox.status == "Published"
        fake.streams["recruitment:redis-loss"] = []
        assert repair_lost_published_outbox(db, client=fake) >= 1
        db.refresh(outbox)
        db.refresh(task)
        assert outbox.status == "Retry"
        assert outbox.redis_stream_id is None
        assert task.redis_stream_id is None
        assert publish_pending_outbox(db, limit=1000, client=fake) >= 1
        assert fake.streams["recruitment:redis-loss"]


def test_redis_repair_scans_beyond_first_100_healthy_published_rows():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        tasks = [
            QueueTask(queue_name=f"redis-page-{i}", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key=f"redis-page-{i}", max_retries=1)
            for i in range(105)
        ]
        db.add_all(tasks)
        db.commit()
        assert publish_pending_outbox(db, limit=1000, client=fake) >= 105
        published = db.execute(select(QueueOutbox).where(QueueOutbox.status == "Published", QueueOutbox.redis_stream_id.is_not(None))).scalars().all()
        for row in published:
            fake.streams.setdefault(row.stream_name, [])
            if not any(message_id == row.redis_stream_id for message_id, _fields in fake.streams[row.stream_name]):
                fake.streams[row.stream_name].append((row.redis_stream_id, serialize_test_outbox(row)))
        target_task = tasks[-1]
        target_outbox = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == target_task.id))
        fake.streams[target_outbox.stream_name] = [(message_id, fields) for message_id, fields in fake.streams[target_outbox.stream_name] if message_id != target_outbox.redis_stream_id]
        assert repair_lost_published_outbox(db, limit=1, scan_limit=500, client=fake) == 1
        db.refresh(target_outbox)
        assert target_outbox.status == "Retry"
        assert target_outbox.redis_stream_id is None


def test_redis_repair_uses_persistent_cursor_for_1001_and_10001_positions():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        tasks = [
            QueueTask(queue_name=f"redis-cursor-{i}", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key=f"redis-cursor-{i}", max_retries=1)
            for i in range(10005)
        ]
        db.add_all(tasks)
        db.commit()
        assert publish_pending_outbox(db, limit=20000, client=fake) >= 10005
        published = db.execute(select(QueueOutbox).where(QueueOutbox.status == "Published", QueueOutbox.redis_stream_id.is_not(None))).scalars().all()
        for row in published:
            fake.streams.setdefault(row.stream_name, [])
            if not any(message_id == row.redis_stream_id for message_id, _fields in fake.streams[row.stream_name]):
                fake.streams[row.stream_name].append((row.redis_stream_id, serialize_test_outbox(row)))
        target_1001 = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == tasks[1000].id))
        target_10001 = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == tasks[10000].id))
        for target in [target_1001, target_10001]:
            fake.streams[target.stream_name] = [(message_id, fields) for message_id, fields in fake.streams[target.stream_name] if message_id != target.redis_stream_id]
            cursor = db.get(QueueRecoveryCursor, "redis_published_outbox_loss") or QueueRecoveryCursor(name="redis_published_outbox_loss", cursor_id=0, metadata_json={})
            cursor.cursor_id = target.id - 1
            db.add(cursor)
            db.commit()
            assert repair_lost_published_outbox(db, limit=1, scan_limit=5, client=fake) == 1
            db.refresh(target)
            assert target.status == "Retry"
            assert target.redis_stream_id is None


def test_redis_repair_scans_from_zero_to_10001st_row():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        tasks = [
            QueueTask(queue_name=f"redis-zero-{i}", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key=f"redis-zero-{i}", max_retries=1)
            for i in range(10001)
        ]
        db.add_all(tasks)
        db.commit()
        assert publish_pending_outbox(db, limit=20000, client=fake) >= 10001
        published = db.execute(select(QueueOutbox).where(QueueOutbox.status == "Published", QueueOutbox.redis_stream_id.is_not(None))).scalars().all()
        for row in published:
            fake.streams.setdefault(row.stream_name, [])
            if not any(message_id == row.redis_stream_id for message_id, _fields in fake.streams[row.stream_name]):
                fake.streams[row.stream_name].append((row.redis_stream_id, serialize_test_outbox(row)))
        target = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == tasks[10000].id))
        db.merge(QueueRecoveryCursor(name="redis_published_outbox_loss", cursor_id=target.id - 10001, metadata_json={}))
        db.commit()
        fake.streams[target.stream_name] = [(message_id, fields) for message_id, fields in fake.streams[target.stream_name] if message_id != target.redis_stream_id]
        assert repair_lost_published_outbox(db, limit=1, scan_limit=10001, client=fake) == 1
        db.refresh(target)
        assert target.status == "Retry"
        cursor = db.get(QueueRecoveryCursor, "redis_published_outbox_loss")
        assert cursor.cursor_id >= target.id


def test_redis_repair_wraps_cursor_to_find_low_id_loss():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-wrap-seed", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key=f"redis-wrap-seed-{datetime.now(UTC).timestamp()}", max_retries=1)
        db.add(task)
        db.commit()
        assert publish_pending_outbox(db, limit=100, client=fake) >= 1
        target = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == task.id))
        fake.streams[target.stream_name] = []
        cursor = QueueRecoveryCursor(name="redis_published_outbox_loss", cursor_id=target.id - 1, metadata_json={})
        db.merge(cursor)
        db.commit()
        assert repair_lost_published_outbox(db, limit=1, scan_limit=10, client=fake) == 1
        db.refresh(target)
        assert target.status == "Retry"
        cursor = db.get(QueueRecoveryCursor, "redis_published_outbox_loss")
        assert cursor.cursor_id >= target.id


def test_multiple_workers_compete_for_one_task_with_single_claim():
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-compete", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-compete", max_retries=1)
        db.add(task)
        db.commit()
        first = claim_task(db, task, worker_id="worker-a")
        assert first is not None
    with SessionLocal() as db:
        task = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == "redis-compete"))
        second = claim_task(db, task, worker_id="worker-b")
        assert second is None
        db.refresh(task)
        assert task.locked_by == "worker-a"


def test_replay_task_does_not_overwrite_concurrent_running_claim():
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-replay-race", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-replay-race", status="Retry", max_retries=3)
        db.add(task)
        db.commit()
        task_id = task.id
    session_a = SessionLocal()
    session_b = SessionLocal()
    try:
        stale = session_a.get(QueueTask, task_id)
        claimed = claim_task(session_b, session_b.get(QueueTask, task_id), worker_id="race-worker")
        assert claimed is not None
        try:
            replay_task(session_a, stale.id)
        except ValueError as exc:
            assert "Running" in str(exc) or "cannot be replayed" in str(exc)
        else:
            raise AssertionError("Replay should not overwrite a concurrent Running claim")
    finally:
        session_a.close()
        session_b.close()
    with SessionLocal() as verify:
        row = verify.get(QueueTask, task_id)
        assert row.status == "Running"
        assert row.locked_by == "race-worker"
        assert row.delivery_revision == 1


def test_redis_repair_cursor_survives_restart_session():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        tasks = [
            QueueTask(queue_name=f"redis-restart-{i}", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key=f"redis-restart-{i}", max_retries=1)
            for i in range(8)
        ]
        db.add_all(tasks)
        db.commit()
        assert publish_pending_outbox(db, limit=100, client=fake) >= 8
        published = db.execute(select(QueueOutbox).where(QueueOutbox.status == "Published", QueueOutbox.redis_stream_id.is_not(None))).scalars().all()
        for row in published:
            fake.streams.setdefault(row.stream_name, [])
            if not any(message_id == row.redis_stream_id for message_id, _fields in fake.streams[row.stream_name]):
                fake.streams[row.stream_name].append((row.redis_stream_id, serialize_test_outbox(row)))
        target = db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == tasks[6].id))
        db.merge(QueueRecoveryCursor(name="redis_published_outbox_loss", cursor_id=target.id - 1, metadata_json={}))
        db.commit()
        fake.streams[target.stream_name] = [(message_id, fields) for message_id, fields in fake.streams[target.stream_name] if message_id != target.redis_stream_id]
        assert repair_lost_published_outbox(db, limit=1, scan_limit=7, client=fake) == 1
        cursor_id = db.get(QueueRecoveryCursor, "redis_published_outbox_loss").cursor_id
    with SessionLocal() as restarted_db:
        restarted_cursor = restarted_db.get(QueueRecoveryCursor, "redis_published_outbox_loss")
        assert restarted_cursor.cursor_id == cursor_id


def serialize_test_outbox(row: QueueOutbox) -> dict:
    return {"queue_task_id": str(row.queue_task_id), "delivery_revision": str(row.delivery_revision or 1), "payload": json.dumps(row.payload or {})}


def test_renew_once_returns_true_for_owned_running_task():
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-renew", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-renew", max_retries=1)
        db.add(task)
        db.commit()
        claimed = claim_task(db, task, worker_id="renew-worker")
        SessionForHeartbeat = sessionmaker(bind=db.get_bind(), autoflush=False, autocommit=False)
        assert _renew_once(SessionForHeartbeat, claimed.id, "renew-worker", int(claimed.fencing_token or 0)) is True


def test_heartbeat_does_not_mark_ownership_lost_after_normal_completion():
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-heartbeat-terminal", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-heartbeat-terminal", max_retries=1)
        db.add(task)
        db.commit()
        claimed = claim_task(db, task, worker_id="heartbeat-worker")
        claimed.status = "Completed"
        claimed.locked_by = None
        claimed.lease_expires_at = None
        db.commit()
        stop_event = asyncio.Event()
        ownership_lost = asyncio.Event()
        asyncio.run(_heartbeat_task(db, task_id=claimed.id, worker_id="heartbeat-worker", fencing_token=int(claimed.fencing_token or 0), stop_event=stop_event, ownership_lost=ownership_lost))
        assert not ownership_lost.is_set()


def test_heartbeat_marks_ownership_lost_from_last_success_without_db_probe(monkeypatch):
    calls = {"terminal": 0}

    def failing_renew(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    def forbidden_terminal_probe(*_args, **_kwargs):
        calls["terminal"] += 1
        raise AssertionError("terminal DB probe should not run after renewal exceptions")

    monkeypatch.setattr(redis_queue_service, "LEASE_SECONDS", 1)
    monkeypatch.setattr(redis_queue_service, "_renew_once", failing_renew)
    monkeypatch.setattr(redis_queue_service, "_task_is_terminal", forbidden_terminal_probe)
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-heartbeat-db-down", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-heartbeat-db-down", max_retries=1)
        db.add(task)
        db.commit()
        claimed = claim_task(db, task, worker_id="heartbeat-db-down-worker")
        stop_event = asyncio.Event()
        ownership_lost = asyncio.Event()
        asyncio.run(
            _heartbeat_task(
                db,
                task_id=claimed.id,
                worker_id="heartbeat-db-down-worker",
                fencing_token=int(claimed.fencing_token or 0),
                stop_event=stop_event,
                ownership_lost=ownership_lost,
            )
        )
        assert ownership_lost.is_set()
        assert calls["terminal"] == 0


def test_redis_configured_streams_respects_worker_queue_filter():
    with SessionLocal() as db:
        owned = QueueTask(queue_name="owned-worker", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="owned-worker-filter", max_retries=1)
        other = QueueTask(queue_name="other-worker", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="other-worker-filter", max_retries=1)
        db.add_all([owned, other])
        db.commit()
        streams = configured_streams(db, {"owned-worker"})
        assert "recruitment:owned-worker" in streams
        assert "recruitment:other-worker" not in streams


def test_redis_configured_streams_includes_more_than_500_and_warns():
    with SessionLocal() as db:
        prefix = "bulk-stream-test"
        for idx in range(505):
            db.add(
                QueueTask(
                    queue_name=f"{prefix}-{idx}",
                    task_type="message.verify",
                    payload={"message_id": 999999, "platform": "line"},
                    idempotency_key=f"{prefix}-{idx}",
                    status="Queued",
                    max_retries=1,
                )
            )
        db.commit()
        streams = configured_streams(db)
        assert f"recruitment:{prefix}-0" in streams
        assert f"recruitment:{prefix}-504" in streams
        assert len([stream for stream in streams if stream.startswith(f"recruitment:{prefix}-")]) == 505
        warning = db.scalar(select(SystemError).where(SystemError.feature == "configured_streams").order_by(SystemError.id.desc()))
        assert warning is not None
        discovered = int(warning.message.split("discovered ", 1)[1].split(" ", 1)[0])
        assert discovered >= 505


def test_replay_task_requires_expected_delivery_revision_and_is_idempotent():
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-replay-cas", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-replay-cas", status="Retry", delivery_revision=4, max_retries=3)
        db.add(task)
        db.commit()
        try:
            replay_task(db, task.id, expected_delivery_revision=3, replay_request_key="request-a")
        except ValueError as exc:
            assert "does not match expected" in str(exc)
        else:
            raise AssertionError("stale expected delivery_revision should block replay")
        db.refresh(task)
        assert task.delivery_revision == 4
        replayed = replay_task(db, task.id, expected_delivery_revision=4, replay_request_key="request-b")
        assert replayed.delivery_revision == 5
        again = replay_task(db, task.id, expected_delivery_revision=4, replay_request_key="request-b")
        assert again.delivery_revision == 5
        assert (again.payload or {})["last_replay_request_key"] == "request-b"
        record = db.scalar(select(QueueReplayRecord).where(QueueReplayRecord.replay_request_key == "request-b"))
        assert record is not None
        assert record.status == "Succeeded"
        assert record.replayed_delivery_revision == 5
        assert record.replayed_outbox_id is not None
        task.retry_count = task.max_retries
        db.commit()
        exhausted_retry = replay_task(db, task.id, expected_delivery_revision=4, replay_request_key="request-b")
        assert exhausted_retry.delivery_revision == 5
        try:
            replay_task(db, task.id, expected_delivery_revision=4, replay_request_key="request-b", dlq_record_id=999)
        except ValueError as exc:
            assert "different DLQ" in str(exc)
        else:
            raise AssertionError("reusing a successful replay key with different parameters must fail")


def test_replay_api_accepts_expected_revision_and_persistent_request_key():
    headers = auth_headers()
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "redis-api-replay-cas", "task_type": "message.verify", "payload": {"message_id": 999999, "platform": "line"}, "idempotency_key": "redis-api-replay-cas", "max_retries": 3},
        headers=headers,
    ).json()
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers).json()
    revision = ran["delivery_revision"]
    replay_body = {"expected_delivery_revision": revision, "replay_request_key": "api-replay-key-1"}
    missing = client.post(f"/api/queue/tasks/{task['id']}/replay", headers=headers)
    assert missing.status_code == 422
    bad_key = client.post(f"/api/queue/tasks/{task['id']}/replay", json={"expected_delivery_revision": revision, "replay_request_key": "short"}, headers=headers)
    assert bad_key.status_code == 422
    bad_revision = client.post(f"/api/queue/tasks/{task['id']}/replay", json={"expected_delivery_revision": 0, "replay_request_key": "api-replay-key-bad"}, headers=headers)
    assert bad_revision.status_code == 422
    first = client.post(f"/api/queue/tasks/{task['id']}/replay", json=replay_body, headers=headers)
    assert first.status_code == 200
    second = client.post(f"/api/queue/tasks/{task['id']}/replay", json=replay_body, headers=headers)
    assert second.status_code == 200
    assert second.json()["delivery_revision"] == first.json()["delivery_revision"]
    with SessionLocal() as db:
        records = db.execute(select(QueueReplayRecord).where(QueueReplayRecord.replay_request_key == "api-replay-key-1")).scalars().all()
        assert len(records) == 1
        outboxes = db.execute(select(QueueOutbox).where(QueueOutbox.queue_task_id == task["id"], QueueOutbox.delivery_revision == first.json()["delivery_revision"])).scalars().all()
        assert len(outboxes) == 1


def test_dlq_replay_in_progress_requires_matching_replay_request_key():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        unrelated = QueueTask(queue_name="redis-replay-proof", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-replay-proof-unrelated", status="Retry", delivery_revision=2)
        related = QueueTask(queue_name="redis-replay-proof", task_type="message.verify", payload={"message_id": 999999, "platform": "line", "last_replay_request_key": "placeholder"}, idempotency_key="redis-replay-proof-related", status="Retry", delivery_revision=2)
        db.add_all([unrelated, related])
        db.flush()
        unrelated_record = QueueDlqRecord(stream_name="recruitment:proof", redis_stream_id="proof-1", queue_task_id=unrelated.id, delivery_revision=1, reason="unrelated bump", status="Replay In Progress", replay_started_at=utc_now())
        related_record = QueueDlqRecord(stream_name="recruitment:proof", redis_stream_id="proof-2", queue_task_id=related.id, delivery_revision=1, reason="related bump", status="Replay In Progress", replay_started_at=utc_now())
        db.add_all([unrelated_record, related_record])
        db.flush()
        db.add(
            QueueReplayRecord(
                replay_request_key=f"dlq-replay:{related_record.id}:1",
                queue_task_id=related.id,
                dlq_record_id=related_record.id,
                expected_delivery_revision=1,
                replayed_delivery_revision=2,
                status="Succeeded",
                metadata_json={},
            )
        )
        db.commit()
        assert reconcile_dlq_records(db, client=fake, replay_timeout_seconds=3600) == 1
        db.refresh(unrelated_record)
        db.refresh(related_record)
        assert unrelated_record.status == "Replay In Progress"
        assert related_record.status == "Replayed"


def test_structured_blacklist_job_scope_is_exact_not_substring():
    with SessionLocal() as db:
        candidate = Candidate(name="Scoped Blacklist", phone="0911999000", email="scoped@example.test", source_platform="manual")
        db.add(candidate)
        db.flush()
        db.add(RecruitmentBlacklist(candidate_id=candidate.id, reason="blocked for job 10", rule_code="job:10", scope_type="job", scope_id="10"))
        db.commit()
        assert has_active_recruitment_blacklist(db, candidate, job_id=10) is True
        assert has_active_recruitment_blacklist(db, candidate, job_id=1) is False


def test_recruitment_blacklist_api_persists_and_validates_scope():
    headers = auth_headers()
    candidate = client.post("/api/candidates", json={"name": "Scoped API Blacklist", "phone": "0911777666", "email": "scope-api@example.test", "source_platform": "manual"}, headers=headers).json()
    job = client.get("/api/jobs", headers=headers).json()[0]
    created = client.post(
        "/api/recruitment-blacklist",
        json={"candidate_id": candidate["id"], "reason": "job scoped via API", "rule_code": f"job:{job['id']}", "scope_type": "job", "scope_id": str(job["id"])},
        headers=headers,
    )
    assert created.status_code == 200
    assert created.json()["scope_type"] == "job"
    assert created.json()["scope_id"] == str(job["id"])
    padded = client.post(
        "/api/recruitment-blacklist",
        json={"candidate_id": candidate["id"], "reason": "job scoped via padded API", "rule_code": f"job:{job['id']}", "scope_type": "job", "scope_id": f" 0{job['id']} "},
        headers=headers,
    )
    assert padded.status_code == 200
    assert padded.json()["scope_id"] == str(job["id"])
    blank = client.post(
        "/api/recruitment-blacklist",
        json={"candidate_id": candidate["id"], "reason": "blank scope", "scope_type": "job", "scope_id": "   "},
        headers=headers,
    )
    assert blank.status_code == 400
    illegal = client.post(
        "/api/recruitment-blacklist",
        json={"candidate_id": candidate["id"], "reason": "illegal scope", "scope_type": "job", "scope_id": "job-abc"},
        headers=headers,
    )
    assert illegal.status_code == 400
    invalid = client.post(
        "/api/recruitment-blacklist",
        json={"candidate_id": candidate["id"], "reason": "bad scope", "scope_type": "job", "scope_id": "999999"},
        headers=headers,
    )
    assert invalid.status_code == 404
    bad_combo = client.post(
        "/api/recruitment-blacklist",
        json={"candidate_id": candidate["id"], "reason": "bad combo", "scope_type": "candidate", "scope_id": str(job["id"])},
        headers=headers,
    )
    assert bad_combo.status_code == 400


def test_private_reply_runs_shared_preflight_for_blacklist(tmp_path):
    headers = auth_headers()
    fixture = tmp_path / "fb-private-preflight.json"
    fixture.write_text(json.dumps({"private_reply": {"ok": True, "status": "Sent", "message": "sent", "data": {"platform_message_id": "should-not-send"}}}), encoding="utf-8")
    account = client.post(
        "/api/platform-accounts",
        json={"platform": "facebook", "display_name": "FB private preflight", "status": "Configured", "config": {"fixture_path": str(fixture)}, "capabilities": ["send_message"]},
        headers=headers,
    ).json()
    candidate = client.post("/api/candidates", json={"name": "FB Blocked", "phone": "0911888777", "email": "fb-blocked@example.test", "source_platform": "facebook"}, headers=headers).json()
    job = client.get("/api/jobs", headers=headers).json()[0]
    conversation = client.post("/api/conversations", json={"channel": "facebook", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    message = client.post("/api/messages", json={"conversation_id": conversation["id"], "sender_type": "system", "body": "private reply body"}, headers=headers).json()
    with SessionLocal() as db:
        db.add(RecruitmentBlacklist(candidate_id=candidate["id"], reason="job scoped block", rule_code=f"job:{job['id']}", scope_type="job", scope_id=str(job["id"])))
        db.commit()
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "messaging", "task_type": "message.private_reply", "payload": {"message_id": message["id"], "platform": "facebook", "comment_id": "comment-1", "account_id": account["id"]}, "idempotency_key": "fb-private-preflight", "max_retries": 1},
        headers=headers,
    ).json()
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Manual Review Required"
    assert "RecruitmentBlacklist blocks" in ran.json()["last_error"]


def test_redis_reclaim_pending_message_executes_claimed_message():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-reclaim", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-reclaim", max_retries=1)
        db.add(task)
        db.commit()
        assert publish_pending_outbox(db, limit=1000, client=fake) >= 1
        fake.xgroup_create("recruitment:redis-reclaim", "recruitment-workers", id="0", mkstream=True)
        fake.xreadgroup("recruitment-workers", "dead-worker", {"recruitment:redis-reclaim": ">"}, count=1, block=1)
        message_id = fake.streams["recruitment:redis-reclaim"][0][0]
        fake.set_pending_idle("recruitment:redis-reclaim", "recruitment-workers", message_id, idle_ms=120_000)
        reclaimed = reclaim_pending_messages(db, streams=["recruitment:redis-reclaim"], worker_id="live-worker", client=fake, idle_ms=60_000)
        assert reclaimed == 1
        db.refresh(task)
        assert task.status == "Manual Review Required"
        assert fake.acked[-1] == ("recruitment:redis-reclaim", "recruitment-workers", message_id)


def test_queue_operations_api_exposes_outbox_dlq_metrics_and_guarded_replay():
    headers = auth_headers()
    task = client.post(
        "/api/queue/tasks",
        json={"queue_name": "redis-api", "task_type": "message.verify", "payload": {"message_id": 999999, "platform": "line"}, "idempotency_key": "redis-api-replay", "max_retries": 2},
        headers=headers,
    ).json()
    ran = client.post(f"/api/queue/tasks/{task['id']}/run", headers=headers)
    assert ran.status_code == 200
    assert ran.json()["status"] == "Retry"
    replayed = client.post(
        f"/api/queue/tasks/{task['id']}/replay",
        json={"expected_delivery_revision": ran.json()["delivery_revision"], "replay_request_key": "ops-replay-key-1"},
        headers=headers,
    )
    assert replayed.status_code == 200
    assert replayed.json()["status"] == "Retry"
    assert replayed.json()["delivery_revision"] > ran.json()["delivery_revision"]
    with SessionLocal() as db:
        completed = QueueTask(queue_name="redis-api", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-api-completed", status="Completed")
        dlq_task = QueueTask(queue_name="redis-api", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-api-dlq-task", status="Retry")
        db.add(completed)
        db.add(dlq_task)
        db.flush()
        dlq_record = QueueDlqRecord(
            stream_name="recruitment:redis-api",
            redis_stream_id="99-0",
            queue_task_id=dlq_task.id,
            delivery_revision=dlq_task.delivery_revision,
            reason="dlq-test",
            metadata_json={"source": "test"},
        )
        db.add(dlq_record)
        db.add(SystemError(service="RedisQueue", feature="worker_loop", severity="Warning", message="not-dlq", impact="Redis worker backed off and will retry instead of exiting."))
        db.add(SystemError(service="RedisQueue", feature="recruitment:redis-api", severity="Error", message="dlq-test", impact="Message moved to DLQ after repeated failed delivery."))
        db.commit()
        completed_id = completed.id
        dlq_record_id = dlq_record.id
    blocked = client.post(
        f"/api/queue/tasks/{completed_id}/replay",
        json={"expected_delivery_revision": 1, "replay_request_key": "ops-replay-completed-1"},
        headers=headers,
    )
    assert blocked.status_code == 409
    assert client.get("/api/queue/outbox", headers=headers).status_code == 200
    dlq = client.get("/api/queue/dlq", headers=headers)
    assert dlq.status_code == 200
    assert dlq.json()["total"] >= 1
    assert any(row["reason"] == "dlq-test" and row["stream_name"] == "recruitment:redis-api" for row in dlq.json()["items"])
    assert not any(row.get("reason") == "not-dlq" for row in dlq.json()["items"])
    replayed_dlq = client.post(f"/api/queue/dlq/{dlq_record_id}/replay", headers=headers)
    assert replayed_dlq.status_code == 200
    assert replayed_dlq.json()["status"] == "Replayed"
    assert replayed_dlq.json()["replayed_task_id"] is not None
    metrics = client.get("/api/queue/redis/metrics", headers=headers)
    assert metrics.status_code == 200
    assert "due_backlog" in metrics.json()
    assert metrics.json()["dlq_total"] >= 1


def test_dlq_api_uses_records_not_system_error_impact_and_requires_manager():
    with SessionLocal() as db:
        db.add(User(email="hr-dlq@example.com", name="HR DLQ", role="Recruiter / HR", password_hash=hash_password("hrpass1234")))
        db.add(SystemError(service="RedisQueue", feature="recruitment:ghost", severity="Error", message="legacy-only", impact="Message moved to DLQ after repeated failed delivery."))
        db.add(QueueDlqRecord(stream_name="recruitment:records", redis_stream_id="1-0", reason="record-source", metadata_json={}))
        db.commit()
    dlq = client.get("/api/queue/dlq", headers=auth_headers())
    assert dlq.status_code == 200
    reasons = [row["reason"] for row in dlq.json()["items"]]
    assert "record-source" in reasons
    assert "legacy-only" not in reasons
    hr_client = TestClient(app)
    assert hr_client.post("/api/auth/login", json={"email": "hr-dlq@example.com", "password": "hrpass1234"}).status_code == 200
    assert hr_client.get("/api/queue/dlq").status_code == 403


def test_redis_move_to_dlq_persists_queryable_record():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        fake.xadd("recruitment:broken", {"payload": "{not-json", "queue_task_id": "bad", "delivery_revision": "bad"})
        processed = asyncio.run(consume_redis_once(db, streams=["recruitment:broken"], worker_id="test-worker", client=fake, block_ms=1))
        assert processed == 1
        record = db.scalar(select(QueueDlqRecord).where(QueueDlqRecord.stream_name == "recruitment:broken"))
        assert record is not None
        assert record.redis_stream_id == "1-0"
        assert record.dlq_stream_id == "2-0"
        assert "not valid JSON" in record.reason
        assert fake.acked[-1] == ("recruitment:broken", "recruitment-workers", "1-0")


def test_redis_move_to_dlq_preserves_missing_task_and_outbox_ids_without_invalid_fk():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        fake.xadd(
            "recruitment:missing",
            {
                "payload": json.dumps({"queue_task_id": 999999, "delivery_revision": 3}),
                "queue_task_id": "999999",
                "outbox_id": "888888",
                "delivery_revision": "3",
            },
        )
        processed = asyncio.run(consume_redis_once(db, streams=["recruitment:missing"], worker_id="test-worker", client=fake, block_ms=1))
        assert processed == 1
        record = db.scalar(select(QueueDlqRecord).where(QueueDlqRecord.stream_name == "recruitment:missing"))
        assert record.queue_task_id is None
        assert record.outbox_id is None
        assert record.delivery_revision == 3
        assert record.metadata_json["invalid_queue_task_id"] == "999999"
        assert record.metadata_json["invalid_outbox_id"] == "888888"
        assert record.ack_status == "Acked"


def test_redis_move_to_dlq_ack_failure_recovers_without_duplicate_dlq_entry():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        fake.xadd("recruitment:ack-fail", {"payload": "{not-json", "queue_task_id": "bad"})
        fake.fail_xack_once = True
        try:
            asyncio.run(consume_redis_once(db, streams=["recruitment:ack-fail"], worker_id="test-worker", client=fake, block_ms=1))
        except Exception:
            pass
        record = db.scalar(select(QueueDlqRecord).where(QueueDlqRecord.stream_name == "recruitment:ack-fail"))
        assert record.status == "Ack Pending"
        assert record.ack_status == "Failed"
        assert len(fake.streams["recruitment:ack-fail:dlq"]) == 1
        fake.set_pending_idle("recruitment:ack-fail", "recruitment-workers", "1-0", idle_ms=120_000)
        processed = reclaim_pending_messages(db, streams=["recruitment:ack-fail"], worker_id="test-worker-2", client=fake, idle_ms=60_000)
        assert processed == 1
        db.refresh(record)
        assert record.status == "Open"
        assert record.ack_status == "Acked"
        assert len(fake.streams["recruitment:ack-fail:dlq"]) == 1


def test_dlq_index_finds_existing_deep_entry_without_scan_or_duplicate_xadd():
    for filler_count in [500, 10000]:
        fake = FakeRedisStreams()
        stream = f"recruitment:deep-{filler_count}"
        with SessionLocal() as db:
            source_message_id = fake.xadd(stream, {"payload": "{not-json", "queue_task_id": "bad"})
            for index in range(filler_count):
                fake.xadd(f"{stream}:dlq", {"failed_stream": "other", "failed_message_id": f"{index}-0"})
            existing_dlq_id = fake.xadd(f"{stream}:dlq", {"failed_stream": stream, "failed_message_id": source_message_id, "dlq_reason": "previous"})
            fake.set(redis_queue_service._dlq_index_key(stream, source_message_id), existing_dlq_id)
            before = len(fake.streams[f"{stream}:dlq"])
            processed = asyncio.run(consume_redis_once(db, streams=[stream], worker_id=f"deep-worker-{filler_count}", client=fake, block_ms=1))
            assert processed == 1
            record = db.scalar(select(QueueDlqRecord).where(QueueDlqRecord.stream_name == stream))
            assert record.dlq_stream_id == existing_dlq_id
            assert len(fake.streams[f"{stream}:dlq"]) == before


def test_dlq_scan_disconnect_recovers_on_reclaim_without_duplicate_xadd():
    fake = FakeRedisStreams()
    stream = "recruitment:scan-disconnect"
    with SessionLocal() as db:
        source_message_id = fake.xadd(stream, {"payload": "{not-json", "queue_task_id": "bad"})
        for index in range(600):
            fake.xadd(f"{stream}:dlq", {"failed_stream": "other", "failed_message_id": f"{index}-0"})
        existing_dlq_id = fake.xadd(f"{stream}:dlq", {"failed_stream": stream, "failed_message_id": source_message_id, "dlq_reason": "previous"})
        fake.set(redis_queue_service._dlq_index_key(stream, source_message_id), existing_dlq_id)
        fake.fail_xrange_once = True
        try:
            asyncio.run(consume_redis_once(db, streams=[stream], worker_id="scan-worker", client=fake, block_ms=1))
        except Exception:
            pass
        record = db.scalar(select(QueueDlqRecord).where(QueueDlqRecord.stream_name == stream))
        assert record.status == "Redis Failed"
        before = len(fake.streams[f"{stream}:dlq"])
        fake.set_pending_idle(stream, "recruitment-workers", source_message_id, idle_ms=120_000)
        repaired = reclaim_pending_messages(db, streams=[stream], worker_id="scan-worker-2", client=fake, idle_ms=60_000)
        assert repaired == 1
        db.refresh(record)
        assert record.status == "Open"
        assert record.dlq_stream_id == existing_dlq_id
        assert len(fake.streams[f"{stream}:dlq"]) == before


def test_dlq_stale_index_or_trimmed_pointer_rewrites_before_ack():
    fake = FakeRedisStreams()
    stream = "recruitment:stale-dlq-pointer"
    with SessionLocal() as db:
        source_message_id = fake.xadd(stream, {"payload": "{not-json", "queue_task_id": "bad"})
        stale_dlq_id = fake.xadd(f"{stream}:dlq", {"failed_stream": stream, "failed_message_id": source_message_id, "dlq_reason": "trimmed"})
        fake.streams[f"{stream}:dlq"] = []
        fake.set(redis_queue_service._dlq_index_key(stream, source_message_id), stale_dlq_id)
        record = QueueDlqRecord(
            stream_name=stream,
            redis_stream_id=source_message_id,
            dlq_stream_id=stale_dlq_id,
            reason="stale pointer",
            status="Ack Pending",
            redis_dlq_status="Written",
            ack_status="Failed",
            metadata_json={"fields": {"payload": "{not-json", "queue_task_id": "bad"}, "consumer_group": "recruitment-workers"},
        )
        db.add(record)
        db.commit()
        repaired = reconcile_dlq_records(db, client=fake)
        db.refresh(record)
        assert repaired == 1
        assert record.status == "Open"
        assert record.ack_status == "Acked"
        assert record.dlq_stream_id != stale_dlq_id
        assert fake.acked[-1] == (stream, "recruitment-workers", source_message_id)
        assert len(fake.streams[f"{stream}:dlq"]) == 1
        assert db.scalar(select(SystemError).where(SystemError.feature == f"{stream}:dlq", SystemError.severity == "Critical")) is not None


def test_dlq_reconcile_recovers_ack_pending_and_replay_in_progress():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        source_message_id = fake.xadd("recruitment:reconcile", {"payload": "{not-json", "queue_task_id": "bad"})
        dlq_id = fake.xadd("recruitment:reconcile:dlq", {"failed_stream": "recruitment:reconcile", "failed_message_id": source_message_id})
        record = QueueDlqRecord(
            stream_name="recruitment:reconcile",
            redis_stream_id=source_message_id,
            dlq_stream_id=dlq_id,
            reason="ack pending",
            status="Ack Pending",
            redis_dlq_status="Written",
            ack_status="Failed",
            metadata_json={"fields": {"payload": "{not-json", "queue_task_id": "bad"}, "consumer_group": "recruitment-workers"},
        )
        replayed_task = QueueTask(queue_name="redis-reconcile", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-reconcile-replayed", status="Retry", delivery_revision=2)
        stuck_task = QueueTask(queue_name="redis-reconcile", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-reconcile-stuck", status="Retry", delivery_revision=1)
        db.add_all([record, replayed_task, stuck_task])
        db.flush()
        replayed_record = QueueDlqRecord(stream_name="recruitment:replay", redis_stream_id="2-0", queue_task_id=replayed_task.id, delivery_revision=1, reason="crash after replay", status="Replay In Progress", replay_started_at=utc_now())
        old_started = (datetime.now(UTC) - timedelta(minutes=10)).isoformat(timespec="seconds").replace("+00:00", "Z")
        stuck_record = QueueDlqRecord(stream_name="recruitment:replay", redis_stream_id="3-0", queue_task_id=stuck_task.id, delivery_revision=1, reason="stuck replay", status="Replay In Progress", replay_started_at=old_started)
        db.add_all([replayed_record, stuck_record])
        db.flush()
        db.add(
            QueueReplayRecord(
                replay_request_key=f"dlq-replay:{replayed_record.id}:1",
                queue_task_id=replayed_task.id,
                dlq_record_id=replayed_record.id,
                expected_delivery_revision=1,
                replayed_delivery_revision=2,
                status="Succeeded",
                metadata_json={},
            )
        )
        db.commit()
        assert reconcile_dlq_records(db, client=fake, replay_timeout_seconds=60) == 3
        db.refresh(record)
        db.refresh(replayed_record)
        db.refresh(stuck_record)
        assert record.status == "Open"
        assert record.ack_status == "Acked"
        assert replayed_record.status == "Replayed"
        assert stuck_record.status == "Replay Failed"


def test_dlq_replay_blocks_stale_revision_dnc_deleted_candidate_and_completed_external_action():
    headers = auth_headers()
    with SessionLocal() as db:
        candidate = Candidate(name="DLQ Replay Guard", phone="0911222333", source_platform="manual")
        db.add(candidate)
        db.flush()
        stale = QueueTask(queue_name="redis-guard", task_type="message.send", payload={"candidate_id": candidate.id}, idempotency_key="dlq-stale", status="Retry", delivery_revision=2)
        dnc_task = QueueTask(queue_name="redis-guard", task_type="message.send", payload={"candidate_id": candidate.id}, idempotency_key="dlq-dnc", status="Retry", delivery_revision=1)
        sent_task = QueueTask(queue_name="redis-guard", task_type="message.send", payload={"candidate_id": candidate.id}, idempotency_key="dlq-sent", status="Retry", delivery_revision=1)
        deleted_candidate = Candidate(name="DLQ Deleted", phone="0911222444", source_platform="manual", tags=["deleted"])
        db.add_all([stale, dnc_task, sent_task, deleted_candidate])
        db.flush()
        deleted_task = QueueTask(queue_name="redis-guard", task_type="message.send", payload={"candidate_id": deleted_candidate.id}, idempotency_key="dlq-deleted", status="Retry", delivery_revision=1)
        db.add(deleted_task)
        db.flush()
        db.add(DoNotContact(candidate_id=candidate.id, reason="No contact"))
        db.add(ExternalActionLog(action_type="message.send", platform="line", queue_task_id=sent_task.id, status="Sent", attempted_at=datetime.now(UTC).isoformat()))
        records = [
            QueueDlqRecord(stream_name="recruitment:guard", redis_stream_id="stale-0", queue_task_id=stale.id, delivery_revision=1, reason="stale"),
            QueueDlqRecord(stream_name="recruitment:guard", redis_stream_id="dnc-0", queue_task_id=dnc_task.id, delivery_revision=1, reason="dnc"),
            QueueDlqRecord(stream_name="recruitment:guard", redis_stream_id="sent-0", queue_task_id=sent_task.id, delivery_revision=1, reason="sent"),
            QueueDlqRecord(stream_name="recruitment:guard", redis_stream_id="deleted-0", queue_task_id=deleted_task.id, delivery_revision=1, reason="deleted"),
        ]
        db.add_all(records)
        db.commit()
        record_ids = [record.id for record in records]
    for record_id in record_ids:
        response = client.post(f"/api/queue/dlq/{record_id}/replay", headers=headers)
        assert response.status_code == 409
    with SessionLocal() as db:
        statuses = [db.get(QueueDlqRecord, record_id).status for record_id in record_ids]
        assert statuses == ["Replay Blocked", "Replay Blocked", "Replay Blocked", "Replay Blocked"]


def test_frontend_queue_renderer_uses_dlq_total_items_contract():
    script = r"""
const fs = require('fs');
const vm = require('vm');
let source = fs.readFileSync('frontend/app.js', 'utf8');
source = source.replace(/refresh\(\)\.catch\([\s\S]*?\n\}\);[\s]*$/, '');
const app = { innerHTML: '' };
const sandbox = {
  console,
  document: {
    cookie: '',
    querySelector(selector) {
      if (selector === '#app') return app;
      return { textContent: '', innerHTML: '', addEventListener() {} };
    },
    querySelectorAll() { return []; }
  },
  fetch(url) {
    const path = String(url).replace('/api', '');
    const fixtures = {
      '/queue/tasks': [],
      '/health': { queue_outbox_pending: 2 },
      '/operations/metrics': { queue_depth: 3, manual_review_count: 1 },
      '/queue/redis/metrics': { redis: { status: 'Healthy' } },
      '/queue/outbox?limit=50': [],
      '/queue/dlq?limit=50': { total: 12, items: [{ id: 7, stream_name: 'recruitment:x', queue_task_id: 9, outbox_id: 4, delivery_revision: 2, status: 'Open', reason: 'lost message' }] }
    };
    return Promise.resolve({ ok: true, json: () => Promise.resolve(fixtures[path] ?? {}) });
  }
};
vm.createContext(sandbox);
vm.runInContext(source, sandbox);
sandbox.renderQueue().then(() => {
  if (!app.innerHTML.includes('12') || !app.innerHTML.includes('lost message') || !app.innerHTML.includes('replayDlq(7)')) process.exit(2);
}).catch((error) => { console.error(error); process.exit(1); });
"""
    result = subprocess.run(["node", "-e", script], cwd=Path.cwd(), text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr + result.stdout


def test_redis_consumer_acks_stale_delivery_revision_without_execution():
    fake = FakeRedisStreams()
    with SessionLocal() as db:
        task = QueueTask(queue_name="redis-stale", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="redis-stale", max_retries=1, delivery_revision=2)
        db.add(task)
        db.commit()
        fake.xadd("recruitment:redis-stale", {"payload": json.dumps({"queue_task_id": task.id, "delivery_revision": 1}), "queue_task_id": str(task.id), "delivery_revision": "1"})
        processed = asyncio.run(consume_redis_once(db, streams=["recruitment:redis-stale"], worker_id="test-worker", client=fake, block_ms=1))
        assert processed == 1
        db.refresh(task)
        assert task.status == "Queued"
        assert task.locked_by is None
        assert fake.acked


def test_external_purge_dry_run_and_legal_hold():
    headers = auth_headers()
    purge_task = client.post("/api/privacy/external-purges", json={"provider": "s3", "object_key": "resumes/a.pdf", "dry_run": True}, headers=headers).json()
    assert client.post(f"/api/queue/tasks/{purge_task['id']}/run", headers=headers).json()["status"] == "Completed"
    legal_task = client.post("/api/privacy/external-purges", json={"provider": "s3", "object_key": "resumes/legal.pdf", "legal_hold": True, "dry_run": True}, headers=headers).json()
    assert client.post(f"/api/queue/tasks/{legal_task['id']}/run", headers=headers).json()["status"] == "Manual Review Required"
    with SessionLocal() as db:
        dry_run = db.scalar(select(ExternalRetentionPurge).where(ExternalRetentionPurge.object_key == "resumes/a.pdf"))
        assert dry_run.status == "Dry Run - Not Deleted"
        assert dry_run.evidence["provider_deleted"] is False
        assert db.scalar(select(ExternalRetentionPurge).where(ExternalRetentionPurge.object_key == "resumes/legal.pdf")).status == "Legal Hold"


def test_same_sha_upload_creates_independent_storage_objects(tmp_path):
    headers = auth_headers()
    from pypdf import PdfWriter

    pdf_path = tmp_path / "same.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with pdf_path.open("wb") as handle:
        writer.write(handle)
    candidate_a = client.post("/api/candidates", json={"name": "Storage A", "source_platform": "manual"}, headers=headers).json()
    candidate_b = client.post("/api/candidates", json={"name": "Storage B", "source_platform": "manual"}, headers=headers).json()
    for candidate in [candidate_a, candidate_b]:
        with pdf_path.open("rb") as handle:
            assert client.post(f"/api/resumes/ocr?candidate_id={candidate['id']}", files={"file": ("same.pdf", handle, "application/pdf")}, headers=headers).status_code == 200
    with SessionLocal() as db:
        docs = db.execute(select(StoredDocument).where(StoredDocument.file_name == "same.pdf")).scalars().all()
        paths = {doc.storage_path for doc in docs if doc.candidate_id in {candidate_a["id"], candidate_b["id"]}}
        assert len(paths) == 2
        objects = db.execute(select(StorageObject).where(StorageObject.object_key.in_(paths))).scalars().all()
        assert len(objects) == 2
        assert all(obj.ref_count == 1 for obj in objects)


def test_voice_intent_negation_uncertainty_and_yes_no_problem():
    assert classify_voice_transcript("yes, no problem").intent == "confirm_interest"
    assert classify_voice_transcript("I confirm, no problem").intent == "confirm_interest"
    assert classify_voice_transcript("I have no time").intent == "decline_or_unavailable"
    assert classify_voice_transcript("不願意面試但可以回答問題").intent == "decline_or_unavailable"
    assert classify_voice_transcript("尚未確認").intent == "decline_or_unavailable"
    assert classify_voice_transcript("可能可以").intent == "unclear"
    assert disclosure_was_confirmed({"disclosure_played": "false"}) is False
    assert disclosure_was_confirmed({"disclosure_played": True}) is True


def test_voice_intent_explicit_negations_are_not_confirmed():
    for transcript in ["I am unavailable", "I do not confirm", "do not confirm interest", "not available to confirm", "confirm no"]:
        assert classify_voice_transcript(transcript).intent != "confirm_interest"


def test_ai_schema_rejects_nan_and_infinity():
    schema = {"type": "object", "required": ["confidence"], "properties": {"confidence": {"type": "number", "minimum": 0, "maximum": 1}}}
    for output in ['{"confidence": NaN}', '{"confidence": Infinity}', '{"confidence": -Infinity}']:
        try:
            _validate_json_schema(output, schema)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Non-finite number passed validation: {output}")


def test_calendar_readback_rejects_graph_cancelled_and_requires_timezone_for_naive_times():
    start = datetime(2026, 10, 5, 2, 0, tzinfo=UTC)
    end = datetime(2026, 10, 5, 3, 0, tzinfo=UTC)
    assert not _read_back_matches({"id": "evt-1", "isCancelled": True}, "evt-1", start=start, end=end)
    assert not _read_back_matches({"id": "evt-1", "start": {"dateTime": "2026-10-05T10:00:00"}, "end": {"dateTime": "2026-10-05T11:00:00"}}, "evt-1", start=start, end=end)
    assert _read_back_matches(
        {"id": "evt-1", "start": {"dateTime": "2026-10-05T10:00:00", "timeZone": "Asia/Taipei"}, "end": {"dateTime": "2026-10-05T11:00:00", "timeZone": "Asia/Taipei"}},
        "evt-1",
        start=start,
        end=end,
    )


def test_arrived_interview_cancels_running_no_show_without_message():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[9]
    job = client.get("/api/jobs").json()[0]
    future = datetime.now(ZoneInfo("Asia/Taipei")) + timedelta(hours=2)
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "reminder_policy": ["1h"]},
        headers=headers,
    ).json()
    with SessionLocal() as db:
        no_show = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_no_show_check", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        no_show.status = "Running"
        no_show.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        no_show.locked_by = "stale-worker"
        no_show.fencing_token = int(no_show.fencing_token or 0) + 1
        no_show.lease_expires_at = (datetime.now(UTC) + timedelta(minutes=10)).isoformat(timespec="seconds").replace("+00:00", "Z")
        row = db.get(Interview, interview["id"])
        row.attendance_status = "Arrived"
        row.attended_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        db.commit()
        no_show_id = no_show.id
    with SessionLocal() as db:
        task = db.get(QueueTask, no_show_id)
        asyncio.run(process_task(db, task))
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        assert row.status != "No Response Follow-up"
        assert db.scalar(select(Message).where(Message.metadata_json["flow"].as_string() == "interview_no_show_30m", Message.conversation_id == conversation["id"])) is None


def test_interview_reminder_downstream_send_key_includes_booking_version():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[10]
    job = client.get("/api/jobs").json()[0]
    future = datetime.now(ZoneInfo("Asia/Taipei")) + timedelta(hours=2)
    conversation = client.post("/api/conversations", json={"channel": "line", "candidate_id": candidate["id"], "job_id": job["id"]}, headers=headers).json()
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "reminder_policy": ["1h"]},
        headers=headers,
    ).json()
    with SessionLocal() as db:
        schedule_interview_tasks(db, db.get(Interview, interview["id"]))
        db.commit()
        reminder = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_reminder", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        reminder.status = "Queued"
        reminder.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        first_version = reminder.payload["booking_version"]
        reminder_id = reminder.id
        db.commit()
    with SessionLocal() as db:
        asyncio.run(process_task(db, db.get(QueueTask, reminder_id)))
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        row.interview_time = (future + timedelta(minutes=30)).strftime("%H:%M")
        schedule_interview_tasks(db, row)
        second = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_reminder", QueueTask.payload["interview_id"].as_integer() == interview["id"], QueueTask.status == "Queued"))
        second.status = "Queued"
        second.scheduled_at = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds").replace("+00:00", "Z")
        second_version = second.payload["booking_version"]
        second_id = second.id
        db.commit()
    assert first_version != second_version
    with SessionLocal() as db:
        asyncio.run(process_task(db, db.get(QueueTask, second_id)))
    with SessionLocal() as db:
        sends = db.execute(select(QueueTask).where(QueueTask.task_type == "message.send", QueueTask.payload["interview_id"].as_integer() == interview["id"])).scalars().all()
        keys = {task.idempotency_key for task in sends}
        assert f"interview-reminder:{interview['id']}:{first_version}:1h" in keys
        assert f"interview-reminder:{interview['id']}:{second_version}:1h" in keys
        assert len(keys) == 2
        assert all(task.payload.get("booking_version") in {first_version, second_version} for task in sends)
        assert conversation["id"]


def test_schedule_interview_tasks_does_not_restart_completed_or_valid_running_same_version():
    headers = auth_headers()
    candidate = client.get("/api/candidates").json()[11]
    job = client.get("/api/jobs").json()[0]
    future = datetime.now(ZoneInfo("Asia/Taipei")) + timedelta(hours=3)
    interview = client.post(
        "/api/interviews",
        json={"candidate_id": candidate["id"], "job_id": job["id"], "interview_date": future.strftime("%Y-%m-%d"), "interview_time": future.strftime("%H:%M"), "reminder_policy": ["1h"]},
        headers=headers,
    ).json()
    with SessionLocal() as db:
        row = db.get(Interview, interview["id"])
        schedule_interview_tasks(db, row)
        db.commit()
        reminder = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_reminder", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        reminder.status = "Completed"
        reminder.completed_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        reminder_revision = reminder.delivery_revision
        no_show = db.scalar(select(QueueTask).where(QueueTask.task_type == "flow.interview_no_show_check", QueueTask.payload["interview_id"].as_integer() == interview["id"]))
        no_show.status = "Running"
        no_show.locked_by = "worker-live"
        no_show.lease_expires_at = (datetime.now(UTC) + timedelta(minutes=20)).isoformat(timespec="seconds").replace("+00:00", "Z")
        no_show_revision = no_show.delivery_revision
        db.commit()
        schedule_interview_tasks(db, row)
        db.commit()
        db.refresh(reminder)
        db.refresh(no_show)
        assert reminder.status == "Completed"
        assert reminder.delivery_revision == reminder_revision
        assert no_show.status == "Running"
        assert no_show.locked_by == "worker-live"
        assert no_show.delivery_revision == no_show_revision


def test_queue_task_delete_retention_fk_keeps_logs_and_cascades_outbox():
    with SessionLocal() as db:
        task = QueueTask(queue_name="retention-fk", task_type="message.verify", payload={"message_id": 999999, "platform": "line"}, idempotency_key="retention-fk", max_retries=1)
        db.add(task)
        db.commit()
        task_id = task.id
        db.add(ExternalActionLog(action_type="message.verify", platform="line", queue_task_id=task_id, status="Failed", attempted_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")))
        db.commit()
        assert db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == task_id)) is not None
        db.delete(db.get(QueueTask, task_id))
        db.commit()
        assert db.scalar(select(QueueOutbox).where(QueueOutbox.queue_task_id == task_id)) is None
        log = db.scalar(select(ExternalActionLog).where(ExternalActionLog.action_type == "message.verify", ExternalActionLog.platform == "line").order_by(ExternalActionLog.id.desc()))
        assert log is not None
        assert log.queue_task_id is None


def test_sqlite_0017_rebuilds_legacy_queue_outbox_unique_constraint(tmp_path):
    legacy = tmp_path / "legacy-0016.db"
    import sqlite3

    con = sqlite3.connect(legacy)
    try:
        con.executescript(
            """
            create table alembic_version (version_num varchar(32) not null);
            insert into alembic_version values ('0016_queue_outbox_revision_repair');
            create table queue_tasks (
              id integer primary key,
              queue_name varchar(120) not null,
              task_type varchar(120) not null,
              payload json not null,
              idempotency_key varchar(255),
              status varchar(40) not null,
              scheduled_at varchar(40),
              locked_at varchar(40),
              locked_by varchar(160),
              lease_expires_at varchar(40),
              fencing_token integer not null default 0,
              delivery_revision integer not null default 1,
              redis_stream_id varchar(120),
              redis_claimed_at varchar(40),
              completed_at varchar(40),
              retry_count integer not null default 0,
              max_retries integer not null default 3,
              last_error text,
              created_at datetime not null,
              updated_at datetime not null
            );
            create table queue_outbox (
              id integer primary key,
              queue_task_id integer not null references queue_tasks(id),
              event_type varchar(80) not null,
              delivery_revision integer not null default 1,
              stream_name varchar(160) not null,
              payload json not null,
              status varchar(40) not null,
              published_at varchar(40),
              redis_stream_id varchar(120),
              retry_count integer not null default 0,
              last_error text,
              created_at datetime not null,
              updated_at datetime not null,
              constraint uq_queue_outbox_task_event unique(queue_task_id, event_type)
            );
            insert into queue_tasks(id, queue_name, task_type, payload, status, created_at, updated_at)
            values (1, 'flow', 'message.verify', '{}', 'Queued', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            insert into queue_outbox(queue_task_id, event_type, delivery_revision, stream_name, payload, status, created_at, updated_at)
            values (1, 'queue.publish', 1, 'recruitment:flow', '{}', 'Published', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            """
        )
        con.commit()
    finally:
        con.close()
    env = {**os.environ, "DATABASE_URL": f"sqlite:///{legacy.as_posix()}"}
    result = subprocess.run(["alembic", "upgrade", "head"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr + result.stdout
    con = sqlite3.connect(legacy)
    try:
        con.execute(
            "insert into queue_outbox(queue_task_id, event_type, delivery_revision, stream_name, payload, status, created_at, updated_at) values (1, 'queue.publish', 2, 'recruitment:flow', '{}', 'Pending', '2026-10-05 00:00:01', '2026-10-05 00:00:01')"
        )
        con.commit()
        rows = con.execute("select delivery_revision from queue_outbox where queue_task_id=1 order by delivery_revision").fetchall()
        assert rows == [(1,), (2,)]
        assert con.execute("select version_num from alembic_version").fetchone()[0] == "0026_scope_event_repairs"
    finally:
        con.close()


def _create_misversioned_0017_queue_db(path: Path, version: str) -> None:
    import sqlite3

    con = sqlite3.connect(path)
    try:
        con.executescript(
            f"""
            create table alembic_version (version_num varchar(64) not null);
            insert into alembic_version values ('{version}');
            create table queue_tasks (
              id integer primary key,
              queue_name varchar(120) not null,
              task_type varchar(120) not null,
              payload json not null,
              idempotency_key varchar(255),
              status varchar(40) not null,
              scheduled_at varchar(40),
              locked_at varchar(40),
              locked_by varchar(160),
              lease_expires_at varchar(40),
              fencing_token integer not null default 0,
              delivery_revision integer not null default 1,
              redis_stream_id varchar(120),
              redis_claimed_at varchar(40),
              completed_at varchar(40),
              retry_count integer not null default 0,
              max_retries integer not null default 3,
              last_error text,
              created_at datetime not null,
              updated_at datetime not null
            );
            create table queue_outbox (
              id integer primary key,
              queue_task_id integer not null references queue_tasks(id),
              event_type varchar(80) not null,
              delivery_revision integer not null default 1,
              stream_name varchar(160) not null,
              payload json not null,
              status varchar(40) not null,
              published_at varchar(40),
              redis_stream_id varchar(120),
              retry_count integer not null default 0,
              last_error text,
              created_at datetime not null,
              updated_at datetime not null,
              constraint uq_queue_outbox_task_event unique(queue_task_id, event_type)
            );
            insert into queue_tasks(id, queue_name, task_type, payload, status, created_at, updated_at)
            values (1, 'flow', 'message.verify', '{{}}', 'Queued', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            insert into queue_outbox(queue_task_id, event_type, delivery_revision, stream_name, payload, status, created_at, updated_at)
            values (1, 'queue.publish', 1, 'recruitment:flow', '{{}}', 'Published', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            """
        )
        con.commit()
    finally:
        con.close()


def test_sqlite_upgrade_to_0016_does_not_finalize_to_0017(tmp_path):
    db_path = tmp_path / "target-0016.db"
    env = {**os.environ, "DATABASE_URL": f"sqlite:///{db_path.as_posix()}"}
    result = subprocess.run(["alembic", "upgrade", "0016_queue_outbox_rev_repair"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr + result.stdout
    import sqlite3

    con = sqlite3.connect(db_path)
    try:
        assert con.execute("select version_num from alembic_version").fetchone()[0] == "0016_queue_outbox_rev_repair"
    finally:
        con.close()


def test_misversioned_0017_databases_run_schema_repair(tmp_path):
    import sqlite3

    for version in ["0017_queue_retention_fk", "0017_queue_outbox_constraints_and_retention_fk"]:
        legacy = tmp_path / f"{version}.db"
        _create_misversioned_0017_queue_db(legacy, version)
        env = {**os.environ, "DATABASE_URL": f"sqlite:///{legacy.as_posix()}"}
        result = subprocess.run(["alembic", "upgrade", "head"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
        assert result.returncode == 0, result.stderr + result.stdout
        con = sqlite3.connect(legacy)
        try:
            assert con.execute("select version_num from alembic_version").fetchone()[0] == "0026_scope_event_repairs"
            con.execute(
                "insert into queue_outbox(queue_task_id, event_type, delivery_revision, stream_name, payload, status, created_at, updated_at) values (1, 'queue.publish', 2, 'recruitment:flow', '{}', 'Pending', '2026-10-05 00:00:01', '2026-10-05 00:00:01')"
            )
            con.commit()
            fks = con.execute("pragma foreign_key_list(queue_outbox)").fetchall()
            assert len([row for row in fks if row[2] == "queue_tasks" and row[3] == "queue_task_id"]) == 1
            assert fks[0][6].upper() == "CASCADE"
        finally:
            con.close()


def test_blacklist_scope_repair_migration_handles_legacy_shapes(tmp_path):
    import sqlite3

    legacy = tmp_path / "blacklist-scope-repair.db"
    con = sqlite3.connect(legacy)
    try:
        con.executescript(
            """
            create table alembic_version (version_num varchar(64) not null);
            insert into alembic_version values ('0024_queue_replay_records');
            create table recruitment_blacklist (
              id integer primary key,
              candidate_id integer,
              phone varchar(40),
              email varchar(255),
              reason text not null,
              rule_code varchar(120),
              scope_type varchar(40),
              scope_id varchar(120),
              created_by integer,
              evidence text,
              status varchar(40),
              created_at datetime not null,
              updated_at datetime not null
            );
            insert into recruitment_blacklist(id, candidate_id, phone, email, reason, rule_code, scope_type, scope_id, created_by, evidence, status, created_at, updated_at)
            values
              (1, 1, null, null, 'old candidate job rule', 'job:01 job:2', 'candidate', null, null, 'legacy import', 'Active', '2026-10-08', '2026-10-08'),
              (2, 2, null, null, 'explicit global rule', 'global_block', 'global', null, null, 'explicit global', 'Active', '2026-10-08', '2026-10-08'),
              (3, 3, null, null, 'ambiguous legacy rule', 'legacy_block', 'legacy', null, null, 'missing scope', 'Active', '2026-10-08', '2026-10-08'),
              (4, 4, null, null, 'padded job scope', 'customer_rule', 'job', '007', null, 'known job', 'Active', '2026-10-08', '2026-10-08');
            """
        )
        con.commit()
    finally:
        con.close()

    env = {**os.environ, "DATABASE_URL": f"sqlite:///{legacy.as_posix()}"}
    result = subprocess.run(["alembic", "upgrade", "head"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr + result.stdout
    con = sqlite3.connect(legacy)
    try:
        assert con.execute("select version_num from alembic_version").fetchone()[0] == "0026_scope_event_repairs"
        rows = con.execute("select candidate_id, scope_type, scope_id, status from recruitment_blacklist order by id").fetchall()
        assert rows[0] == (1, "job", "1", "Active")
        assert rows[1] == (2, "global", None, "Active")
        assert rows[2] == (3, "legacy", None, "Pending Scope Confirmation")
        assert rows[3] == (4, "job", "7", "Active")
        assert ("job", "2", "Active") in con.execute("select scope_type, scope_id, status from recruitment_blacklist where candidate_id=1").fetchall()
    finally:
        con.close()


def test_sqlite_0018_repairs_full_legacy_audit_webhook_fks_and_preserves_data(tmp_path):
    import sqlite3

    legacy = tmp_path / "legacy-full-fks.db"
    con = sqlite3.connect(legacy)
    try:
        con.executescript(
            """
            create table alembic_version (version_num varchar(64) not null);
            insert into alembic_version values ('0017_queue_retention_fk');
            create table queue_tasks (
              id integer primary key,
              queue_name varchar(120) not null,
              task_type varchar(120) not null,
              payload json not null,
              idempotency_key varchar(255),
              status varchar(40) not null,
              scheduled_at varchar(40),
              locked_at varchar(40),
              locked_by varchar(160),
              lease_expires_at varchar(40),
              fencing_token integer not null default 0,
              delivery_revision integer not null default 1,
              redis_stream_id varchar(120),
              redis_claimed_at varchar(40),
              completed_at varchar(40),
              retry_count integer not null default 0,
              max_retries integer not null default 3,
              last_error text,
              created_at datetime not null,
              updated_at datetime not null
            );
            create table queue_outbox (
              id integer primary key,
              queue_task_id integer not null references queue_tasks(id) on delete no action,
              event_type varchar(80) not null,
              delivery_revision integer not null default 1,
              stream_name varchar(160) not null,
              payload json not null,
              status varchar(40) not null,
              published_at varchar(40),
              redis_stream_id varchar(120),
              retry_count integer not null default 0,
              last_error text,
              created_at datetime not null,
              updated_at datetime not null,
              constraint uq_queue_outbox_task_event unique(queue_task_id, event_type)
            );
            create table external_action_logs (
              id integer primary key,
              action_type varchar(120) not null,
              platform varchar(80) not null,
              queue_task_id integer,
              status varchar(40) not null,
              attempted_at varchar(40) not null,
              result text,
              created_at datetime not null,
              updated_at datetime not null,
              foreign key(queue_task_id) references queue_tasks(id) on delete no action,
              foreign key(queue_task_id) references queue_tasks(id) on delete set null
            );
            create index ix_external_action_logs_queue_task_id on external_action_logs(queue_task_id);
            create table platform_automation_runs (
              id integer primary key,
              platform varchar(80) not null,
              capability varchar(120) not null,
              queue_task_id integer,
              status varchar(40) not null,
              started_at varchar(40),
              created_at datetime not null,
              updated_at datetime not null,
              foreign key(queue_task_id) references queue_tasks(id) on delete no action
            );
            create table phone_webhook_events (
              id integer primary key,
              provider varchar(120) not null,
              event_id varchar(255) not null,
              nonce varchar(255) not null,
              fallback_task_id integer,
              received_at varchar(40) not null,
              status varchar(40) not null,
              payload_hash varchar(64) not null,
              created_at datetime not null,
              updated_at datetime not null,
              foreign key(fallback_task_id) references queue_tasks(id) on delete no action
            );
            insert into queue_tasks(id, queue_name, task_type, payload, status, created_at, updated_at)
            values (1, 'flow', 'message.verify', '{}', 'Queued', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            insert into queue_outbox(queue_task_id, event_type, delivery_revision, stream_name, payload, status, created_at, updated_at)
            values (1, 'queue.publish', 1, 'recruitment:flow', '{}', 'Published', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            insert into external_action_logs(action_type, platform, queue_task_id, status, attempted_at, result, created_at, updated_at)
            values ('message.verify', 'line', 1, 'Failed', '2026-10-05T00:00:00Z', 'kept', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            insert into platform_automation_runs(platform, capability, queue_task_id, status, started_at, created_at, updated_at)
            values ('line', 'message.verify', 1, 'Failed', '2026-10-05T00:00:00Z', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            insert into phone_webhook_events(provider, event_id, nonce, fallback_task_id, received_at, status, payload_hash, created_at, updated_at)
            values ('phone', 'evt-1', 'nonce-1', 1, '2026-10-05T00:00:00Z', 'Accepted', 'abc', '2026-10-05 00:00:00', '2026-10-05 00:00:00');
            """
        )
        con.commit()
    finally:
        con.close()

    env = {**os.environ, "DATABASE_URL": f"sqlite:///{legacy.as_posix()}"}
    result = subprocess.run(["alembic", "upgrade", "head"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr + result.stdout
    con = sqlite3.connect(legacy)
    try:
        assert con.execute("select version_num from alembic_version").fetchone()[0] == "0026_scope_event_repairs"
        for table, column in [
            ("external_action_logs", "queue_task_id"),
            ("platform_automation_runs", "queue_task_id"),
            ("phone_webhook_events", "fallback_task_id"),
        ]:
            fks = [row for row in con.execute(f"pragma foreign_key_list({table})").fetchall() if row[2] == "queue_tasks" and row[3] == column]
            assert len(fks) == 1
            assert fks[0][6].upper() == "SET NULL"
        queue_fks = [row for row in con.execute("pragma foreign_key_list(queue_outbox)").fetchall() if row[2] == "queue_tasks" and row[3] == "queue_task_id"]
        assert len(queue_fks) == 1
        assert queue_fks[0][6].upper() == "CASCADE"
        con.execute("pragma foreign_keys=on")
        con.execute("delete from queue_tasks where id=1")
        con.commit()
        assert con.execute("select count(*) from queue_outbox").fetchone()[0] == 0
        assert con.execute("select queue_task_id, result from external_action_logs").fetchone() == (None, "kept")
        assert con.execute("select queue_task_id from platform_automation_runs").fetchone()[0] is None
        assert con.execute("select fallback_task_id from phone_webhook_events").fetchone()[0] is None
        assert con.execute("pragma foreign_key_check").fetchall() == []
    finally:
        con.close()


def test_sqlite_upgrade_from_0015_to_head_is_idempotent_and_fk_clean(tmp_path):
    import sqlite3

    legacy = tmp_path / "from-0015.db"
    env = {**os.environ, "DATABASE_URL": f"sqlite:///{legacy.as_posix()}"}
    first = subprocess.run(["alembic", "upgrade", "0015_flow_redis_storage"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert first.returncode == 0, first.stderr + first.stdout
    head = subprocess.run(["alembic", "upgrade", "head"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert head.returncode == 0, head.stderr + head.stdout
    rerun = subprocess.run(["alembic", "upgrade", "head"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert rerun.returncode == 0, rerun.stderr + rerun.stdout
    con = sqlite3.connect(legacy)
    try:
        assert con.execute("select version_num from alembic_version").fetchone()[0] == "0026_scope_event_repairs"
        assert con.execute("pragma foreign_key_check").fetchall() == []
    finally:
        con.close()


def test_application_status_writes_business_flow_state():
    headers = auth_headers()
    candidate = client.post("/api/candidates", json={"name": "Flow State Candidate", "source_platform": "manual"}, headers=headers).json()
    job_id = client.get("/api/jobs", headers=headers).json()[0]["id"]
    application = client.post("/api/applications", json={"candidate_id": candidate["id"], "job_id": job_id}, headers=headers).json()
    changed = client.patch(f"/api/applications/{application['id']}/status", json={"status": "Interview", "reason": "candidate accepted"}, headers=headers)
    assert changed.status_code == 200
    with SessionLocal() as db:
        flow = db.scalar(select(BusinessFlowState).where(BusinessFlowState.application_id == application["id"]))
        assert flow is not None
        assert flow.stage == "interview_accepted"
        assert flow.revision >= 2
        assert "candidate accepted" in str(flow.evidence)


def test_legacy_unversioned_sqlite_upgrades_to_head(tmp_path):
    source = Path("recruitment.db")
    if not source.exists():
        return
    import sqlite3

    source_con = sqlite3.connect(source)
    try:
        source_candidate_count = source_con.execute("select count(*) from candidates").fetchone()[0]
    finally:
        source_con.close()
    legacy = tmp_path / "legacy-recruitment.db"
    shutil.copyfile(source, legacy)
    env = {**os.environ, "DATABASE_URL": f"sqlite:///{legacy.as_posix()}"}
    result = subprocess.run(["alembic", "upgrade", "head"], cwd=Path.cwd(), env=env, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr + result.stdout

    con = sqlite3.connect(legacy)
    try:
        version = con.execute("select version_num from alembic_version").fetchone()[0]
        columns = {row[1] for row in con.execute("pragma table_info(ai_providers)").fetchall()}
        assert version == "0026_scope_event_repairs"
        assert {"supports_vision", "supports_tools", "cost_policy"}.issubset(columns)
        assert con.execute("select count(*) from candidates").fetchone()[0] == source_candidate_count
        assert con.execute("select name from sqlite_master where type='table' and name='phone_webhook_events'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='platform_webhook_events'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='platform_rate_limit_state'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='data_retention_runs'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='calendar_bookings'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='stored_documents'").fetchone()
        interview_columns = {row[1] for row in con.execute("pragma table_info(interviews)").fetchall()}
        assert {"timezone", "attendance_status", "attended_at", "attendance_evidence"}.issubset(interview_columns)
        assert con.execute("select name from sqlite_master where type='table' and name='external_retention_purges'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='queue_outbox'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='business_flow_states'").fetchone()
        assert con.execute("select name from sqlite_master where type='table' and name='storage_objects'").fetchone()
        interview_columns = {row[1] for row in con.execute("pragma table_info(interviews)").fetchall()}
        assert "timezone" in interview_columns
        application_columns = {row[1] for row in con.execute("pragma table_info(applications)").fetchall()}
        assert {"decision_source", "decision_event_key"}.issubset(application_columns)
    finally:
        con.close()


def test_package_script_excludes_private_and_cache_artifacts(tmp_path):
    destination = tmp_path / "recruitops-clean.zip"
    result = subprocess.run(
        ["powershell", "-ExecutionPolicy", "Bypass", "-File", "scripts/package.ps1", "-Destination", str(destination)],
        cwd=Path.cwd(),
        text=True,
        encoding="utf-8",
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    forbidden_parts = ["__pycache__/", ".pytest_cache/", "postgres_data/", "data/inbound_mail/", "data/resumes/", "data/sessions/", "local_private/"]
    forbidden_suffixes = (".db", ".sqlite", ".sqlite3", ".pyc", ".pyo", ".log", ".zip")
    with zipfile.ZipFile(destination) as archive:
        names = [name.replace("\\", "/") for name in archive.namelist()]
        raw_names = archive.namelist()
    assert names
    assert not any("\\" in name for name in raw_names)
    assert not any(any(part in name for part in forbidden_parts) for name in names)
    assert not any(name.endswith(forbidden_suffixes) for name in names)
    assert "recruitment.db" not in {Path(name).name for name in names}


def test_alembic_revision_identifiers_fit_postgres_version_column():
    import re

    too_long = []
    for path in Path("alembic/versions").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        for field in ["revision", "down_revision"]:
            match = re.search(rf"^{field}\s*=\s*[\"']([^\"']+)[\"']", source, re.MULTILINE)
            if match and len(match.group(1)) > 32:
                too_long.append((path.name, field, match.group(1), len(match.group(1))))
    assert too_long == []


def test_cross_platform_package_script_excludes_private_and_cache_artifacts(tmp_path):
    destination = tmp_path / "recruitops-clean-python.zip"
    result = subprocess.run(
        ["python", "scripts/package.py", "--destination", str(destination)],
        cwd=Path.cwd(),
        text=True,
        encoding="utf-8",
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    forbidden_parts = ["__pycache__/", ".pytest_cache/", "postgres_data/", "redis_data/", "data/inbound_mail/", "data/resumes/", "data/sessions/", "local_private/"]
    forbidden_suffixes = (".db", ".sqlite", ".sqlite3", ".pyc", ".pyo", ".log", ".zip")
    with zipfile.ZipFile(destination) as archive:
        names = [name.replace("\\", "/") for name in archive.namelist()]
        raw_names = archive.namelist()
    assert names
    assert not any("\\" in name for name in raw_names)
    assert not any(any(part in name for part in forbidden_parts) for name in names)
    assert not any(name.endswith(forbidden_suffixes) for name in names)
    assert "recruitment.db" not in {Path(name).name for name in names}
