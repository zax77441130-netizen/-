from __future__ import annotations

import json
import hashlib
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.models.domain import (
    Application,
    ApplicationStatusHistory,
    BusinessEvent,
    BusinessFlowState,
    Candidate,
    CandidateQuestionAnswer,
    Conversation,
    DoNotContact,
    Interview,
    Job,
    Message,
    QueueTask,
    SystemError,
)
from backend.app.services.ai import run_task_with_router_sync
from backend.app.services.queue_utils import utc_now


AMBIGUOUS_WORDS = {"unknown", "not sure", "maybe", "unsure", "depends", "unclear", "不知道", "不確定", "都可以"}
MULTI_VALUE_MARKERS = {"/", " and ", " or ", ",", ";", "或"}
AI_DECISION_TASKS = {
    "MessageIntentClassification",
    "CurrentJobIdentification",
    "RequiredQuestionAnswerExtraction",
    "RequiredQuestionAnswerConfidence",
    "ConversationNextStepDecision",
    "ReplyDraftGeneration",
}
QUESTION_AI_SCHEMA = {
    "type": "object",
    "required": ["decision", "question_key", "answer_text", "confidence", "risk_flags"],
    "properties": {
        "decision": {"type": "string"},
        "question_key": {"type": "string"},
        "answer_text": {"type": "string"},
        "confidence": {"type": "number"},
        "risk_flags": {"type": "array"},
    },
}
INITIAL_INQUIRY_SCHEMA = {
    "type": "object",
    "required": ["intent", "job_id", "confidence", "risk_flags"],
    "properties": {
        "intent": {"type": "string"},
        "job_id": {"type": "string"},
        "confidence": {"type": "number"},
        "risk_flags": {"type": "array"},
    },
}
DELIVERED_STATUSES = {"Delivered", "Verified", "Read", "Sent"}
UNSUITABLE_STATUSES = {"Unsuitable", "Not Suitable", "不適合"}
CANCEL_UNSUITABLE_REJECTION_STATUSES = {
    "Suitable",
    "適合",
    "Interview",
    "Offer",
    "Hired",
    "Closed",
    "Talent Pool",
    "Manual Review",
    "HR Review",
}
INTERVIEW_REPLY_SCHEMA = {
    "type": "object",
    "required": ["intent", "confidence", "risk_flags", "evidence"],
    "properties": {
        "intent": {"type": "string"},
        "confidence": {"type": "number"},
        "risk_flags": {"type": "array"},
        "evidence": {"type": "array"},
    },
}
INTERVIEW_ACTIVE_STATUSES = {"Scheduled", "Reminder Sent", "Pending Confirmation", "Confirmed"}
INTERVIEW_TERMS = {"interview", "面試", "約", "時間", "報到", "到場", "改期", "取消", "確認"}
CONFIRM_TERMS = {"confirm", "confirmed", "yes", "ok", "okay", "will attend", "會去", "可以", "確認", "準時", "會到"}
DECLINE_TERMS = {"cannot", "can't", "no", "decline", "不去", "不會去", "不方便", "不要", "不行", "拒絕"}
CANCEL_TERMS = {"cancel", "取消"}
RESCHEDULE_TERMS = {"reschedule", "改期", "換時間", "另約", "延後", "提早"}
NEGATION_TERMS = {"not confirm", "not confirmed", "不確認", "不會", "不好", "不能", "不行", "沒辦法"}


FLOW_STAGE_BY_STATUS = {
    "New": "received",
    "Contacting": "contacting",
    "Waiting Reply": "contacting",
    "Pre-screen": "faq_or_prescreen",
    "HR Review": "hr_handoff",
    "Interview": "interview_accepted",
    "Offer": "hr_handoff",
    "Hired": "arrived_or_hired",
    "Talent Pool": "talent_pool",
    "No Response / Inactive": "inactive",
    "Closed": "closed",
    "Unsuitable": "rejection_pending",
    "Not Suitable": "rejection_pending",
}
APPLICATION_STATUS_RANK = {
    "New": 10,
    "Contacting": 20,
    "Waiting Reply": 25,
    "Pre-screen": 30,
    "HR Review": 40,
    "Interview": 50,
    "Offer": 60,
    "Hired": 70,
    "Talent Pool": 75,
    "No Response / Inactive": 80,
    "Closed": 90,
    "Unsuitable": 90,
    "Not Suitable": 90,
}


def record_business_event(
    db: Session,
    *,
    event_key: str,
    entity_type: str,
    entity_id: str,
    from_state: str | None,
    to_state: str,
    status: str = "Applied",
    reason: str | None = None,
    evidence: dict | None = None,
) -> BusinessEvent:
    existing = db.scalar(select(BusinessEvent).where(BusinessEvent.event_key == event_key))
    if existing:
        return existing
    event = BusinessEvent(
        event_key=event_key,
        entity_type=entity_type,
        entity_id=str(entity_id),
        from_state=from_state,
        to_state=to_state,
        status=status,
        reason=reason,
        evidence=evidence or {},
    )
    db.add(event)
    db.flush()
    return event


def transition_business_flow(
    db: Session,
    *,
    candidate_id: int,
    job_id: int | None = None,
    application_id: int | None = None,
    conversation_id: int | None = None,
    stage: str,
    event_key: str,
    evidence: dict | None = None,
    status: str = "Active",
    rule_version: str = "2026-10-04",
    config_version: str | None = None,
) -> BusinessFlowState:
    existing_event = db.scalar(select(BusinessFlowState).where(BusinessFlowState.last_event_key == event_key))
    if existing_event:
        return existing_event
    flow = db.scalar(
        select(BusinessFlowState).where(
            BusinessFlowState.candidate_id == candidate_id,
            BusinessFlowState.job_id == job_id,
            BusinessFlowState.flow_key == "default",
        )
    )
    if not flow:
        flow = BusinessFlowState(candidate_id=candidate_id, job_id=job_id, flow_key="default", stage=stage)
        db.add(flow)
        db.flush()
    flow.application_id = application_id or flow.application_id
    flow.conversation_id = conversation_id or flow.conversation_id
    flow.stage = stage
    flow.status = status
    flow.rule_version = rule_version
    flow.config_version = config_version or flow.config_version
    flow.revision = int(flow.revision or 0) + 1
    flow.last_event_key = event_key
    flow.evidence = {**(flow.evidence or {}), event_key: evidence or {}}
    return flow


def _contains_any(text: str, terms: set[str]) -> list[str]:
    lowered = text.lower()
    return [term for term in terms if term.lower() in lowered]


def has_active_dnc(db: Session, candidate_id: int | None) -> bool:
    if not candidate_id:
        return False
    return bool(db.scalar(select(DoNotContact).where(DoNotContact.candidate_id == candidate_id, DoNotContact.status == "Active")))


def question_key(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text.strip().lower()).strip("_")[:120] or "question"


def ensure_required_questions(db: Session, application: Application, conversation_id: int | None = None) -> list[CandidateQuestionAnswer]:
    job = db.get(Job, application.job_id)
    if not job:
        return []
    records: list[CandidateQuestionAnswer] = []
    for item in job.required_questions or []:
        text = item.get("text") if isinstance(item, dict) else str(item)
        key = item.get("key") if isinstance(item, dict) else question_key(text)
        existing = db.scalar(
            select(CandidateQuestionAnswer).where(
                CandidateQuestionAnswer.application_id == application.id,
                CandidateQuestionAnswer.question_key == key,
            )
        )
        if existing:
            if conversation_id and not existing.conversation_id:
                existing.conversation_id = conversation_id
            records.append(existing)
            continue
        record = CandidateQuestionAnswer(
            candidate_id=application.candidate_id,
            job_id=application.job_id,
            application_id=application.id,
            conversation_id=conversation_id,
            question_key=key,
            question_text=text,
            answer_status="Unanswered",
        )
        db.add(record)
        db.flush()
        records.append(record)
    return records


def classify_answer(text: str) -> str:
    normalized = text.strip().lower()
    if not normalized:
        return "Unanswered"
    if any(word in normalized for word in AMBIGUOUS_WORDS) or normalized.count("?") >= 2:
        return "Needs Clarification"
    if any(marker in normalized for marker in MULTI_VALUE_MARKERS):
        return "Multi Value"
    return "Answered"


def find_answer_target(db: Session, conversation_id: int, questions: list[CandidateQuestionAnswer]) -> CandidateQuestionAnswer | None:
    by_id = {item.id: item for item in questions}
    last_question_message = db.scalar(
        select(Message)
        .where(Message.conversation_id == conversation_id, Message.sender_type.in_(["system", "ai", "hr"]))
        .where(Message.metadata_json["question_answer_id"].as_integer().is_not(None))
        .where(Message.delivery_status.in_(DELIVERED_STATUSES))
        .order_by(Message.created_at.desc())
        .limit(1)
    )
    if not last_question_message:
        return None
    question_id = (last_question_message.metadata_json or {}).get("question_answer_id")
    target = by_id.get(question_id)
    if target and target.answer_status in {"Unanswered", "Needs Clarification"} and target.asked_at:
        return target
    return None


def analyze_answer_with_ai(db: Session, *, message: Message, conversation: Conversation, application: Application, target: CandidateQuestionAnswer, questions: list[CandidateQuestionAnswer]) -> dict | None:
    prompt = {
        "instruction": (
            "Classify a candidate reply for a recruiting required-question state machine. "
            "Return strict JSON only. Do not make hiring decisions, offers, rejection decisions, or browser actions. "
            "Use decision='answer_current_question' only when the reply clearly answers the active delivered question. "
            "Use manual_review for multi-job intent, skipped question, correction, contradiction, low confidence, non-text/STT uncertainty, unsafe content, or unclear mapping."
        ),
        "active_question": {"id": target.id, "key": target.question_key, "text": target.question_text},
        "all_questions": [{"id": item.id, "key": item.question_key, "status": item.answer_status} for item in questions],
        "conversation": {"id": conversation.id, "candidate_id": conversation.candidate_id, "job_id": conversation.job_id, "channel": conversation.channel},
        "application_id": application.id,
        "candidate_message": message.body,
        "required_schema": QUESTION_AI_SCHEMA,
    }
    result = run_task_with_router_sync(
        db,
        "RequiredQuestionAnswerExtraction",
        json.dumps(prompt, ensure_ascii=False),
        candidate_id=conversation.candidate_id,
        job_id=conversation.job_id,
        commit=False,
    )
    if result["status"] != "success":
        return None
    try:
        decoded = json.loads(result["output"])
    except json.JSONDecodeError:
        return None
    decoded["_provider"] = result["provider"]
    decoded["_model"] = result["model"]
    return decoded


def record_candidate_answer(db: Session, message: Message) -> None:
    conversation = db.get(Conversation, message.conversation_id)
    if not conversation or not conversation.candidate_id or not conversation.job_id:
        return
    application = db.scalar(
        select(Application).where(
            Application.candidate_id == conversation.candidate_id,
            Application.job_id == conversation.job_id,
        )
    )
    if not application:
        return
    questions = ensure_required_questions(db, application, conversation.id)
    target = find_answer_target(db, conversation.id, questions)
    if not target:
        enqueue_manual_exception(
            db,
            feature="question_answer_correlation",
            message="Inbound candidate reply did not match an active asked question.",
            impact="The conversation is held for manual review instead of assigning the answer to the wrong question.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "idempotency_key": f"manual-answer-correlation:{message.id}"},
        )
        return
    ai_decision = analyze_answer_with_ai(db, message=message, conversation=conversation, application=application, target=target, questions=questions)
    if not ai_decision:
        enqueue_manual_exception(
            db,
            feature="question_answer_ai_route",
            message="AI route did not return a successful valid JSON answer decision.",
            impact="The system will not assign the answer or continue automation without a reliable AI route result.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "answer_id": target.id, "idempotency_key": f"manual-answer-ai:{target.id}:{message.id}"},
        )
        return
    confidence = float(ai_decision.get("confidence") or 0)
    risk_flags = ai_decision.get("risk_flags") or []
    if ai_decision.get("decision") != "answer_current_question" or ai_decision.get("question_key") != target.question_key or confidence < 0.8 or risk_flags:
        enqueue_manual_exception(
            db,
            feature="question_answer_ai_decision",
            message="AI route did not produce a high-confidence state-machine-safe answer decision.",
            impact="The reply may be multi-job, skipped-question, contradictory, unsafe, or low confidence; HR review is required.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "answer_id": target.id, "ai_decision": ai_decision, "idempotency_key": f"manual-answer-ai-decision:{target.id}:{message.id}"},
        )
        return
    target.answer_text = ai_decision.get("answer_text") or message.body
    target.answer_status = classify_answer(target.answer_text)
    target.evidence_message_id = message.id
    target.answered_at = utc_now()
    target.metadata_json = {**(target.metadata_json or {}), "ai_task_routes": sorted(AI_DECISION_TASKS), "decision_mode": "controlled_state_machine", "ai_decision": ai_decision, "answer_evidence_message_id": message.id}
    cancel_pending_followups(db, conversation.id, target.id)
    if target.answer_status == "Answered":
        next_task = enqueue_next_question(db, application, conversation)
        if not next_task:
            application.status = "HR Review"
    elif target.answer_status in {"Needs Clarification", "Multi Value"}:
        enqueue_manual_exception(
            db,
            feature="question_answer_classification",
            message="Candidate answer needs clarification before sending the next required question.",
            impact="The system will not guess which question was answered or send the next prompt automatically.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "answer_id": target.id, "idempotency_key": f"manual-answer-classification:{target.id}:{message.id}"},
        )


def enqueue_manual_exception(db: Session, *, feature: str, message: str, impact: str, payload: dict | None = None) -> QueueTask:
    key = (payload or {}).get("idempotency_key")
    if key:
        existing = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == key))
        if existing:
            return existing
    db.add(SystemError(service="Flow", feature=feature, severity="Warning", message=message, impact=impact))
    task = QueueTask(
        queue_name="manual-exception",
        task_type=f"manual.{feature}",
        payload=payload or {},
        idempotency_key=key,
        status="Manual Review Required",
        max_retries=0,
    )
    db.add(task)
    db.flush()
    return task


def set_application_status(
    db: Session,
    application: Application,
    *,
    to_status: str,
    source: str = "manual",
    changed_by: int | None = None,
    reason: str | None = None,
    event_key: str | None = None,
) -> Application:
    if event_key:
        existing = db.scalar(select(ApplicationStatusHistory).where(ApplicationStatusHistory.event_key == event_key))
        if existing:
            return application
    old_status = application.status
    if event_key and source != "manual" and APPLICATION_STATUS_RANK.get(to_status, 0) < APPLICATION_STATUS_RANK.get(old_status, 0):
        record_business_event(
            db,
            event_key=event_key,
            entity_type="Application",
            entity_id=str(application.id),
            from_state=old_status,
            to_state=to_status,
            status="Ignored Stale Transition",
            reason=reason or "Rejected non-manual status regression.",
            evidence={"source": source},
        )
        return application
    if old_status == to_status and event_key:
        record_business_event(
            db,
            event_key=event_key,
            entity_type="Application",
            entity_id=str(application.id),
            from_state=old_status,
            to_state=to_status,
            status="Duplicate",
            reason=reason,
            evidence={"source": source},
        )
        return application
    application.status = to_status
    application.decision_source = source
    application.decision_event_key = event_key
    transition_business_flow(
        db,
        candidate_id=application.candidate_id,
        job_id=application.job_id,
        application_id=application.id,
        stage=FLOW_STAGE_BY_STATUS.get(to_status, to_status.lower().replace(" ", "_")),
        event_key=event_key or f"application-status:{application.id}:{to_status}:{datetime.now(UTC).timestamp()}",
        evidence={"from_status": old_status, "to_status": to_status, "source": source, "reason": reason},
        status="Closed" if to_status in {"Closed", "Hired"} else "Active",
    )
    db.add(
        ApplicationStatusHistory(
            application_id=application.id,
            from_status=old_status,
            to_status=to_status,
            changed_by=changed_by,
            reason=reason,
            source=source,
            event_key=event_key,
        )
    )
    record_business_event(
        db,
        event_key=event_key or f"application-status:{application.id}:{to_status}:{datetime.now(UTC).timestamp()}",
        entity_type="Application",
        entity_id=str(application.id),
        from_state=old_status,
        to_state=to_status,
        status="Applied",
        reason=reason,
        evidence={"source": source},
    )
    if to_status in UNSUITABLE_STATUSES:
        schedule_unsuitable_rejection(db, application, source=source, reason=reason)
    elif to_status in CANCEL_UNSUITABLE_REJECTION_STATUSES:
        cancel_pending_unsuitable_rejections(db, application.id, f"Cancelled because application status changed to {to_status}.")
    return application


def schedule_unsuitable_rejection(db: Session, application: Application, *, source: str = "manual", reason: str | None = None, delay: timedelta = timedelta(days=3)) -> QueueTask:
    scheduled_at = (datetime.now(UTC) + delay).isoformat(timespec="seconds").replace("+00:00", "Z")
    key = f"unsuitable-rejection-due:{application.id}"
    existing = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == key))
    if existing:
        if existing.status in {"Cancelled", "Completed", "Manual Review Required"}:
            existing.status = "Queued"
            existing.scheduled_at = scheduled_at
            existing.last_error = None
        return existing
    task = QueueTask(
        queue_name="flow",
        task_type="flow.unsuitable_rejection_due",
        payload={
            "application_id": application.id,
            "candidate_id": application.candidate_id,
            "job_id": application.job_id,
            "status_source": source,
            "reason": reason,
            "marked_unsuitable_at": utc_now(),
        },
        idempotency_key=key,
        scheduled_at=scheduled_at,
        max_retries=3,
    )
    db.add(task)
    db.flush()
    return task


def cancel_pending_unsuitable_rejections(db: Session, application_id: int, reason: str) -> int:
    tasks = db.execute(
        select(QueueTask).where(
            QueueTask.task_type == "flow.unsuitable_rejection_due",
            QueueTask.status.in_(["Queued", "Retry"]),
        )
    ).scalars().all()
    cancelled = 0
    for task in tasks:
        if (task.payload or {}).get("application_id") == application_id:
            task.status = "Cancelled"
            task.last_error = reason
            cancelled += 1
    return cancelled


def cancel_pending_interview_tasks(db: Session, interview_id: int, reason: str, task_types: set[str] | None = None) -> int:
    task_types = task_types or {"flow.interview_no_show_check", "flow.interview_reminder"}
    pending = db.execute(
        select(QueueTask).where(
            QueueTask.task_type.in_(task_types),
            QueueTask.status.in_(["Queued", "Retry", "Running"]),
        )
    ).scalars().all()
    cancelled = 0
    for task in pending:
        if (task.payload or {}).get("interview_id") == interview_id:
            task.status = "Cancelled"
            task.last_error = reason
            cancelled += 1
    return cancelled


def _running_lease_is_valid(task: QueueTask) -> bool:
    try:
        return (
            task.status == "Running"
            and bool(task.lease_expires_at)
            and datetime.fromisoformat(str(task.lease_expires_at).replace("Z", "+00:00")) > datetime.now(UTC)
        )
    except ValueError:
        return False


def _refresh_existing_scheduled_task(db: Session, task: QueueTask, *, scheduled_at: str, booking_version: str) -> QueueTask:
    if task.status == "Completed":
        return task
    if _running_lease_is_valid(task):
        return task
    task.status = "Queued"
    task.scheduled_at = scheduled_at
    task.last_error = None
    task.locked_by = None
    task.lease_expires_at = None
    task.payload = {**(task.payload or {}), "booking_version": booking_version}
    task.delivery_revision = int(task.delivery_revision or 1) + 1
    from backend.app.services.queue import enqueue_outbox_for_task

    enqueue_outbox_for_task(db, task)
    return task


def classify_interview_reply_text(text: str) -> dict:
    evidence: list[str] = []
    risk_flags: list[str] = []
    if not text.strip():
        return {"intent": "uncertain", "confidence": 0.0, "risk_flags": ["empty_reply"], "evidence": []}
    interview_hits = _contains_any(text, INTERVIEW_TERMS)
    confirm_hits = _contains_any(text, CONFIRM_TERMS)
    decline_hits = _contains_any(text, DECLINE_TERMS)
    cancel_hits = _contains_any(text, CANCEL_TERMS)
    reschedule_hits = _contains_any(text, RESCHEDULE_TERMS)
    negation_hits = _contains_any(text, NEGATION_TERMS)
    evidence.extend(interview_hits + confirm_hits + decline_hits + cancel_hits + reschedule_hits + negation_hits)
    if confirm_hits and (decline_hits or cancel_hits or reschedule_hits or negation_hits):
        return {"intent": "uncertain", "confidence": 0.4, "risk_flags": ["contradictory_or_negated_confirmation"], "evidence": evidence}
    if reschedule_hits:
        return {"intent": "reschedule", "confidence": 0.9, "risk_flags": [], "evidence": evidence}
    if cancel_hits:
        return {"intent": "cancel", "confidence": 0.9, "risk_flags": [], "evidence": evidence}
    if decline_hits or negation_hits:
        return {"intent": "decline", "confidence": 0.86, "risk_flags": [], "evidence": evidence}
    if confirm_hits:
        return {"intent": "confirm", "confidence": 0.88, "risk_flags": [], "evidence": evidence}
    if interview_hits:
        return {"intent": "uncertain", "confidence": 0.45, "risk_flags": ["interview_related_unclear"], "evidence": evidence}
    return {"intent": "unrelated", "confidence": 0.7, "risk_flags": [], "evidence": evidence}


def classify_interview_reply_with_ai(db: Session, *, message: Message, interview: Interview, conversation: Conversation) -> dict:
    fallback = classify_interview_reply_text(message.body)
    route_result = run_task_with_router_sync(
        db,
        "InterviewReplyClassification",
        json.dumps(
            {
                "instruction": (
                    "Classify the candidate's interview reply. Return strict JSON with intent, confidence, risk_flags, and evidence. "
                    "Allowed intent values: confirm, decline, cancel, reschedule, unrelated, uncertain. "
                    "Treat negated confirmations such as 'I will not go', 'not confirmed', or '不好' as decline/uncertain, never confirm."
                ),
                "interview": {"id": interview.id, "status": interview.status, "date": interview.interview_date, "time": interview.interview_time, "timezone": interview.timezone},
                "conversation": {"id": conversation.id, "candidate_id": conversation.candidate_id, "job_id": conversation.job_id},
                "candidate_message": message.body,
                "required_schema": INTERVIEW_REPLY_SCHEMA,
            },
            ensure_ascii=False,
        ),
        candidate_id=conversation.candidate_id,
        job_id=conversation.job_id,
        commit=False,
    )
    if route_result["status"] != "success":
        return {**fallback, "source": "rules_fallback"}
    try:
        decoded = json.loads(route_result["output"])
    except json.JSONDecodeError:
        return {**fallback, "source": "rules_fallback"}
    if not {"intent", "confidence", "risk_flags", "evidence"}.issubset(decoded.keys()):
        return {**fallback, "source": "rules_fallback"}
    intent = str(decoded.get("intent") or "uncertain")
    confidence = float(decoded.get("confidence") or 0)
    risk_flags = decoded.get("risk_flags") or []
    if intent not in {"confirm", "decline", "cancel", "reschedule", "unrelated", "uncertain"}:
        return {**fallback, "source": "rules_fallback"}
    return {"intent": intent, "confidence": confidence, "risk_flags": risk_flags, "evidence": decoded.get("evidence") or [], "source": f"{route_result['provider']}/{route_result['model']}"}


def process_interview_reply(db: Session, message: Message) -> bool:
    conversation = db.get(Conversation, message.conversation_id)
    if not conversation or message.sender_type != "candidate" or not conversation.candidate_id or not conversation.job_id:
        return False
    interview = db.scalar(
        select(Interview)
        .where(
            Interview.candidate_id == conversation.candidate_id,
            Interview.job_id == conversation.job_id,
            Interview.status.in_(INTERVIEW_ACTIVE_STATUSES),
        )
        .order_by(Interview.created_at.desc())
        .limit(1)
    )
    if not interview:
        return False
    metadata = message.metadata_json or {}
    if metadata.get("interview_id") == interview.id and isinstance(metadata.get("interview_reply_decision"), dict):
        decision = metadata["interview_reply_decision"]
    else:
        decision = classify_interview_reply_with_ai(db, message=message, interview=interview, conversation=conversation)
        message.metadata_json = {**metadata, "interview_reply_decision": decision, "interview_id": interview.id}
    intent = decision.get("intent")
    confidence = float(decision.get("confidence") or 0)
    risk_flags = decision.get("risk_flags") or []
    if intent == "unrelated":
        return False
    if confidence < 0.75 or risk_flags or intent == "uncertain":
        interview.status = "Manual Review Required"
        cancel_pending_interview_tasks(db, interview.id, "Cancelled because candidate sent an unclear interview-related reply.")
        enqueue_manual_exception(
            db,
            feature="interview_reply_classification",
            message="Interview reply was low-confidence, contradictory, negated, or unclear.",
            impact="Interview automation is paused so HR can classify confirmation, cancellation, or reschedule safely.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "interview_id": interview.id, "decision": decision, "idempotency_key": f"manual-interview-reply:{message.id}"},
        )
        return True
    if intent == "confirm":
        interview.status = "Confirmed"
        cancel_pending_interview_tasks(db, interview.id, "Cancelled because candidate confirmed the interview.", {"flow.interview_reminder"})
    elif intent in {"decline", "cancel"}:
        interview.status = "Cancelled"
        cancel_pending_interview_tasks(db, interview.id, "Cancelled because candidate declined or cancelled the interview.")
    elif intent == "reschedule":
        interview.status = "Reschedule Requested"
        cancel_pending_interview_tasks(db, interview.id, "Cancelled because candidate requested rescheduling.")
        enqueue_manual_exception(
            db,
            feature="interview_reschedule_requested",
            message="Candidate requested an interview reschedule.",
            impact="HR must offer a new approved time slot before any new reminder/no-show tasks are created.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "interview_id": interview.id, "decision": decision, "idempotency_key": f"manual-interview-reschedule:{message.id}"},
        )
    return True


def process_inbound_candidate_message(db: Session, message: Message) -> None:
    conversation = db.get(Conversation, message.conversation_id)
    if not conversation or message.sender_type != "candidate":
        return
    if process_interview_reply(db, message):
        return
    application = None
    if conversation.candidate_id and conversation.job_id:
        application = db.scalar(select(Application).where(Application.candidate_id == conversation.candidate_id, Application.job_id == conversation.job_id))
        if application:
            cancel_pending_unsuitable_rejections(db, application.id, "Cancelled because candidate replied before automatic rejection was due.")
    if application and find_answer_target(db, conversation.id, ensure_required_questions(db, application, conversation.id)):
        record_candidate_answer(db, message)
        return
    prompt = {
        "instruction": (
            "Classify an inbound recruiting inquiry. Return strict JSON only. "
            "Use intent='apply_to_job' only for clear application intent and a single identified job. "
            "Use manual_review for multi-job intent, unclear job, correction, unsafe content, low confidence, or non-text/STT uncertainty. "
            "Never make hiring, rejection, or offer decisions."
        ),
        "conversation": {"id": conversation.id, "candidate_id": conversation.candidate_id, "job_id": conversation.job_id, "channel": conversation.channel},
        "candidate_message": message.body,
        "known_job_id": conversation.job_id,
        "required_schema": INITIAL_INQUIRY_SCHEMA,
    }
    result = run_task_with_router_sync(db, "InitialInquiryClassification", json.dumps(prompt, ensure_ascii=False), candidate_id=conversation.candidate_id, job_id=conversation.job_id, commit=False)
    if result["status"] != "success":
        enqueue_manual_exception(
            db,
            feature="initial_inquiry_ai_route",
            message="Initial inquiry AI route did not return a successful valid JSON decision.",
            impact="Inbound message is stored, but automation is paused until HR maps the job/application.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "idempotency_key": f"manual-initial-ai:{message.id}"},
        )
        return
    try:
        decision = json.loads(result["output"])
    except json.JSONDecodeError:
        decision = {}
    confidence = float(decision.get("confidence") or 0)
    risk_flags = decision.get("risk_flags") or []
    job_id = conversation.job_id or _coerce_job_id(decision.get("job_id"))
    if decision.get("intent") != "apply_to_job" or not job_id or confidence < 0.8 or risk_flags:
        enqueue_manual_exception(
            db,
            feature="initial_inquiry_decision",
            message="Initial inquiry was not a high-confidence single-job application intent.",
            impact="The system will not create an application or send follow-up questions automatically.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "ai_decision": decision, "idempotency_key": f"manual-initial-decision:{message.id}"},
        )
        return
    if not conversation.job_id:
        conversation.job_id = job_id
    if not conversation.candidate_id:
        enqueue_manual_exception(
            db,
            feature="initial_inquiry_candidate_mapping",
            message="Conversation has no candidate mapping.",
            impact="HR must resolve the external sender before automation continues.",
            payload={"conversation_id": conversation.id, "message_id": message.id, "idempotency_key": f"manual-initial-candidate:{message.id}"},
        )
        return
    application = db.scalar(select(Application).where(Application.candidate_id == conversation.candidate_id, Application.job_id == job_id))
    if not application:
        application = Application(candidate_id=conversation.candidate_id, job_id=job_id, status="Pre-screen", ai_notes=f"Initial inquiry classified by {result['provider']}/{result['model']}.")
        db.add(application)
        db.flush()
    message.metadata_json = {**(message.metadata_json or {}), "initial_inquiry_decision": decision}
    enqueue_next_question(db, application, conversation)


def _coerce_job_id(value) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def enqueue_next_question(db: Session, application: Application, conversation: Conversation) -> QueueTask | None:
    questions = ensure_required_questions(db, application, conversation.id)
    next_question = next((item for item in questions if item.answer_status == "Unanswered"), None)
    if not next_question:
        return None
    existing = db.scalar(
        select(QueueTask).where(
            QueueTask.idempotency_key == f"question:{conversation.channel}:{application.id}:{next_question.question_key}",
            QueueTask.status.in_(["Queued", "Retry", "Running", "Completed"]),
        )
    )
    if existing:
        return existing
    next_question.asked_at = utc_now()
    message = Message(
        conversation_id=conversation.id,
        sender_type="system",
        body=next_question.question_text,
        metadata_json={"question_answer_id": next_question.id, "question_key": next_question.question_key, "approved_template": True},
        delivery_status="Pending Send",
    )
    db.add(message)
    db.flush()
    task = QueueTask(
        queue_name="messaging",
        task_type="message.send",
        payload={"message_id": message.id, "conversation_id": conversation.id, "platform": conversation.channel},
        idempotency_key=f"question:{conversation.channel}:{application.id}:{next_question.question_key}",
    )
    db.add(task)
    db.flush()
    return task


def schedule_inactive_follow_up(db: Session, conversation_id: int, question_answer_id: int, message_id: int, delivered_at: str, delay: timedelta = timedelta(days=3)) -> QueueTask:
    delivered = datetime.fromisoformat(delivered_at.replace("Z", "+00:00"))
    scheduled_at = (delivered + delay).isoformat(timespec="seconds").replace("+00:00", "Z")
    key = f"inactive-follow-up-due:{conversation_id}:{question_answer_id}:{message_id}"
    existing = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == key))
    if existing:
        return existing
    task = QueueTask(
        queue_name="flow",
        task_type="flow.follow_up_inactive",
        payload={"conversation_id": conversation_id, "question_answer_id": question_answer_id, "message_id": message_id, "delivered_at": delivered_at},
        idempotency_key=key,
        scheduled_at=scheduled_at,
        max_retries=3,
    )
    db.add(task)
    db.flush()
    return task


def cancel_pending_followups(db: Session, conversation_id: int, question_answer_id: int | None = None) -> int:
    pending = db.execute(
        select(QueueTask).where(
            QueueTask.task_type == "flow.follow_up_inactive",
            QueueTask.status.in_(["Queued", "Retry"]),
        )
    ).scalars().all()
    cancelled = 0
    for task in pending:
        payload = task.payload or {}
        if payload.get("conversation_id") == conversation_id and (question_answer_id is None or payload.get("question_answer_id") == question_answer_id):
            task.status = "Cancelled"
            task.last_error = "Cancelled because candidate replied before follow-up was due."
            cancelled += 1
    return cancelled


def schedule_interview_tasks(db: Session, interview) -> list[QueueTask]:
    tasks: list[QueueTask] = []
    booking_version = hashlib.sha256(
        f"{interview.id}:{interview.interview_date}:{interview.interview_time}:{getattr(interview, 'timezone', None)}".encode("utf-8")
    ).hexdigest()[:16]
    try:
        local_zone = ZoneInfo(getattr(interview, "timezone", None) or "Asia/Taipei")
        local_start = datetime.fromisoformat(f"{interview.interview_date}T{interview.interview_time}").replace(tzinfo=local_zone)
        utc_start = local_start.astimezone(UTC)
    except ValueError:
        enqueue_manual_exception(
            db,
            feature="interview_schedule",
            message="Interview date/time is not parseable for automatic reminders.",
            impact="HR must review and schedule reminders manually.",
            payload={"interview_id": interview.id, "idempotency_key": f"interview-invalid-time:{interview.id}"},
        )
        return tasks
    for offset in interview.reminder_policy or ["24h", "3h", "1h"]:
        seconds = _offset_to_seconds(str(offset))
        scheduled = utc_start - timedelta(seconds=seconds)
        if scheduled < datetime.now(UTC):
            continue
        idempotency_key = f"interview-reminder-due:{interview.id}:{booking_version}:{offset}"
        existing = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == idempotency_key))
        if existing:
            _refresh_existing_scheduled_task(db, existing, scheduled_at=scheduled.isoformat(timespec="seconds").replace("+00:00", "Z"), booking_version=booking_version)
            tasks.append(existing)
            continue
        task = QueueTask(
            queue_name="flow",
            task_type="flow.interview_reminder",
            payload={"interview_id": interview.id, "offset": str(offset), "booking_version": booking_version},
            idempotency_key=idempotency_key,
            scheduled_at=scheduled.isoformat(timespec="seconds").replace("+00:00", "Z"),
            max_retries=3,
        )
        db.add(task)
        tasks.append(task)
    no_show_due = utc_start + timedelta(minutes=30)
    no_show_key = f"interview-no-show:{interview.id}:{booking_version}:30m"
    existing_no_show = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == no_show_key))
    if existing_no_show:
        _refresh_existing_scheduled_task(db, existing_no_show, scheduled_at=no_show_due.isoformat(timespec="seconds").replace("+00:00", "Z"), booking_version=booking_version)
        tasks.append(existing_no_show)
        db.flush()
        return tasks
    no_show_task = QueueTask(
        queue_name="flow",
        task_type="flow.interview_no_show_check",
        payload={"interview_id": interview.id, "offset": "30m", "booking_version": booking_version},
        idempotency_key=no_show_key,
        scheduled_at=no_show_due.isoformat(timespec="seconds").replace("+00:00", "Z"),
        max_retries=1,
    )
    db.add(no_show_task)
    tasks.append(no_show_task)
    db.flush()
    return tasks


def _offset_to_seconds(value: str) -> int:
    normalized = value.strip().lower()
    if normalized.endswith("h"):
        return int(float(normalized[:-1]) * 3600)
    if normalized.endswith("m"):
        return int(float(normalized[:-1]) * 60)
    return int(float(normalized))


def find_or_create_conversation(db: Session, platform: str, candidate: Candidate, job_id: int | None) -> Conversation:
    existing = db.scalar(
        select(Conversation).where(
            Conversation.channel == platform,
            Conversation.candidate_id == candidate.id,
            Conversation.job_id == job_id,
            Conversation.status == "open",
        )
    )
    if existing:
        return existing
    conversation = Conversation(channel=platform, candidate_id=candidate.id, job_id=job_id, ai_mode="suggest", status="open")
    db.add(conversation)
    db.flush()
    return conversation
