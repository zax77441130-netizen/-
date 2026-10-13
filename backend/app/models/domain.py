from datetime import datetime
from enum import Enum

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.db import Base


class JobStatus(str, Enum):
    draft = "Draft"
    recruiting = "Recruiting"
    paused = "Paused"
    filled = "Filled"
    closed = "Closed"
    expired = "Expired"


class PipelineStatus(str, Enum):
    new = "New"
    contacting = "Contacting"
    waiting_reply = "Waiting Reply"
    pre_screen = "Pre-screen"
    hr_review = "HR Review"
    interview = "Interview"
    offer = "Offer"
    hired = "Hired"
    talent_pool = "Talent Pool"
    inactive = "No Response / Inactive"
    closed = "Closed"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Company(Base, TimestampMixin):
    __tablename__ = "companies"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160), unique=True)
    branches: Mapped[list["Branch"]] = relationship(back_populates="company")


DEFAULT_COMPANY_NAME = "預設公司"


class Branch(Base, TimestampMixin):
    __tablename__ = "branches"
    id: Mapped[int] = mapped_column(primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    name: Mapped[str] = mapped_column(String(160))
    area: Mapped[str] = mapped_column(String(80))
    company: Mapped[Company] = relationship(back_populates="branches")


class User(Base, TimestampMixin):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    role: Mapped[str] = mapped_column(String(40), default="Viewer")
    password_hash: Mapped[str] = mapped_column(String(255))
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class UserSession(Base, TimestampMixin):
    __tablename__ = "user_sessions"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    token_hash: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    csrf_token: Mapped[str] = mapped_column(String(128), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ip_address: Mapped[str | None] = mapped_column(String(80), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(255), nullable=True)


class Job(Base, TimestampMixin):
    __tablename__ = "jobs"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(160), index=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"))
    branch_id: Mapped[int | None] = mapped_column(ForeignKey("branches.id"), nullable=True)
    location: Mapped[str] = mapped_column(String(160))
    area: Mapped[str] = mapped_column(String(80), index=True)
    job_type: Mapped[str] = mapped_column(String(80))
    shift: Mapped[str] = mapped_column(String(80))
    hours: Mapped[str] = mapped_column(String(120))
    salary_text: Mapped[str] = mapped_column(String(160))
    hourly_wage: Mapped[float | None] = mapped_column(Float, nullable=True)
    monthly_salary: Mapped[float | None] = mapped_column(Float, nullable=True)
    bonuses: Mapped[str | None] = mapped_column(String(255), nullable=True)
    allowances: Mapped[str | None] = mapped_column(String(255), nullable=True)
    overtime_policy: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description: Mapped[str] = mapped_column(Text)
    requirements: Mapped[str] = mapped_column(Text)
    required_headcount: Mapped[int] = mapped_column(Integer, default=1)
    urgency: Mapped[str] = mapped_column(String(40), default="Normal")
    start_date: Mapped[str | None] = mapped_column(String(40), nullable=True)
    end_date: Mapped[str | None] = mapped_column(String(40), nullable=True)
    contact_name: Mapped[str] = mapped_column(String(120))
    contact_method: Mapped[str] = mapped_column(String(160))
    source: Mapped[str] = mapped_column(String(80), default="manual")
    status: Mapped[str] = mapped_column(String(40), default=JobStatus.draft.value)
    required_questions: Mapped[list] = mapped_column(JSON, default=list)
    version: Mapped[int] = mapped_column(Integer, default=1)


class Candidate(Base, TimestampMixin):
    __tablename__ = "candidates"
    __table_args__ = (
        UniqueConstraint("phone", name="uq_candidate_phone"),
        UniqueConstraint("email", name="uq_candidate_email"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), index=True)
    phone: Mapped[str | None] = mapped_column(String(40), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    region: Mapped[str | None] = mapped_column(String(80), nullable=True)
    skills: Mapped[list] = mapped_column(JSON, default=list)
    licenses: Mapped[list] = mapped_column(JSON, default=list)
    expected_salary: Mapped[str | None] = mapped_column(String(120), nullable=True)
    preferred_area: Mapped[str | None] = mapped_column(String(80), nullable=True)
    preferred_shift: Mapped[str | None] = mapped_column(String(80), nullable=True)
    source_platform: Mapped[str] = mapped_column(String(80), default="manual")
    platform_id: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(40), default=PipelineStatus.new.value)
    tags: Mapped[list] = mapped_column(JSON, default=list)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_hr_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)


class Resume(Base, TimestampMixin):
    __tablename__ = "resumes"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int | None] = mapped_column(ForeignKey("candidates.id"), nullable=True)
    source: Mapped[str] = mapped_column(String(80), default="manual")
    file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_text: Mapped[str] = mapped_column(Text)
    parsed_json: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(40), default="Parsed")


class Application(Base, TimestampMixin):
    __tablename__ = "applications"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"))
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    status: Mapped[str] = mapped_column(String(40), default=PipelineStatus.new.value)
    ai_match_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    ai_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision_source: Mapped[str] = mapped_column(String(40), default="manual", index=True)
    decision_event_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)


class ApplicationStatusHistory(Base, TimestampMixin):
    __tablename__ = "application_status_history"
    id: Mapped[int] = mapped_column(primary_key=True)
    application_id: Mapped[int] = mapped_column(ForeignKey("applications.id"))
    from_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    to_status: Mapped[str] = mapped_column(String(40))
    changed_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(String(40), default="manual", index=True)
    event_key: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True, index=True)


class Interview(Base, TimestampMixin):
    __tablename__ = "interviews"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"))
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    hr_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    interview_date: Mapped[str] = mapped_column(String(40))
    interview_time: Mapped[str] = mapped_column(String(40))
    timezone: Mapped[str] = mapped_column(String(80), default="Asia/Taipei")
    location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    method: Mapped[str] = mapped_column(String(40), default="phone")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    reminder_policy: Mapped[list] = mapped_column(JSON, default=lambda: ["24h", "3h", "1h"])
    status: Mapped[str] = mapped_column(String(40), default="Scheduled")
    attendance_status: Mapped[str] = mapped_column(String(40), default="Not Arrived", index=True)
    attended_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    attendance_evidence: Mapped[dict] = mapped_column(JSON, default=dict)


class MatchRecord(Base, TimestampMixin):
    __tablename__ = "matches"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"))
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    score: Mapped[float] = mapped_column(Float, default=0)
    signals: Mapped[dict] = mapped_column(JSON, default=dict)
    missing_info: Mapped[list] = mapped_column(JSON, default=list)
    recruiter_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="Suggested")


class DoNotContact(Base, TimestampMixin):
    __tablename__ = "do_not_contact"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"))
    reason: Mapped[str] = mapped_column(Text)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    can_lift: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(40), default="Active")


class RecruitmentBlacklist(Base, TimestampMixin):
    __tablename__ = "recruitment_blacklist"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int | None] = mapped_column(ForeignKey("candidates.id"), nullable=True, index=True)
    phone: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    reason: Mapped[str] = mapped_column(Text)
    rule_code: Mapped[str] = mapped_column(String(120), default="customer_rule")
    scope_type: Mapped[str] = mapped_column(String(40), default="candidate", index=True)
    scope_id: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    created_by: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="Active", index=True)


class Conversation(Base, TimestampMixin):
    __tablename__ = "conversations"
    id: Mapped[int] = mapped_column(primary_key=True)
    channel: Mapped[str] = mapped_column(String(80))
    candidate_id: Mapped[int | None] = mapped_column(ForeignKey("candidates.id"), nullable=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), nullable=True)
    ai_mode: Mapped[str] = mapped_column(String(40), default="suggest")
    status: Mapped[str] = mapped_column(String(40), default="open")


class PlatformIdentityMap(Base, TimestampMixin):
    __tablename__ = "platform_identity_maps"
    __table_args__ = (
        UniqueConstraint("platform", "account_id", "entity_type", "external_id", name="uq_platform_identity_external"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(80), index=True)
    account_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True, index=True)
    entity_type: Mapped[str] = mapped_column(String(80), index=True)
    external_id: Mapped[str] = mapped_column(String(255), index=True)
    candidate_id: Mapped[int | None] = mapped_column(ForeignKey("candidates.id"), nullable=True, index=True)
    conversation_id: Mapped[int | None] = mapped_column(ForeignKey("conversations.id"), nullable=True, index=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), nullable=True, index=True)
    external_candidate_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    external_conversation_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    external_job_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class Message(Base, TimestampMixin):
    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(primary_key=True)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id"))
    sender_type: Mapped[str] = mapped_column(String(40))
    body: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    delivery_status: Mapped[str] = mapped_column(String(40), default="Internal Record", index=True)
    platform_message_id: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    external_status_checked_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    candidate_read_status: Mapped[str] = mapped_column(String(40), default="Unknown", index=True)
    candidate_read_at: Mapped[str | None] = mapped_column(String(40), nullable=True)


class CandidateQuestionAnswer(Base, TimestampMixin):
    __tablename__ = "candidate_question_answers"
    __table_args__ = (UniqueConstraint("application_id", "question_key", name="uq_application_question_key"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"), index=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"), index=True)
    application_id: Mapped[int | None] = mapped_column(ForeignKey("applications.id"), nullable=True, index=True)
    conversation_id: Mapped[int | None] = mapped_column(ForeignKey("conversations.id"), nullable=True)
    question_key: Mapped[str] = mapped_column(String(120))
    question_text: Mapped[str] = mapped_column(Text)
    answer_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    answer_status: Mapped[str] = mapped_column(String(40), default="Unanswered", index=True)
    evidence_message_id: Mapped[int | None] = mapped_column(ForeignKey("messages.id"), nullable=True)
    asked_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    answered_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class ExternalActionLog(Base, TimestampMixin):
    __tablename__ = "external_action_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    action_type: Mapped[str] = mapped_column(String(120), index=True)
    platform: Mapped[str] = mapped_column(String(80), index=True)
    account_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True)
    target_account: Mapped[str | None] = mapped_column(String(255), nullable=True)
    platform_object_id: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    queue_task_id: Mapped[int | None] = mapped_column(ForeignKey("queue_tasks.id", ondelete="SET NULL"), nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    attempted_at: Mapped[str] = mapped_column(String(40))
    verified_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)


class PlatformAccount(Base, TimestampMixin):
    __tablename__ = "platform_accounts"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(80), index=True)
    display_name: Mapped[str] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(40), default="Not Configured")
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    capabilities: Mapped[list] = mapped_column(JSON, default=list)
    work_windows: Mapped[list] = mapped_column(JSON, default=list)
    last_success_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    last_failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class PlatformGroup(Base, TimestampMixin):
    __tablename__ = "platform_groups"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform_account_id: Mapped[int] = mapped_column(ForeignKey("platform_accounts.id"))
    name: Mapped[str] = mapped_column(String(160))
    url: Mapped[str] = mapped_column(String(500))
    area: Mapped[str] = mapped_column(String(80))
    group_type: Mapped[str] = mapped_column(String(80))
    allowed_windows: Mapped[list] = mapped_column(JSON, default=list)
    last_posted_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    posting_frequency: Mapped[str] = mapped_column(String(80), default="manual")
    success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="Active")


class Post(Base, TimestampMixin):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    platform: Mapped[str] = mapped_column(String(80))
    account_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True)
    group_id: Mapped[int | None] = mapped_column(ForeignKey("platform_groups.id"), nullable=True)
    scheduled_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="Queued")
    content: Mapped[str] = mapped_column(Text)
    post_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    platform_post_id: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    verified_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    error_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)


class InboundEmail(Base, TimestampMixin):
    __tablename__ = "inbound_emails"
    __table_args__ = (UniqueConstraint("message_id", name="uq_inbound_email_message_id"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[str] = mapped_column(String(255), index=True)
    sender: Mapped[str] = mapped_column(String(255))
    received_at: Mapped[str] = mapped_column(String(40), index=True)
    subject: Mapped[str] = mapped_column(String(500))
    source_platform: Mapped[str] = mapped_column(String(80), default="518")
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), nullable=True)
    candidate_id: Mapped[int | None] = mapped_column(ForeignKey("candidates.id"), nullable=True)
    application_id: Mapped[int | None] = mapped_column(ForeignKey("applications.id"), nullable=True)
    raw_text: Mapped[str] = mapped_column(Text)
    attachment_names: Mapped[list] = mapped_column(JSON, default=list)
    import_status: Mapped[str] = mapped_column(String(40), default="Received", index=True)
    parse_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    platform_link: Mapped[str | None] = mapped_column(String(500), nullable=True)
    mailbox: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    imap_uid: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    uid_validity: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)


class EmailCursor(Base, TimestampMixin):
    __tablename__ = "email_cursors"
    __table_args__ = (UniqueConstraint("account_id", "mailbox", "uid_validity", name="uq_email_cursor_scope"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    account_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True, index=True)
    mailbox: Mapped[str] = mapped_column(String(160), default="INBOX", index=True)
    uid_validity: Mapped[str] = mapped_column(String(80), default="unknown", index=True)
    last_imported_uid: Mapped[int] = mapped_column(Integer, default=0)
    last_ack_uid: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(40), default="Active")


class PlatformAutomationRun(Base, TimestampMixin):
    __tablename__ = "platform_automation_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(80), index=True)
    capability: Mapped[str] = mapped_column(String(120), index=True)
    account_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True)
    queue_task_id: Mapped[int | None] = mapped_column(ForeignKey("queue_tasks.id", ondelete="SET NULL"), nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="Queued", index=True)
    started_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    finished_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    platform_entity_id: Mapped[str | None] = mapped_column(String(160), nullable=True)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    error_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    screenshot_path: Mapped[str | None] = mapped_column(String(500), nullable=True)


class PhoneCallLog(Base, TimestampMixin):
    __tablename__ = "phone_call_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"), index=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), nullable=True, index=True)
    conversation_id: Mapped[int | None] = mapped_column(ForeignKey("conversations.id"), nullable=True, index=True)
    provider: Mapped[str | None] = mapped_column(String(120), nullable=True)
    external_call_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    dial_status: Mapped[str] = mapped_column(String(40), default="Not Configured", index=True)
    call_result: Mapped[str | None] = mapped_column(String(120), nullable=True)
    script: Mapped[str | None] = mapped_column(Text, nullable=True)
    transcript: Mapped[str | None] = mapped_column(Text, nullable=True)
    recording_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    started_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    finished_at: Mapped[str | None] = mapped_column(String(40), nullable=True)


class CalendarBooking(Base, TimestampMixin):
    __tablename__ = "calendar_bookings"
    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_calendar_booking_idempotency_key"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    interview_id: Mapped[int] = mapped_column(ForeignKey("interviews.id"), index=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"), index=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"), index=True)
    provider: Mapped[str] = mapped_column(String(80), default="mock")
    interviewer_ids: Mapped[list] = mapped_column(JSON, default=list)
    timezone: Mapped[str] = mapped_column(String(80), default="Asia/Taipei")
    starts_at: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    ends_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    hold_token: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    external_event_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), index=True)
    read_back: Mapped[dict] = mapped_column(JSON, default=dict)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class PhoneWebhookEvent(Base, TimestampMixin):
    __tablename__ = "phone_webhook_events"
    __table_args__ = (
        UniqueConstraint("provider", "nonce", name="uq_phone_webhook_provider_nonce"),
        UniqueConstraint("provider", "event_id", name="uq_phone_webhook_provider_event_id"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(120), default="default", index=True)
    event_id: Mapped[str] = mapped_column(String(255), index=True)
    nonce: Mapped[str] = mapped_column(String(255), index=True)
    external_call_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    received_at: Mapped[str] = mapped_column(String(40), index=True)
    status: Mapped[str] = mapped_column(String(40), default="Accepted", index=True)
    payload_hash: Mapped[str] = mapped_column(String(64))
    fallback_task_id: Mapped[int | None] = mapped_column(ForeignKey("queue_tasks.id", ondelete="SET NULL"), nullable=True)


class PlatformWebhookEvent(Base, TimestampMixin):
    __tablename__ = "platform_webhook_events"
    __table_args__ = (UniqueConstraint("platform", "account_id", "event_id", name="uq_platform_webhook_event"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(80), index=True)
    account_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True, index=True)
    event_id: Mapped[str] = mapped_column(String(255), index=True)
    external_conversation_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    external_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    received_at: Mapped[str] = mapped_column(String(40), index=True)
    status: Mapped[str] = mapped_column(String(40), default="Accepted", index=True)
    payload_hash: Mapped[str] = mapped_column(String(64))
    canonical_event_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    raw_body_b64: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)


class StoredDocument(Base, TimestampMixin):
    __tablename__ = "stored_documents"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int | None] = mapped_column(ForeignKey("candidates.id"), nullable=True, index=True)
    file_name: Mapped[str] = mapped_column(String(255))
    storage_path: Mapped[str] = mapped_column(String(500))
    content_type: Mapped[str] = mapped_column(String(120), default="application/octet-stream")
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    byte_size: Mapped[int] = mapped_column(Integer, default=0)
    page_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(40), default="Stored", index=True)
    retention_policy: Mapped[dict] = mapped_column(JSON, default=dict)
    deleted_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    delete_audit: Mapped[dict] = mapped_column(JSON, default=dict)


class StorageObject(Base, TimestampMixin):
    __tablename__ = "storage_objects"
    __table_args__ = (UniqueConstraint("object_key", "version_id", name="uq_storage_object_version"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    object_key: Mapped[str] = mapped_column(String(500), index=True)
    version_id: Mapped[str] = mapped_column(String(160), default="local", index=True)
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    byte_size: Mapped[int] = mapped_column(Integer, default=0)
    storage_provider: Mapped[str] = mapped_column(String(80), default="local_file")
    ref_count: Mapped[int] = mapped_column(Integer, default=1)
    legal_hold: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(40), default="Active", index=True)
    deleted_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)


class PlatformRateLimitState(Base, TimestampMixin):
    __tablename__ = "platform_rate_limit_state"
    __table_args__ = (UniqueConstraint("platform", "account_id", "scope_type", "scope_id", name="uq_platform_rate_limit_scope"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(80), index=True)
    account_id: Mapped[int | None] = mapped_column(ForeignKey("platform_accounts.id"), nullable=True, index=True)
    scope_type: Mapped[str] = mapped_column(String(80), index=True)
    scope_id: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(40), default="Active", index=True)
    hour_bucket: Mapped[str | None] = mapped_column(String(40), nullable=True)
    hour_count: Mapped[int] = mapped_column(Integer, default=0)
    day_bucket: Mapped[str | None] = mapped_column(String(40), nullable=True)
    day_count: Mapped[int] = mapped_column(Integer, default=0)
    last_sent_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    last_content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    next_allowed_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    consecutive_errors: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class CandidateMergeConflict(Base, TimestampMixin):
    __tablename__ = "candidate_merge_conflicts"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int | None] = mapped_column(ForeignKey("candidates.id"), nullable=True)
    incoming_source: Mapped[str] = mapped_column(String(80))
    conflict_type: Mapped[str] = mapped_column(String(80))
    incoming_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    existing_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(40), default="Open")


class AIProvider(Base, TimestampMixin):
    __tablename__ = "ai_providers"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    provider_type: Mapped[str] = mapped_column(String(80))
    base_url: Mapped[str] = mapped_column(String(500))
    endpoint: Mapped[str] = mapped_column(String(160), default="/chat/completions")
    api_key_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    model: Mapped[str] = mapped_column(String(160))
    supports_text: Mapped[bool] = mapped_column(Boolean, default=True)
    supports_vision: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_json_schema: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_tools: Mapped[bool] = mapped_column(Boolean, default=False)
    context_window: Mapped[int] = mapped_column(Integer, default=8192)
    max_output: Mapped[int] = mapped_column(Integer, default=1024)
    temperature: Mapped[float] = mapped_column(Float, default=0.2)
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=30)
    retry_policy: Mapped[dict] = mapped_column(JSON, default=dict)
    cost_policy: Mapped[dict] = mapped_column(JSON, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_status: Mapped[str] = mapped_column(String(40), default="Not Tested")
    last_latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_tested_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    circuit_open_until: Mapped[str | None] = mapped_column(String(40), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)


class TaskModelRoute(Base, TimestampMixin):
    __tablename__ = "task_model_routes"
    id: Mapped[int] = mapped_column(primary_key=True)
    task_name: Mapped[str] = mapped_column(String(120), unique=True)
    primary_provider_id: Mapped[int | None] = mapped_column(ForeignKey("ai_providers.id"), nullable=True)
    fallback_provider_ids: Mapped[list] = mapped_column(JSON, default=list)
    budget_policy: Mapped[dict] = mapped_column(JSON, default=dict)


class AIRequest(Base, TimestampMixin):
    __tablename__ = "ai_requests"
    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(120))
    model: Mapped[str] = mapped_column(String(160))
    feature: Mapped[str] = mapped_column(String(120))
    user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    candidate_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cached_tokens: Mapped[int] = mapped_column(Integer, default=0)
    usage_source: Mapped[str] = mapped_column(String(40), default="Estimated")
    estimated_cost: Mapped[float] = mapped_column(Float, default=0)
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(40))
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    prompt_masked: Mapped[str] = mapped_column(Text)
    response_masked: Mapped[str | None] = mapped_column(Text, nullable=True)


class Prompt(Base, TimestampMixin):
    __tablename__ = "prompts"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    purpose: Mapped[str] = mapped_column(String(160))
    active_version: Mapped[int] = mapped_column(Integer, default=1)


class PromptVersion(Base, TimestampMixin):
    __tablename__ = "prompt_versions"
    id: Mapped[int] = mapped_column(primary_key=True)
    prompt_id: Mapped[int] = mapped_column(ForeignKey("prompts.id"))
    version: Mapped[int] = mapped_column(Integer)
    system_prompt: Mapped[str] = mapped_column(Text)
    template: Mapped[str] = mapped_column(Text)
    model_hint: Mapped[str | None] = mapped_column(String(160), nullable=True)
    temperature: Mapped[float] = mapped_column(Float, default=0.2)
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True)


class AuditLog(Base, TimestampMixin):
    __tablename__ = "audit_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    actor_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action: Mapped[str] = mapped_column(String(120))
    entity_type: Mapped[str] = mapped_column(String(120))
    entity_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    details: Mapped[dict] = mapped_column(JSON, default=dict)


class SystemError(Base, TimestampMixin):
    __tablename__ = "system_errors"
    id: Mapped[int] = mapped_column(primary_key=True)
    service: Mapped[str] = mapped_column(String(120))
    feature: Mapped[str] = mapped_column(String(120))
    severity: Mapped[str] = mapped_column(String(40), default="Warning")
    message: Mapped[str] = mapped_column(Text)
    impact: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(40), default="Open")


class QueueTask(Base, TimestampMixin):
    __tablename__ = "queue_tasks"
    id: Mapped[int] = mapped_column(primary_key=True)
    queue_name: Mapped[str] = mapped_column(String(120), index=True)
    task_type: Mapped[str] = mapped_column(String(120))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(40), default="Queued", index=True)
    scheduled_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    locked_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    lease_expires_at: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    fencing_token: Mapped[int] = mapped_column(Integer, default=0)
    delivery_revision: Mapped[int] = mapped_column(Integer, default=1)
    redis_stream_id: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    redis_claimed_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    completed_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class QueueOutbox(Base, TimestampMixin):
    __tablename__ = "queue_outbox"
    __table_args__ = (UniqueConstraint("queue_task_id", "event_type", "delivery_revision", name="uq_queue_outbox_task_event_revision"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    queue_task_id: Mapped[int] = mapped_column(ForeignKey("queue_tasks.id", ondelete="CASCADE"), index=True)
    event_type: Mapped[str] = mapped_column(String(80), default="queue.publish", index=True)
    delivery_revision: Mapped[int] = mapped_column(Integer, default=1)
    stream_name: Mapped[str] = mapped_column(String(160), index=True)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    published_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    redis_stream_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class QueueDlqRecord(Base, TimestampMixin):
    __tablename__ = "queue_dlq_records"
    __table_args__ = (UniqueConstraint("stream_name", "redis_stream_id", name="uq_queue_dlq_stream_message"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    stream_name: Mapped[str] = mapped_column(String(160), index=True)
    redis_stream_id: Mapped[str] = mapped_column(String(120), index=True)
    dlq_stream_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    queue_task_id: Mapped[int | None] = mapped_column(ForeignKey("queue_tasks.id", ondelete="SET NULL"), nullable=True, index=True)
    outbox_id: Mapped[int | None] = mapped_column(ForeignKey("queue_outbox.id", ondelete="SET NULL"), nullable=True, index=True)
    delivery_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reason: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(40), default="Open", index=True)
    db_write_status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    redis_dlq_status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    ack_status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    replay_started_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    replayed_task_id: Mapped[int | None] = mapped_column(ForeignKey("queue_tasks.id", ondelete="SET NULL"), nullable=True)
    resolved_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class QueueReplayRecord(Base, TimestampMixin):
    __tablename__ = "queue_replay_records"
    __table_args__ = (
        UniqueConstraint("replay_request_key", name="uq_queue_replay_request_key"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    replay_request_key: Mapped[str] = mapped_column(String(255), index=True)
    queue_task_id: Mapped[int] = mapped_column(ForeignKey("queue_tasks.id", ondelete="CASCADE"), index=True)
    dlq_record_id: Mapped[int | None] = mapped_column(ForeignKey("queue_dlq_records.id", ondelete="SET NULL"), nullable=True, index=True)
    expected_delivery_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    replayed_delivery_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    replayed_outbox_id: Mapped[int | None] = mapped_column(ForeignKey("queue_outbox.id", ondelete="SET NULL"), nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class QueueRecoveryCursor(Base, TimestampMixin):
    __tablename__ = "queue_recovery_cursors"
    name: Mapped[str] = mapped_column(String(120), primary_key=True)
    cursor_id: Mapped[int] = mapped_column(Integer, default=0)
    wrapped_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class BusinessFlowState(Base, TimestampMixin):
    __tablename__ = "business_flow_states"
    __table_args__ = (UniqueConstraint("candidate_id", "job_id", "flow_key", name="uq_business_flow_scope"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("candidates.id"), index=True)
    job_id: Mapped[int | None] = mapped_column(ForeignKey("jobs.id"), nullable=True, index=True)
    application_id: Mapped[int | None] = mapped_column(ForeignKey("applications.id"), nullable=True, index=True)
    conversation_id: Mapped[int | None] = mapped_column(ForeignKey("conversations.id"), nullable=True, index=True)
    flow_key: Mapped[str] = mapped_column(String(120), default="default")
    stage: Mapped[str] = mapped_column(String(80), default="received", index=True)
    status: Mapped[str] = mapped_column(String(40), default="Active", index=True)
    rule_version: Mapped[str] = mapped_column(String(80), default="2026-10-04")
    config_version: Mapped[str | None] = mapped_column(String(80), nullable=True)
    revision: Mapped[int] = mapped_column(Integer, default=1)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    last_event_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)


class BusinessEvent(Base, TimestampMixin):
    __tablename__ = "business_events"
    __table_args__ = (UniqueConstraint("event_key", name="uq_business_event_key"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    event_key: Mapped[str] = mapped_column(String(255), index=True)
    entity_type: Mapped[str] = mapped_column(String(120), index=True)
    entity_id: Mapped[str] = mapped_column(String(120), index=True)
    from_state: Mapped[str | None] = mapped_column(String(80), nullable=True)
    to_state: Mapped[str] = mapped_column(String(80), index=True)
    status: Mapped[str] = mapped_column(String(40), default="Applied", index=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)


class DataRetentionRun(Base, TimestampMixin):
    __tablename__ = "data_retention_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    policy_name: Mapped[str] = mapped_column(String(120), index=True)
    target_table: Mapped[str] = mapped_column(String(120), index=True)
    strategy: Mapped[str] = mapped_column(String(40), default="delete")
    cutoff_at: Mapped[str] = mapped_column(String(40), index=True)
    rows_scanned: Mapped[int] = mapped_column(Integer, default=0)
    rows_changed: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(40), default="Completed", index=True)
    details: Mapped[dict] = mapped_column(JSON, default=dict)


class ExternalRetentionPurge(Base, TimestampMixin):
    __tablename__ = "external_retention_purges"
    __table_args__ = (UniqueConstraint("provider", "object_key", "purge_type", name="uq_external_purge_scope"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    provider: Mapped[str] = mapped_column(String(80), index=True)
    object_key: Mapped[str] = mapped_column(String(500), index=True)
    purge_type: Mapped[str] = mapped_column(String(80), default="object")
    legal_hold: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(40), default="Pending", index=True)
    requested_at: Mapped[str] = mapped_column(String(40), default=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z")
    confirmed_at: Mapped[str | None] = mapped_column(String(40), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)


class Notification(Base, TimestampMixin):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(primary_key=True)
    channel: Mapped[str] = mapped_column(String(80))
    severity: Mapped[str] = mapped_column(String(40), default="Info")
    title: Mapped[str] = mapped_column(String(160))
    body: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(40), default="Unread")
    target_role: Mapped[str | None] = mapped_column(String(40), nullable=True)
