import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_serializer, field_validator


class JobCreate(BaseModel):
    title: str
    company_id: int | None = None
    branch_id: int | None = None
    location: str
    area: str
    job_type: str = "派遣"
    shift: str
    hours: str
    salary_text: str
    hourly_wage: float | None = None
    monthly_salary: float | None = None
    bonuses: str | None = None
    allowances: str | None = None
    overtime_policy: str | None = None
    description: str
    requirements: str
    required_headcount: int = 1
    urgency: str = "Normal"
    start_date: str | None = None
    end_date: str | None = None
    contact_name: str
    contact_method: str
    source: str = "manual"
    status: str = "Draft"
    required_questions: list[str] = Field(default_factory=list)


class JobOut(JobCreate):
    id: int
    version: int

    model_config = {"from_attributes": True}


class JobUpdate(BaseModel):
    title: str | None = None
    location: str | None = None
    area: str | None = None
    job_type: str | None = None
    shift: str | None = None
    hours: str | None = None
    salary_text: str | None = None
    hourly_wage: float | None = None
    monthly_salary: float | None = None
    bonuses: str | None = None
    allowances: str | None = None
    overtime_policy: str | None = None
    description: str | None = None
    requirements: str | None = None
    required_headcount: int | None = None
    urgency: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    contact_name: str | None = None
    contact_method: str | None = None
    status: str | None = None
    required_questions: list[str] | None = None


class CandidateCreate(BaseModel):
    name: str
    phone: str | None = None
    email: str | None = None
    region: str | None = None
    skills: list[str] = Field(default_factory=list)
    licenses: list[str] = Field(default_factory=list)
    expected_salary: str | None = None
    preferred_area: str | None = None
    preferred_shift: str | None = None
    source_platform: str = "manual"
    platform_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    notes: str | None = None


class CandidateOut(CandidateCreate):
    id: int
    status: str

    model_config = {"from_attributes": True}


class CandidateUpdate(BaseModel):
    name: str | None = None
    phone: str | None = None
    email: str | None = None
    region: str | None = None
    skills: list[str] | None = None
    licenses: list[str] | None = None
    expected_salary: str | None = None
    preferred_area: str | None = None
    preferred_shift: str | None = None
    status: str | None = None
    tags: list[str] | None = None
    notes: str | None = None


class CopywritingRequest(BaseModel):
    job_id: int
    tone: str = "recruiting"


class CopywritingOut(BaseModel):
    job_id: int
    titles: list[str]
    facebook_long: str
    facebook_short: str
    recruit518: str
    chickpt: str
    line: str
    dm: str
    ai_notice: str


class AIProviderCreate(BaseModel):
    name: str
    provider_type: str
    base_url: str
    endpoint: str = "/chat/completions"
    api_key: str | None = None
    model: str
    supports_text: bool = True
    supports_vision: bool = False
    supports_json_schema: bool = False
    supports_tools: bool = False
    context_window: int = 8192
    max_output: int = 1024
    temperature: float = 0.2
    timeout_seconds: int = 30
    retry_policy: dict = Field(default_factory=dict)
    cost_policy: dict = Field(default_factory=dict)
    enabled: bool = True


class AIProviderOut(BaseModel):
    id: int
    name: str
    provider_type: str
    base_url: str
    endpoint: str
    model: str
    supports_text: bool
    supports_vision: bool
    supports_json_schema: bool
    supports_tools: bool
    context_window: int
    max_output: int
    temperature: float
    timeout_seconds: int
    retry_policy: dict
    cost_policy: dict
    enabled: bool
    last_status: str
    last_latency_ms: int | None
    last_tested_at: str | None
    circuit_open_until: str | None
    consecutive_failures: int
    test_http_status: int | None = None
    test_message: str | None = None

    model_config = {"from_attributes": True}


class PostCreate(BaseModel):
    job_id: int
    platform: str
    content: str
    account_id: int | None = None
    group_id: int | None = None
    scheduled_at: str | None = None


class PostOut(PostCreate):
    id: int
    status: str
    post_url: str | None
    platform_post_id: str | None
    verified_at: str | None
    error_reason: str | None
    retry_count: int
    metrics: dict

    model_config = {"from_attributes": True}


class UsageSummary(BaseModel):
    today_tokens: int
    month_tokens: int
    estimated_cost: float
    by_provider: dict[str, int]
    status_counts: dict[str, int]
    usage_source: dict[str, int] = Field(default_factory=dict)


class LoginRequest(BaseModel):
    email: str
    password: str


class UserOut(BaseModel):
    id: int
    email: str
    name: str
    role: str

    model_config = {"from_attributes": True}


class QueueTaskCreate(BaseModel):
    queue_name: str = "default"
    task_type: str
    payload: dict = Field(default_factory=dict)
    idempotency_key: str | None = None
    scheduled_at: str | None = None
    max_retries: int = 3


class QueueTaskOut(QueueTaskCreate):
    id: int
    status: str
    locked_by: str | None = None
    lease_expires_at: str | None = None
    fencing_token: int = 0
    delivery_revision: int = 1
    redis_stream_id: str | None = None
    retry_count: int
    last_error: str | None

    model_config = {"from_attributes": True}

    @field_serializer("payload")
    def redact_payload(self, payload: dict) -> dict:
        return redact_secret_values(payload or {})


class QueueDlqRecordOut(BaseModel):
    id: int
    stream_name: str
    redis_stream_id: str
    dlq_stream_id: str | None
    queue_task_id: int | None
    outbox_id: int | None
    delivery_revision: int | None
    reason: str
    status: str
    db_write_status: str
    redis_dlq_status: str
    ack_status: str
    last_error: str | None
    replay_started_at: str | None
    replayed_task_id: int | None
    resolved_at: str | None
    metadata_json: dict

    model_config = {"from_attributes": True}


def redact_secret_values(value):
    if isinstance(value, dict):
        output = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if any(secret in lowered for secret in ["password", "token", "secret", "key", "cookie", "session"]):
                output[key] = "***configured***" if item else None
            else:
                output[key] = redact_secret_values(item)
        return output
    if isinstance(value, list):
        return [redact_secret_values(item) for item in value]
    return value


class ConversationCreate(BaseModel):
    channel: str
    candidate_id: int | None = None
    job_id: int | None = None
    ai_mode: str = "suggest"


class ConversationOut(ConversationCreate):
    id: int
    status: str

    model_config = {"from_attributes": True}


class MessageCreate(BaseModel):
    conversation_id: int
    sender_type: str
    body: str
    metadata_json: dict = Field(default_factory=dict)
    send_external: bool = False


class MessageOut(MessageCreate):
    id: int
    delivery_status: str
    platform_message_id: str | None
    external_status_checked_at: str | None
    candidate_read_status: str
    candidate_read_at: str | None

    model_config = {"from_attributes": True}


class PlatformIdentityMapCreate(BaseModel):
    platform: str
    account_id: int | None = None
    entity_type: str
    external_id: str
    candidate_id: int | None = None
    conversation_id: int | None = None
    job_id: int | None = None
    external_candidate_id: str | None = None
    external_conversation_id: str | None = None
    external_job_id: str | None = None
    metadata_json: dict = Field(default_factory=dict)


class PlatformIdentityMapOut(PlatformIdentityMapCreate):
    id: int

    model_config = {"from_attributes": True}


class CandidateQuestionAnswerOut(BaseModel):
    id: int
    candidate_id: int
    job_id: int
    application_id: int | None
    conversation_id: int | None
    question_key: str
    question_text: str
    answer_text: str | None
    answer_status: str
    evidence_message_id: int | None
    asked_at: str | None
    answered_at: str | None
    metadata_json: dict

    model_config = {"from_attributes": True}


class AuditLogOut(BaseModel):
    id: int
    actor_user_id: int | None
    action: str
    entity_type: str
    entity_id: str | None
    details: dict

    model_config = {"from_attributes": True}


class TaskModelRouteCreate(BaseModel):
    task_name: str
    primary_provider_id: int | None = None
    fallback_provider_ids: list[int] = Field(default_factory=list)
    budget_policy: dict = Field(default_factory=dict)


class TaskModelRouteOut(TaskModelRouteCreate):
    id: int

    model_config = {"from_attributes": True}


class AIRunRequest(BaseModel):
    task_name: str
    prompt: str
    candidate_id: int | None = None
    job_id: int | None = None


class AIRunResponse(BaseModel):
    task_name: str
    provider: str
    model: str
    status: str
    output: str
    attempts: list[dict]


class PromptCreate(BaseModel):
    name: str
    purpose: str
    system_prompt: str
    template: str
    model_hint: str | None = None
    temperature: float = 0.2


class PromptOut(BaseModel):
    id: int
    name: str
    purpose: str
    active_version: int

    model_config = {"from_attributes": True}


class PromptVersionOut(BaseModel):
    id: int
    prompt_id: int
    version: int
    system_prompt: str
    template: str
    model_hint: str | None
    temperature: float

    model_config = {"from_attributes": True}


class SystemErrorOut(BaseModel):
    id: int
    service: str
    feature: str
    severity: str
    message: str
    impact: str | None
    retry_count: int
    status: str

    model_config = {"from_attributes": True}


class ApplicationCreate(BaseModel):
    candidate_id: int
    job_id: int
    status: str = "New"


class ApplicationOut(ApplicationCreate):
    id: int
    ai_match_score: float | None
    ai_notes: str | None

    model_config = {"from_attributes": True}


class ApplicationStatusUpdate(BaseModel):
    status: str
    reason: str | None = None


class ApplicationStatusHistoryOut(BaseModel):
    id: int
    application_id: int
    from_status: str | None
    to_status: str
    changed_by: int | None
    reason: str | None

    model_config = {"from_attributes": True}


class InterviewCreate(BaseModel):
    candidate_id: int
    job_id: int
    interview_date: str
    interview_time: str
    timezone: str = "Asia/Taipei"
    location: str | None = None
    method: str = "phone"
    notes: str | None = None
    reminder_policy: list[str] = Field(default_factory=lambda: ["24h", "3h", "1h"])

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timezone must be a valid IANA timezone such as Asia/Taipei") from exc
        return value

    @field_validator("reminder_policy")
    @classmethod
    def validate_reminder_policy(cls, value: list[str]) -> list[str]:
        for item in value:
            normalized = str(item).strip().lower()
            if not normalized.endswith(("m", "h")):
                raise ValueError("reminder_policy values must use minutes or hours, for example 30m, 3h, 24h")
            try:
                amount = float(normalized[:-1])
            except ValueError as exc:
                raise ValueError("reminder_policy values must start with a number") from exc
            seconds = amount * (60 if normalized.endswith("m") else 3600)
            if seconds < 300 or seconds > 30 * 24 * 3600:
                raise ValueError("reminder_policy values must be between 5 minutes and 30 days")
        return value


class InterviewOut(InterviewCreate):
    id: int
    hr_user_id: int | None
    status: str
    attendance_status: str = "Not Arrived"
    attended_at: str | None = None
    attendance_evidence: dict = Field(default_factory=dict)

    model_config = {"from_attributes": True}


class MatchRequest(BaseModel):
    candidate_id: int
    job_id: int


class MatchOut(BaseModel):
    id: int
    candidate_id: int
    job_id: int
    score: float
    signals: dict
    missing_info: list[str]
    recruiter_notes: str | None
    status: str

    model_config = {"from_attributes": True}


class AnalyticsSummary(BaseModel):
    jobs: int
    candidates: int
    applications: int
    interviews: int
    hired: int
    talent_pool: int
    by_platform: dict[str, int]
    by_area: dict[str, int]
    funnel: dict[str, int]


class DoNotContactCreate(BaseModel):
    candidate_id: int
    reason: str
    evidence: str | None = None
    can_lift: bool = True


class DoNotContactOut(DoNotContactCreate):
    id: int
    created_by: int | None
    status: str

    model_config = {"from_attributes": True}


class DoNotContactLift(BaseModel):
    reason: str


class RecruitmentBlacklistCreate(BaseModel):
    candidate_id: int | None = None
    phone: str | None = None
    email: str | None = None
    reason: str
    rule_code: str = "customer_rule"
    scope_type: str = "candidate"
    scope_id: str | None = None
    evidence: str | None = None

    @field_validator("scope_type")
    @classmethod
    def valid_scope_type(cls, value: str) -> str:
        normalized = (value or "candidate").strip().lower()
        if normalized not in {"candidate", "job", "global", "legacy"}:
            raise ValueError("scope_type must be candidate, job, global, or legacy")
        return normalized

    @field_validator("scope_id")
    @classmethod
    def normalize_scope_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = str(value).strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return str(int(stripped))
        return stripped


class RecruitmentBlacklistOut(RecruitmentBlacklistCreate):
    id: int
    created_by: int | None
    status: str

    model_config = {"from_attributes": True}


class QueueReplayRequest(BaseModel):
    expected_delivery_revision: int = Field(..., ge=1, le=2_147_483_647)
    replay_request_key: str = Field(..., min_length=12, max_length=120)

    @field_validator("replay_request_key")
    @classmethod
    def valid_replay_request_key(cls, value: str) -> str:
        key = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._-]{11,119}", key):
            raise ValueError("replay_request_key must be 12-120 chars using letters, numbers, colon, dot, underscore, or hyphen")
        return key


class CandidateExport(BaseModel):
    candidate: dict
    resumes: list[dict] = Field(default_factory=list)
    stored_documents: list[dict] = Field(default_factory=list)
    applications: list[dict]
    interviews: list[dict]
    matches: list[dict]
    conversations: list[dict]
    messages: list[dict]


class ResumeParseRequest(BaseModel):
    raw_text: str
    source: str = "manual"
    file_name: str | None = None
    create_candidate: bool = True


class ResumeOut(BaseModel):
    id: int
    candidate_id: int | None
    source: str
    file_name: str | None
    raw_text: str
    parsed_json: dict
    status: str

    model_config = {"from_attributes": True}


class BulkCandidateImport(BaseModel):
    candidates: list[CandidateCreate]


class BulkCandidateImportResult(BaseModel):
    created: int
    deduplicated: int
    candidate_ids: list[int]


class PlatformAccountCreate(BaseModel):
    platform: str
    display_name: str
    status: str = "Not Configured"
    config: dict = Field(default_factory=dict)
    capabilities: list[str] = Field(default_factory=list)
    work_windows: list[str] = Field(default_factory=list)


class PlatformAccountOut(PlatformAccountCreate):
    id: int
    last_success_at: str | None
    last_failure_reason: str | None

    model_config = {"from_attributes": True}

    @field_serializer("config")
    def redact_config(self, config: dict) -> dict:
        redacted = {}
        for key, value in (config or {}).items():
            lowered = key.lower()
            if any(secret in lowered for secret in ["password", "token", "secret", "key", "cookie", "session"]):
                redacted[key] = "***configured***" if value else None
            else:
                redacted[key] = value
        return redacted


class PlatformGroupCreate(BaseModel):
    platform_account_id: int
    name: str
    url: str
    area: str
    group_type: str = "recruiting"
    allowed_windows: list[str] = Field(default_factory=list)
    posting_frequency: str = "manual"
    notes: str | None = None
    status: str = "Active"


class PlatformGroupOut(PlatformGroupCreate):
    id: int
    last_posted_at: str | None
    success_rate: float | None

    model_config = {"from_attributes": True}


class PostMetricsUpdate(BaseModel):
    status: str | None = None
    post_url: str | None = None
    platform_post_id: str | None = None
    verified_at: str | None = None
    metrics: dict = Field(default_factory=dict)
    error_reason: str | None = None


class InboundEmailImport(BaseModel):
    message_id: str
    sender: str
    received_at: str
    subject: str
    raw_text: str
    attachment_names: list[str] = Field(default_factory=list)
    source_platform: str = "518"
    job_id: int | None = None
    mailbox: str | None = None
    imap_uid: str | None = None
    uid_validity: str | None = None


class InboundEmailOut(InboundEmailImport):
    id: int
    candidate_id: int | None
    application_id: int | None
    import_status: str
    parse_error: str | None
    platform_link: str | None

    model_config = {"from_attributes": True}


class PlatformAutomationRunOut(BaseModel):
    id: int
    platform: str
    capability: str
    account_id: int | None
    queue_task_id: int | None
    status: str
    started_at: str | None
    finished_at: str | None
    platform_entity_id: str | None
    evidence: dict
    error_reason: str | None
    screenshot_path: str | None

    model_config = {"from_attributes": True}


class ExternalActionLogOut(BaseModel):
    id: int
    action_type: str
    platform: str
    account_id: int | None
    target_account: str | None
    platform_object_id: str | None
    queue_task_id: int | None
    status: str
    attempted_at: str
    verified_at: str | None
    result: str | None
    evidence: dict

    model_config = {"from_attributes": True}


class PhoneCallLogOut(BaseModel):
    id: int
    candidate_id: int
    job_id: int | None
    conversation_id: int | None
    provider: str | None
    external_call_id: str | None
    dial_status: str
    call_result: str | None
    script: str | None
    transcript: str | None
    recording_url: str | None
    evidence: dict
    started_at: str | None
    finished_at: str | None

    model_config = {"from_attributes": True}


class CalendarBookRequest(BaseModel):
    interview_id: int
    calendar_config: dict = Field(default_factory=dict)


class ExternalPurgeRequest(BaseModel):
    provider: str
    object_key: str
    purge_type: str = "object"
    legal_hold: bool = False
    dry_run: bool = True
