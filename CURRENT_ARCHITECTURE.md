# Current Architecture

Last updated: 2026-10-02

## Backend

- FastAPI API with SQLAlchemy models and Alembic migrations through `0011_identity_status_retention`.
- Database-backed auth sessions, CSRF checks, role guards, audit logs, privacy export/delete, and retention cleanup run records.
- Recruiting core: jobs, copywriting, candidates, resumes, applications, application status history, match suggestions, interviews, conversations, messages, posts, platform accounts/groups, queue tasks, system errors, notifications, platform identity maps, platform webhook events, and external action logs.
- Application decisions store `decision_source` and `decision_event_key`. Status history stores source and idempotency event keys.
- External identity continuity is represented by `PlatformIdentityMap`, linking external candidate/conversation/job IDs back to candidate, application/job, and conversation.

## Workers

`backend.app.workers.scheduler` polls DB queue tasks. Redis is available in Docker compose only as future infrastructure; the active queue is SQL-backed.

Supported task types include:

- `platform.create_post`, `post.verify`
- `message.send`, `message.private_reply`, `message.verify`
- `platform.fetch_resume`, `platform.sync_messages`
- `email.poll_518`, `email.ack_518`
- `flow.process_inbound_message`
- `flow.follow_up_inactive`
- `flow.unsuitable_rejection_due`
- `flow.interview_reminder`, `flow.interview_no_show_check`
- `flow.regional_notify`, `flow.shortage_recall`
- `phone.dial`
- `privacy.retention_cleanup`

Unsupported live tasks fail clearly and move through retry/manual-review states; they are not marked successful. Stale running tasks older than 15 minutes are returned to retry.

## Platform Adapters

- Registry maps platforms to explicit adapters instead of a semi-auto fallback.
- Facebook and LINE use official API/webhook paths when account config supplies credentials. Signed webhook ingest is supported locally; authorized production evidence is still required.
- Candidate-specific outbound messages must resolve to a verified private recipient. Public FB comments and LINE group/room mappings are not accepted as private targets.
- Sending separates internal conversation keys from actual recipient IDs. Facebook private replies use original comment IDs only on the private-reply endpoint; Facebook Messenger sends use verified PSIDs; LINE push sends use verified `userId`.
- ChickPT and 518 page operations pause until authorized persistent browser sessions/selectors are provided.
- Email has a dedicated 518 inbound import path.

## AI

- Provider routing supports OpenAI-compatible, Anthropic, Gemini, Ollama, LM Studio/vLLM style services through common schema validation and fallback logic.
- `CandidateScoring` is a configurable route. Only high-confidence, job-related, schema-valid output is used; otherwise rules scoring is retained and marked as fallback/manual review.
- `supports_vision` is stored as provider metadata only. It does not mean screenshot understanding or general computer control is implemented.
- AI phone calls are not complete without a configured telephony/STT/TTS provider and signed callback evidence.
- Interview reply classification can use the `InterviewReplyClassification` AI route when configured and otherwise falls back to conservative deterministic classification. Low confidence, negation, or contradiction is routed to Manual Review.

## Privacy And Retention

- Candidate delete anonymizes candidate fields, resumes, messages, identity maps, queued candidate payloads, and candidate-linked AI request text.
- `privacy.retention_cleanup` can batch mask/delete old live DB records for inbound email raw text, resume raw text, webhook payload evidence, messages, AI request prompt/output, external action evidence, completed queue tasks, and closed system errors.
- `backend.app.workers.scheduler` periodically creates idempotent retention cleanup tasks. Batch size and retry count are configurable through environment variables.
- Retention cleanup does not purge backups or external archives. Backup deletion must be handled by the backup system and must not be claimed as completed by this app.

## Production Notes

- Production refuses `AUTO_CREATE_TABLES=true`; run `alembic upgrade head`.
- Keep credentials and persistent browser session files outside Git and logs.
- Live external validation requires customer-supplied test accounts, mailbox samples, provider credentials, platform IDs, and official authorization scopes.
