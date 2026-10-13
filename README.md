# RecruitOps AI

## 2026-10-04 Capability Update

- Second pass fixes: candidate deletion preserves document purge tasks and records deletion only after purge proof; duplicate PDFs can be linked to different candidates; scanned OCR path uses PDF rasterization plus Tesseract TSV confidence; voice classification handles negative/uncertain replies before positive terms and DNC callbacks create DNC records; calendar free/busy/read-back now fails closed and rejects inline credentials; confirmed interviews are separate from verified attendance.
- Preserved the prior fixes: unified candidate inbound routing through `process_inbound_candidate_message`, interview replies handled before general answer/application routing, unclear interview replies moved to manual review with reminder/no-show cancellation, no-show latest-reply recheck, and retention continuation batches.
- Added local, replaceable adapter paths for phone, calendar, OCR and external purge:
  - `phone.dial` now sends the disclosure-augmented script to the provider and stores the same script in `PhoneCallLog` and evidence. Provider acceptance alone is not disclosure proof; success requires signed callback/provider evidence that disclosure playback completed. Current transcript handling is text classification, not a complete live AI phone agent.
  - `calendar.book_interview` creates idempotent booking tasks with configurable workdays, hourly slots, timezone, duration, candidate availability and two-interviewer availability rules. Google Calendar and Microsoft Graph adapter paths exist, but real bookings remain `Unverified` unless provider read-back confirms the event.
  - `/api/resumes/ocr` streams PDF uploads with size limits, validates PDF magic/structure, stores files as `StoredDocument`, and queues `resume.ocr_pdf` by document id only. Embedded-text PDFs and scanned PDFs are split; scanned PDFs require a configured OCR engine/provider, otherwise they go to manual review.
  - `/api/privacy/external-purges` records backup/object/archive purge requests, legal holds, retries and dry-run evidence. Dry-runs are marked `Dry Run - Not Deleted`; live purge is marked deleted only after provider read-back evidence.
- Redis distributed queue support now has PostgreSQL source-of-truth outbox, Redis Streams publish/consume, durable recovery cursor, queryable DLQ records, step-level DLQ recovery state, guarded replay, lease/fencing checks, bounded DB-driven repair, and stale DLQ pointer recovery that rewrites Redis DLQ evidence before ACK. Production enablement still requires Docker Postgres+Redis soak and customer environment monitoring evidence.
- `/api/operations/metrics` exposes queue depth/status, oldest active task, manual review count, open platform failure counts, retention backlog signals and external purge pending count.

Local tests prove code contracts only. They do not prove FB/LINE/518/ChickPT/phone/calendar/OCR production acceptance.

## 2026-10-04 Third Pass Update

- Added persistent `BusinessFlowState` tracking and a Flow UI view. Application status changes now create versioned/revisioned flow evidence instead of relying only on scattered queue tasks.
- Added Redis queue foundation using PostgreSQL as source of truth: transactional `QueueOutbox`, Redis Streams publisher helpers, consumer-group helpers, reconcile repair, task lease/fencing fields, shared atomic claim for manual run/workers, durable DLQ records, idempotent Redis-DLQ/ACK recovery, and guarded DLQ replay.
- Duplicate resume PDFs now produce independent stored object versions and paths. Deleting one candidate/document no longer deletes another candidate's same-SHA file.
- OCR mixed PDFs now OCR only pages without embedded text by creating a temporary subset PDF, preserving page-order evidence and avoiding duplicate text.
- Voice classification now treats negative/uncertain phrases before positive phrases and handles `yes, no problem` correctly.
- Queue UI now exposes outbox, Redis stream id, lease expiry and fencing token.

Still not customer-accepted: live Redis/Postgres/Docker soak, live provider credentials, phone media/STT/TTS runtime, Google/Outlook OAuth/webhooks, customer platform read-back, 10,000 full-workflow load, worker soak, backup restore and purge drills.

## 2026-10-08 Redis Reliability Addendum

- Redis DLQ ACK is now gated on exact Redis evidence: a stored `dlq_stream_id` or Redis dedupe index must still resolve via exact `XRANGE id id` before the original failed message is ACKed.
- Stale/trimmed DLQ pointers are treated as critical recovery events. The worker writes replacement DLQ evidence through an atomic Redis script that checks the dedupe index and performs `XADD` + index update in one Redis operation, then ACKs only after DB and Redis evidence agree.
- Legacy deep DLQ scans are no longer used for uniqueness. PostgreSQL `queue_dlq_records` remains the durable source of truth, while Redis keeps a bounded exact index per failed source stream/message. Migration `0022_queue_dlq_pointer_recovery` adds lookup indexes for DLQ recovery views.
- Local regression evidence: `python -m pytest backend/tests/test_core.py -k "redis or dlq" -q` passed on 2026-10-08 with 23 Redis/DLQ tests, including stale index/trimmed pointer recovery, ACK failure recovery, guarded replay, cursor repair, and multi-worker claim coverage.

## 2026-10-08 Full Repair Addendum

- Settings now use Pydantic `Settings` consistently for scheduler, Redis, queue lease, retention cleanup and OCR runtime values. Health output includes the actual worker mode, queues, Redis consumer group/pending/DLQ settings, lease seconds and OCR engine/limits.
- `replay_task` now uses an atomic conditional update for status, retry budget and delivery revision before creating the new outbox entry. A stale manager session can no longer overwrite a task that another worker has already claimed as `Running`.
- Added `docs/REQUIREMENTS_TRACEABILITY_20261008.md` as the item-by-item requirement/code/test/blocker matrix for the 24-point customer checklist.
- Local regression evidence: `python -m pytest backend/tests/test_core.py -q` passed with 119 tests. Docker/PostgreSQL/Redis integration, real Redis Lua acceptance, 10,000 full-flow load, worker soak, backup restore/purge, and authorized platform/phone/calendar/OCR E2E remain unverified on this host.

RecruitOps AI 是招募流程自動化系統，涵蓋職缺、履歷匯入、候選人溝通、AI 模型路由、平台 connector、queue worker、電話 callback、稽核與例外處理。

## Local Start

SQLite 只可用於本機開發與測試。第一次啟動或套用更新前，先執行 Alembic migration。

```powershell
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
alembic upgrade head
uvicorn backend.app.main:app --reload
```

Open <http://localhost:8000>. API docs are at <http://localhost:8000/docs>.

If demo data is required, set `DEMO_MODE=true` and `DEMO_ADMIN_PASSWORD` only in a local `.env`.

## Production Rules

- Use PostgreSQL only. Do not use SQLite as the production database.
- Set `APP_ENV=production` and `AUTO_CREATE_TABLES=false`.
- Run `alembic upgrade head` before backend or worker startup.
- Do not rely on SQLAlchemy `create_all` to update an existing schema.
- Store API keys, phone tokens, browser sessions, cookies, mailbox passwords, resumes, email archives, screenshots and recordings outside git and outside QueueTask payloads.

## Worker

```powershell
python -m backend.app.workers.scheduler
```

The default queue is PostgreSQL/SQLAlchemy backed. Redis Streams can be enabled by configuration for distributed workers; PostgreSQL remains the source of truth through `queue_outbox`, `queue_recovery_cursors`, and `queue_dlq_records`. DLQ records track DB save, Redis DLQ write, original stream ACK, replay status, and controlled evidence for invalid/missing task or outbox ids. Original Redis messages must not be ACKed until the DLQ record is saved and its Redis DLQ evidence has been exact-read verified or rewritten.

Implemented worker task families as of 2026-10-02 include platform post/message send and verification, 518 email polling/ack, platform resume/message sync, inbound message AI processing, required-question no-reply follow-up, three-day unsuitable rejection, interview reminders/no-show checks, regional/talent-pool messaging, phone dial handoff, and live-DB retention cleanup.

## Recruiting Flow Scope

- FB/LINE inbound identity continuity is implemented through `PlatformIdentityMap`. FB public comments, Messenger PSIDs, LINE group/room users, and LINE 1:1 users can be linked back to the same candidate/job/conversation when the external user and job context are unambiguous.
- Candidate-specific outbound messages require a verified private recipient mapping. Public FB comments and LINE group/room mappings are refused and moved to Manual Review instead of silently falling back to public targets.
- `Unsuitable` / `不適合` application status starts a distinct `flow.unsuitable_rejection_due` timer. It is separate from the existing required-question `flow.follow_up_inactive` timer.
- `CandidateScoring` can use a configured AI route, but HR remains the final decision maker. Low confidence, sensitive/non-job-related risk flags, provider/schema failure, or missing route falls back to rules scoring.
- Interview automation provides timezone-aware reminders, no-response/reschedule prompts, and adapter-backed calendar booking gates. Google/Outlook production acceptance still requires customer OAuth credentials and live read-back evidence.
- AI phone calls are not complete in this build. `phone.dial` requires a real provider config and signed callbacks; STT/TTS/voice-agent runtime must be connected through a provider adapter before claiming live AI calling support.

## Packaging

Use the packaging script for delivery ZIPs:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/package.ps1
```

The script refuses packages containing `.env`, SQLite databases, cache directories, `.pyc`, local sessions, cookies, token files, inbound mail archives, real resumes, screenshots, or other local private artifacts. The checked-in `recruitment.db` is a legacy local SQLite file used only for migration compatibility testing and is excluded from delivery ZIPs.

## Platform Automation

Each external platform must be configured through `PlatformAccount` with authorized credentials/session state and external ID mappings. Browser sessions must live outside the repository. If CAPTCHA, login expiry, unknown layout, 403/429, permission failure, or read-back uncertainty occurs, the worker pauses the account action and leaves evidence for human exception handling instead of reporting success.

## 2026-10-02 Delivery Notes

- Platform sending separates internal conversation keys from actual recipient IDs. Facebook Messenger uses verified PSID, LINE push uses verified `userId`, and FB comment IDs are used only for the private-reply endpoint.
- FB public comments create one idempotent private-reply invitation task; candidate-specific questions, follow-ups, rejection notices, reminders, phone fallback, and recall messages still require verified private recipient mapping.
- Interview automation includes structured reply classification for confirm, decline, cancel, reschedule, unrelated, and uncertain. Negated or contradictory confirmations are not treated as confirmed; low confidence routes to Manual Review.
- `privacy.retention_cleanup` is now periodically scheduled by the DB worker with idempotent bucket keys. It cleans only the live application database, not external backups or archive stores.
- Delivery ZIPs are built with `scripts/package.ps1`, exclude prior ZIP/DB/cache/private artifacts, and validate portable `/` entry paths.
