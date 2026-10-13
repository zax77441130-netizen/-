# Validation Report

Date: 2026-10-02

## 2026-10-04 Second Addendum

### Commands Actually Run

- `python -m pytest backend/tests/test_core.py -q`
  - Result: `65 passed, 2187 warnings in 7.77s`.
- `python -m pytest backend/tests/test_core.py -q -k "delete_candidate_preserves or duplicate_pdf or tesseract_ocr or voice_negative or phone_callback_dnc or running_task_cannot or confirmed_interview or calendar_rejects_inline or google_freebusy or microsoft_graph or calendar_readback_missing or calendar_booking_is_idempotent"`
  - Result: `12 passed, 53 deselected`.
- `python -m compileall backend -q`
  - Result: passed.
- `node --check frontend/app.js`
  - Result: passed.
- Temporary SQLite migration from empty DB through head
  - Result: passed; reached `0014_interview_attendance`.
- Local HTTP/UI smoke with uvicorn on `127.0.0.1:8765`
  - Result: `/api/health` ready and `/` returned HTTP 200, length 1634.
- Local 10,000-row bulk import smoke against a temporary SQLite DB
  - Result: HTTP 200, created 10000, deduplicated 0, DB count 10000, elapsed 4.62 seconds.
- `docker --version`
  - Result: failed; `docker` command is not installed in this environment.

### Implemented And Contract-Tested Locally

- Candidate privacy deletion no longer cancels or scrubs the newly created `privacy.external_purge` task. `StoredDocument.deleted_at` is now written only after purge execution confirms deletion; purge evidence is copied into `StoredDocument.delete_audit`.
- Duplicate PDF uploads now create distinct `StoredDocument` rows and distinct OCR queue tasks, while sharing the same controlled file path by SHA. The `stored_documents.sha256` unique constraint was removed through migration `0013_document_purge_queue_safety`.
- OCR now rasterizes scanned PDFs through `pdftoppm` and reads Tesseract TSV confidence via `tesseract ... stdout ... tsv`; mixed embedded-text/scanned pages are combined only after OCR confidence passes threshold. Docker installs Poppler, Tesseract, English and Traditional Chinese language data.
- Voice text classification now handles negative, contradictory, and uncertain phrases before positive availability terms. Disclosure proof uses strict booleans/events, so string `"false"` is not accepted. DNC callback phrases create active `DoNotContact` records and stop automated success.
- Calendar rejects inline credentials in QueueTask/API payloads, validates positive slot/interview durations and bounded search days, implements Microsoft Graph free/busy, checks Google per-calendar errors, and fails closed when availability is missing or errored.
- Calendar read-back now checks event id, cancelled/deleted status, start/end timestamps, and attendee emails where applicable. Existing unverified bookings can be recovered by read-back instead of creating duplicate external events.
- Verified calendar booking is required before interview date/time/status changes and reminder rescheduling. Reminder/no-show tasks are rescheduled idempotently by true UTC interview time instead of `scheduled_at=now`.
- Interview confirmation and attendance are split. `Confirmed` no longer means arrived; HR can record `Arrived`/`No Show` through `/api/interviews/{id}/attendance`, and no-show remains active until arrival is verified.
- `/api/queue/tasks/{id}/run` now rejects `Running` tasks with HTTP 409, so a task being processed by a worker cannot be manually run again through the API.

### Still Not Implemented Or Not Verified

- Live STT/TTS, multi-turn voice agent runtime, and provider-level human transfer are NOT IMPLEMENTED; current phone logic remains provider handoff plus transcript text classification.
- Google/Outlook OAuth refresh and calendar webhook sync are NOT IMPLEMENTED. Calendar adapter calls exist, but customer OAuth/live provider E2E is NOT VERIFIED.
- Atomic cross-worker calendar slot reservation is only partially mitigated by DB task idempotency and booking overlap checks; a PostgreSQL exclusion/lock design is still NOT IMPLEMENTED.
- Frontend screens for OCR, phone settings/status, calendar reschedule/cancel, purge tracking, manual review, and alerts are NOT IMPLEMENTED in this pass.
- Full business state machine orchestration from intake through regional notification/data pool is NOT COMPLETED; existing flow pieces remain partial.
- External backup/S3/email archive purge gateway is only an HTTP/local-file contract, not a native S3/backup/email adapter. Backup restore/purge drill is NOT RUN.
- PostgreSQL migration/foreign-key verification, Docker Postgres/Redis smoke, long worker soak, real scanned OCR with installed OCR binaries, and customer-platform E2E are NOT RUN in this environment.

## 2026-10-04 Addendum

### Commands Actually Run

- `python -m compileall backend`
  - Result: passed.
- `node --check frontend\app.js`
  - Result: passed with no output.
- `pytest backend/tests/test_core.py -q`
  - Result: `55 passed, 2029 warnings in 7.60s`.
  - Notes: warnings are existing SQLAlchemy/security datetime deprecations and pytest-asyncio loop-scope configuration warnings.
- `alembic upgrade head` against a temporary SQLite database
  - Result: passed; reached `0012_calendar_ocr_purge`.
- `powershell -ExecutionPolicy Bypass -File scripts\package.ps1 -Destination 招募-20261002-收件人面試修正-full-clean.zip`
  - Result: passed; clean package created.
- `pytest backend/tests/test_core.py -q -k "interview_replies_are_consumed or interview_reschedule_and_uncertain or unrelated_interview_reply or no_show_low_confidence or retention_filters"`
  - Result: `5 passed, 42 deselected`.
  - Scope: targeted regression for unified candidate inbound routing, interview reply consumption/no-show safety, and retention cleanup backlog filtering/continuation.
- `pytest backend/tests/test_core.py -q -k "phone_provider_request_body or voice_callback_low_confidence or calendar_readback_missing or calendar_booking_is_idempotent or ocr_blank_pdf or external_purge"`
  - Result: `6 passed, 49 deselected`.
  - Scope: disclosure payload consistency, callback replay/low-confidence gates, calendar read-back gate, OCR manual review, and purge dry-run status.
- `powershell -ExecutionPolicy Bypass -File scripts\package.ps1 -Destination "招募-20261004-電話calendar-ocr-purge修正-full-clean.zip"`
  - Result: passed; clean package created.
- `docker --version`
  - Result: failed; `docker` command is not installed in this environment.

### Implemented And Locally Tested

- Candidate inbound routing now uses `process_inbound_candidate_message` from API-created candidate messages, `flow.process_inbound_message`, and `platform.sync_messages`.
- `process_interview_reply` consumes confirm/decline/cancel/reschedule/uncertain/low-confidence/risk-flag replies before the general application/question-answer flow.
- Unrelated interview replies continue to the normal required-question answer path.
- Reschedule and unclear interview replies set an identifiable manual-review state and cancel queued reminder/no-show tasks.
- No-show checks re-read the latest candidate reply and cancel without sending a no-response message when the reply is unsafe or already changed interview state.
- No-show checks ignore candidate messages older than the interview and reuse existing interview-reply metadata to avoid duplicate AI classification cost.
- Manual exception queue creation is idempotent by `idempotency_key`.
- Retention cleanup validates model/strategy combinations, filters `QueueTask` and `SystemError` by cleanable status in SQL before ordering/limit, records backlog metrics, and queues a bounded continuation task when work remains.
- Phone provider payload now uses the disclosure-augmented script that is also stored in `PhoneCallLog`; provider acceptance alone is not treated as disclosure proof. Consent, DNC, duplicate dial, signed callback replay protection, and low-confidence text-classification manual review are locally tested.
- Calendar has replaceable mock/Google/Microsoft Graph adapter paths, free/busy lookup hooks, full-interval overlap checks, candidate availability windows, timezone-local presentation with UTC storage, and a read-back gate that prevents `Booked` without provider confirmation.
- OCR upload streams with a size limit and PDF magic/structure validation. Embedded-text PDF extraction and scanned-PDF OCR adapter fallback are split; unconfigured or low-confidence scanned OCR goes to manual review.
- External purge dry-runs are explicitly `Dry Run - Not Deleted`; live purge can be marked `Deleted` only after adapter delete plus read-back evidence. Legal hold remains a blocking manual state.
- Redis disabled guard and operations metrics remain locally implemented and tested.

### Still Not Implemented Or Not Verified

- AI voice calling with live outbound provider, STT, TTS and human transfer: NOT CUSTOMER-ENVIRONMENT VERIFIED. Current automated decisioning is text transcript classification with manual-review gates, not a complete live AI phone agent.
- Calendar auto-booking with Google/Outlook/free-busy and live read-back: ADAPTER PATH IMPLEMENTED; NOT CUSTOMER-ENVIRONMENT VERIFIED. Without customer OAuth/read-back, bookings remain unverified/manual.
- Scanned PDF OCR for real Traditional Chinese/English image PDFs: ADAPTER PATH IMPLEMENTED; NOT CUSTOMER-ENVIRONMENT VERIFIED. Real OCR quality depends on configured OCR provider/engine and scanned corpus testing.
- Redis distributed queue with transactional outbox, DB source of truth, leases/heartbeat, DLQ, shutdown recovery, manual replay and rollback path: LOCALLY IMPLEMENTED WITH MOCK/UNIT REGRESSIONS; Docker Postgres+Redis smoke, Redis restart, long worker soak, and production monitoring remain NOT RUN in this environment.
- Backup/archive/object-store/email-archive live purge propagation: NOT CUSTOMER-ENVIRONMENT VERIFIED. Local auditable purge request, legal hold and dry-run task path are implemented and unit-tested.

### Local Acceptance Checklist

- `pytest` full backend test file: PASSED on 2026-10-04 (`55 passed`).
- `compileall`: PASSED on 2026-10-04.
- Frontend syntax check: PASSED on 2026-10-04 (`node --check frontend\app.js`).
- Alembic migration upgrade: PASSED against temporary SQLite on 2026-10-04 to `0012_calendar_ocr_purge`.
- Clean package script: PASSED on 2026-10-04; produced `招募-20261004-電話calendar-ocr-purge修正-full-clean.zip`.
- Postgres/Redis compose smoke: NOT RUN; Docker availability was not rechecked in this addendum.
- Browser UI smoke: NOT RUN.
- 10,000 representative load: NOT RUN.
- Worker soak: NOT RUN.
- Backup restore and purge drill: NOT RUN.

### Customer Authorized Environment Checklist

- FB/LINE webhook, post/private message/read-back: NOT RUN.
- 518 UID/UIDVALIDITY/ACK: NOT RUN.
- ChickPT selector/session browser worker: NOT RUN.
- Phone provider signed callback: NOT RUN.
- Calendar real booking/reschedule/read-back: NOT RUN.
- OCR real scanned resumes: NOT RUN.
- Production quota/403/429/CAPTCHA/login-expiry observation: NOT RUN.
- External backup/archive purge: NOT RUN.

## Commands Actually Run

- `python -m pytest backend\tests\test_core.py -q`
  - Result: `42 passed, 1187 warnings in 5.83s`
  - Notes: warnings are deprecation/config warnings from SQLAlchemy datetime defaults, security session datetime use, and pytest-asyncio loop-scope defaults.
- `python -m compileall backend -q`
  - Result: passed with no output.
- `python -m pytest backend\tests\test_core.py::test_facebook_commenters_are_isolated_and_echoes_are_ignored backend\tests\test_core.py::test_facebook_new_comment_maps_post_to_job_and_runs_inbound_ai_queue backend\tests\test_core.py::test_facebook_comment_then_messenger_reuses_candidate_job_conversation backend\tests\test_core.py::test_facebook_private_reply_uses_comment_id_and_messenger_send_uses_psid backend\tests\test_core.py::test_candidate_specific_send_refuses_public_facebook_comment_mapping backend\tests\test_core.py::test_package_script_excludes_private_and_cache_artifacts -q`
  - Result: `6 passed, 314 warnings in 2.91s`.
- `alembic upgrade head` against a temporary SQLite database
  - Result: passed and reached `0011_identity_status_retention`.
  - Verification output: `0011_identity_status_retention` and `True` for `data_retention_runs` table existence.
- `node --check frontend\app.js`
  - Result: passed with no output.
- `powershell -ExecutionPolicy Bypass -File scripts\package.ps1 -Destination C:\Users\cl201\.openclaw\workspace\招募\招募-20261002-收件人面試修正-full-clean.zip`
  - Result: passed; package size `140739` bytes.
- ZIP verification using Python `zipfile`
  - Result: `entries 61`, `has_backslash False`, `has_db False`, `has_zip_inside False`, `python_extract_ok True`, `size 140739`.
- ZIP verification using PowerShell `Expand-Archive`
  - Result: `powershell_expand_ok True`.

## Not Run In This Workspace

- Full test directory command `python -m pytest backend\tests -q`: NOT RUN after this round. The targeted local test file covers the current backend suite in this repository, but the exact broader command was not rerun.
- Docker compose config/up: NOT RUN. `docker` command is not installed in this environment.
- Full browser UI smoke/E2E: NOT RUN. No browser automation test target was requested or configured for this app.
- Real FB/LINE/518/ChickPT/phone authorized E2E: NOT RUN. No customer credentials, persistent browser session, official platform targets, mailbox, or phone provider test account were supplied.
- Load, soak, backup/restore, and backup-retention purge drills: NOT RUN.

## Implemented In This Round

- Added migration `0011_identity_status_retention`.
- Added `Application.decision_source`, `Application.decision_event_key`, `ApplicationStatusHistory.source`, and `ApplicationStatusHistory.event_key`.
- Added `DataRetentionRun` and `privacy.retention_cleanup` for batch live-DB masking/deletion with per-policy audit rows.
- Strengthened FB/LINE identity continuity:
  - FB public comments can link to later Messenger PSID messages for the same external user and job.
  - LINE group/room user messages can link to later LINE 1:1 user messages after the job/conversation is resolved.
  - Ambiguous multi-job identity mapping enters Manual Review instead of guessing.
- Candidate-specific outbound sends now require a verified private recipient. Public FB comment mappings and LINE group/room mappings are refused.
- Outbound recipient resolution now separates internal conversation keys from platform recipient IDs. Facebook Messenger sends use the verified PSID from `external_candidate_id`; LINE push sends use the verified LINE `userId`; the internal conversation key remains evidence only.
- Facebook comment webhook now creates a single idempotent `message.private_reply` task using the original comment ID against the private-reply endpoint. It does not treat comment IDs as Messenger PSIDs.
- Interview reply classification was added for confirm/decline/cancel/reschedule/unrelated/uncertain. Negated or contradictory confirmations route to manual review; confirmed interviews cancel pending no-show/reminder tasks.
- The scheduler now creates periodic `privacy.retention_cleanup` tasks with an idempotent bucket key and configured batch size/retries.
- Browser worker page loads now pause on HTTP 403/429 in addition to CAPTCHA, login-expired, unknown-layout, selector, domain, and action guards.
- Package script now excludes prior `.zip` artifacts and validates ZIP entries use portable `/` paths.
- Added distinct `flow.unsuitable_rejection_due` for three-day unsuitable rejection. It is separate from `flow.follow_up_inactive`.
- Three-day unsuitable rejection cancels on status change, candidate reply, DNC, missing private conversation, or duplicate notice; it re-checks state at execution time before queuing an approved rejection message.
- Added configurable `CandidateScoring` route integration. High-confidence schema-valid output may score job-related signals; failure, low confidence, or risk flags fall back to rules scoring.
- Expanded privacy delete to anonymize candidate-linked identity maps, queue task payloads, applications AI notes, and AI request prompt/output text.
- Updated README, architecture, platform connector, AI provider, security, roadmap, validation, and acceptance documentation to reflect the actual scope and remaining gaps.

## Automated Tests Added Or Updated

- FB comment -> Messenger message reuses candidate/job/conversation.
- FB private reply fixture asserts the outbound payload uses `comment_id`, while subsequent Messenger send asserts the payload uses raw PSID and not an internal/page-prefixed key.
- LINE fixture asserts `to` is raw verified `userId`, not destination, group, room, or internal conversation key.
- LINE group user -> LINE 1:1 user message reuses the resolved candidate/job/conversation.
- Interview confirm/no-show tests cover confirmation cancellation, negated reply safety, and no-show re-check before sending.
- Retention scheduler idempotency test verifies only one periodic task is created per bucket.
- Candidate-specific sends refuse public FB comment mappings.
- Unsuitable three-day rejection creates a distinct queue task and idempotent approved rejection message.
- Retention cleanup masks live inbound email and AI request text and records `DataRetentionRun`.
- Legacy migration expectation updated to `0011_identity_status_retention`.
- Existing LINE private-send fixture mapping updated to carry verified private-recipient evidence.

## Still Not Fully Implemented

- AI voice calling/STT/TTS/voice-agent intent extraction. `phone.dial` remains a provider handoff and callback state machine, not a complete AI calling feature.
- Full calendar auto-booking. Interview support is reminders/no-response follow-up, not complete calendar scheduling.
- OCR for scanned resume PDFs. Empty text from scanned PDFs must still be treated as manual review until an OCR adapter is added.
- Redis-backed distributed queue is implemented as an optional Redis Streams worker with PostgreSQL outbox/source of truth. Production acceptance still requires Docker Postgres+Redis and customer-environment soak evidence.
- Backup/archive retention deletion. `privacy.retention_cleanup` only affects the live application database.
- Production platform evidence, quota observation, and long-running worker metrics/alerts.

## Authorized Environment Tests Required

- 518 mailbox poll with real UID/UIDVALIDITY and post-commit ACK evidence.
- FB page/group post, private reply/Messenger, webhook, read-back, 403/429 behavior, and duplicate public fallback guard.
- LINE official account push, group/room mapping, 1:1 reply, webhook delivery/read evidence, and private recipient proof.
- ChickPT/518 browser worker with authorized persistent session, selector version evidence, CAPTCHA/login-expired/manual review behavior.
- Phone provider dial with signed callbacks for connected/no-answer/rejected/failed and idempotent fallback.
- Load/soak and backup/restore/purge drills.

## 2026-10-04 Third Pass Verification

基準：`招募-20261004-second-pass-full-clean`。

已實作且自動測試通過：

- Persistent business flow state: `BusinessFlowState` + `/api/flow/states` + Flow UI，Application 狀態變更會寫入 stage/revision/rule evidence。
- Redis queue foundation: PostgreSQL remains source of truth; `QueueOutbox` transactional outbox, Redis Streams publisher helpers, consumer-group helpers, DB/Redis reconcile, lease/fencing fields, manual/worker shared atomic claim, durable `QueueDlqRecord`, step-level DLQ write/ACK recovery, stale DLQ pointer/index recovery before ACK, DLQ replay guards, and persistent recovery cursor are implemented. Redis disconnect leaves outbox retry evidence and DB queue fallback instead of pretending success.
- Storage lifecycle: duplicate SHA uploads now create independent `StorageObject` versions and independent filesystem object keys; purge success updates `StoredDocument` and `StorageObject` only after read-back proof.
- Purge retry semantics: transient provider failures increment purge retry state and cause QueueTask retry/backoff; legal hold and permanent failures remain manual review.
- OCR mixed PDF handling: embedded-text pages are preserved, only scanned/blank pages are subset into a temporary PDF for raster OCR to avoid duplicate text.
- Voice intent safety: negative/uncertain terms take priority; `yes, no problem` is accepted; `不可以`, `沒有空`, `尚未確認`, `可能可以` are not treated as automated consent; string `"false"` is not disclosure proof.
- Frontend: Flow page plus Queue Redis/outbox/lease/fencing visibility added.

Commands and results:

- `pytest backend/tests/test_core.py -q` -> `68 passed`.
- `python -m compileall backend -q` -> passed.
- `node --check frontend/app.js` -> passed.
- Temp SQLite `alembic upgrade head` -> reached `0015_flow_redis_storage`; verified `queue_outbox`, `business_flow_states`, and `storage_objects`.
- Targeted regressions added for Redis outbox/atomic claim, DLQ record API/UI contract, missing task/outbox DLQ evidence, ACK failure replay without duplicate Redis DLQ entries, stale/trimmed DLQ pointer rewrite before ACK, indexed DLQ dedupe without deep scans, guarded DLQ replay for stale revision/DNC/deleted candidate/completed external actions, multi-worker claim competition, recovery cursor restart/wrap, 10,001-row recovery scan, heartbeat ownership loss during DB renewal failure, independent same-SHA storage objects, voice negation/uncertainty, strict disclosure boolean, and business flow state.

Not fully implemented / not verified:

- True Redis/PostgreSQL integration under Docker, Redis process restart, long consumer pending soak, and DLQ capacity/alert soak were not run on this host.
- Live FB/LINE/518/ChickPT, true phone provider media/STT/TTS, Google/Outlook OAuth/webhook sync, and external backup/storage provider acceptance still require customer credentials/environments.
- Multi-round voice agent runtime and real media bridge are still adapter contracts plus callback reducer, not a fully exercised live phone agent.
- Full customer-flow E2E, 10,000 complete workflow load, one-year capacity growth, worker soak, fault injection, backup restore, and purge drill were not completed locally.

## 2026-10-08 Redis DLQ Pointer Regression

- Code changed: `backend/app/services/redis_queue.py` now exact-checks stored DLQ Redis stream ids before ACK, rewrites stale/trimmed Redis DLQ pointers, and uses a Redis-side atomic indexed `XADD` path instead of deep DLQ stream scans for uniqueness.
- Migration changed: `alembic/versions/0022_queue_dlq_pointer_recovery.py` adds DLQ pointer/status indexes for bounded recovery queries.
- Tests changed: `backend/tests/test_core.py` adds stale index/trimmed pointer recovery before ACK and updates migration-head assertions to `0022_queue_dlq_pointer_recovery`.
- Executed locally: `python -m pytest backend/tests/test_core.py -k "redis or dlq" -q` -> 23 passed.
- Executed locally: `python -m pytest backend/tests/test_core.py -q` -> 117 passed.
- Executed locally: `alembic upgrade head` on a temporary/default SQLite DB -> reached `0022_queue_dlq_pointer_recovery`; generated local `recruitment.db` was removed after verification.
- Not run: Docker Postgres+Redis smoke, Redis restart, long worker soak, and 10,000 full-flow load. `docker --version` failed on this host because the `docker` command is not installed/available.

## 2026-10-08 Full Repair Regression

- Code changed: Pydantic `Settings` now owns scheduler, Redis, queue lease, retention cleanup and OCR runtime defaults. Direct `os.getenv` drift was removed from `scheduler.py`, `queue.py`, `redis_queue.py`, and `ocr.py` for these paths.
- Health changed: `/api/health` now reports `worker_config` with effective queue backend, worker queues, lease seconds, Redis consumer settings, OCR engine/languages/DPI/timeout and retention cleanup settings.
- Replay changed: `replay_task` now uses a conditional SQL update to reject stale cached task state after another worker has claimed the task as `Running`.
- Traceability added: `docs/REQUIREMENTS_TRACEABILITY_20261008.md` maps all 24 requested items to files/tests/status/blockers.
- Executed locally: `python -m pytest backend/tests/test_core.py -k "health_reports_worker_runtime_settings_from_pydantic or replay_task_does_not_overwrite_concurrent_running_claim or redis or dlq" -q` -> 25 passed.
- Executed locally: `python -m pytest backend/tests/test_core.py -q` -> 119 passed.
- Not run: PostgreSQL/Redis Docker integration, true Redis 7 Lua integration, two real workers, two real manager sessions, 10,000 complete workflow load, 24-72 hour soak, network/crash/fault injection, backup restore/purge drill, and authorized FB/LINE/518/ChickPT/phone/Google/Outlook/OCR E2E. This host has no `docker` CLI and no customer credentials/session state.

## 2026-10-08 061042 Baseline Integration Repair

- Baseline changed: this pass starts from `recruitops-20261008-061042-full-clean.zip`.
- Redis replay changed: `replay_task` now has expected delivery revision CAS plus a replay request idempotency key. `replay_dlq_record` passes the DLQ delivery revision into that CAS and stores `last_replay_request_key`; recovery only marks `Replay In Progress` as `Replayed` when the task revision bump is tied to that exact replay key, not merely because the task version is larger.
- Redis worker changed: `run_redis_worker` now imports `os` for worker id generation; this fixes a startup failure in the Redis worker loop.
- Outbox reconciliation changed: pending outbox metrics use a bounded count query and orphan repair is capped per reconciliation batch instead of loading all orphaned tasks.
- Blacklist changed: `RecruitmentBlacklist` now has structured `scope_type` and `scope_id`; migration `0023_blacklist_scope_and_replay_guards` backfills explicit legacy `job:<id>` scope and exact matching prevents `job:10` from blocking `job:1`.
- Outbound preflight changed: normal messages, FB private replies, phone dialing, regional notify, and recall-created sends now share a common preflight for DNC, blacklist, conversation pause/manual status, account state, quota/cooldown, and job scope before external side effects.

Commands and results:

- `python -m pytest backend/tests/test_core.py -q -k "replay_task_requires_expected or dlq_replay_in_progress_requires or structured_blacklist or private_reply_runs_shared or replay_task_does_not_overwrite or redis_configured_streams"` -> 6 passed.
- `python -m pytest backend/tests/test_core.py -q -k "redis or dlq or sqlite_upgrade or misversioned or unversioned"` -> 28 passed.
- `python -m pytest backend/tests/test_core.py -q` -> 123 passed.
- `docker --version` -> failed, `docker` command is not installed/available on this host.

Still blocked for customer/production acceptance:

- Docker Postgres/Redis smoke, real Redis 7 Lua/restart/pending reclaim tests, multi-worker process race tests, 10,000 full-flow load, 24-72 hour soak, backup restore/purge drill, and authorized FB/LINE/518/ChickPT/phone/calendar/OCR E2E.

## 2026-10-08 Closeout Pass

- Migration compatibility changed: long Alembic revision identifiers were shortened to fit PostgreSQL's common 32-character `alembic_version.version_num` limit. `env.py` maps legacy long revision values to their short replacements before running migrations; this preserves existing databases without stamping or skipping migrations.
- New migration: `0024_queue_replay_records` creates a persistent replay request ledger with unique `replay_request_key`, expected/replayed revisions, replayed outbox id, optional DLQ id, and status/failure evidence.
- Replay API changed: `/api/queue/tasks/{task_id}/replay` accepts `expected_delivery_revision` and `replay_request_key`; identical retries return the already-recorded result and do not create another outbox/revision.
- Blacklist API/UI changed: `RecruitmentBlacklistCreate/Out`, `/api/recruitment-blacklist`, and the Privacy frontend now expose `scope_type`/`scope_id`. The API validates legal scope values, job existence, and invalid scope combinations.
- Blacklist migration changed: old entries with multiple `job:<id>` tokens create one logical job-scoped row per job; unclear legacy scope remains `legacy` and is not widened into a job-specific rejection.
- Redis stream discovery changed: `configured_streams` uses `DISTINCT` and bounded result limits.

Commands and results:

- `python -m pytest backend/tests/test_core.py -q -k "alembic_revision_identifiers or replay_api_accepts or replay_task_requires_expected or recruitment_blacklist_api or structured_blacklist or redis_configured_streams or sqlite_upgrade or misversioned or unversioned"` -> 10 passed.
- `python -m pytest backend/tests/test_core.py -q -k "redis or dlq or replay or blacklist or sqlite_upgrade or misversioned or unversioned or alembic_revision"` -> 37 passed.
- `python -m pytest backend/tests/test_core.py -q` -> 126 passed.
- `python -m compileall backend -q` -> passed.
- `node --check frontend/app.js` -> passed.
- `docker --version` -> failed, `docker` command is not installed/available on this host.

Not completed in this local closeout:

- True PostgreSQL/Redis Docker smoke, live Redis 7 Lua/restart/two-worker tests, authorized platform E2E, 10,000-flow load, 24-72 hour soak, and backup restore/delete drills still require a runnable Docker/CI or customer environment.

## 2026-10-08 Gapfix Closeout

Implemented and locally tested:

- Added follow-up migration `0025_blacklist_scope_repair` instead of editing published `0023`. It repairs post-0022/old-0023/new-0023 blacklist data shapes: job rules misstored as candidate/global/legacy become canonical `scope_type='job'` with integer string `scope_id`; multiple `job:<id>` tokens create per-job rows; explicit global rules remain global; ambiguous legacy rules move to `Pending Scope Confirmation` rather than silently allowing everything or expanding the blocked scope.
- Canonicalized job blacklist `scope_id` to integer-string form across schema validation, API storage/response, matching, frontend submission, and migration repair. API regressions cover padded `"01"`, blank, and illegal job scope values.
- Changed replay idempotency ordering so a successful persistent `queue_replay_records` result is returned before retry-budget rejection. Reusing the same key for a different task, expected revision, or DLQ parameter is rejected.
- Made task replay API require `expected_delivery_revision` and `replay_request_key`, with length/format/range validation. The frontend now generates and persists the operation key in `localStorage` per task/revision so network retries do not create a new key and operators do not manually enter technical identifiers.
- Removed the `configured_streams` 500-row hard cap. Discovery now returns all configured streams and emits a `SystemError` warning when the set exceeds 500.
- Made platform and phone webhook duplicate handling idempotent per event: an exact replay returns success/duplicate and skips side effects, while event-id collisions with different payload hashes still fail. Phone status updates remain monotonic, preventing A -> B -> replay A rollback.
- Tightened voice classification so `"I confirm, no problem"` remains positive while explicit interview refusal such as `不願意面試但可以回答問題` is not treated as interview confirmation.

Commands and results:

- `python -m pytest backend/tests/test_core.py -q -k "blacklist_scope_repair or recruitment_blacklist_api or replay_api_accepts or replay_task_requires_expected or redis_configured_streams or line_webhook_signature_and_inbound_message or voice_intent"` -> 8 passed.
- `python -m pytest backend/tests/test_core.py -q -k "redis or dlq or replay or blacklist or sqlite_upgrade or misversioned or unversioned or alembic_revision or webhook or voice"` -> 45 passed.
- `python -m pytest backend/tests/test_core.py -q` -> 128 passed.

Still missing implementation:

- Full business orchestration beyond current local flow handlers: consultation clarification, polite rejection variants, full operations page coverage, complete worker maintenance automation, model budget enforcement across real providers, atomic quota controls, long-term data/backup/delete provider adapters, and production-grade Redis old-index cleanup/fairness recovery remain incomplete.
- Live phone STT/TTS/media bridge, multi-turn voice agent runtime, and human transfer remain adapter contracts plus callback reducer tests, not a working live telephony agent.
- Calendar OAuth, webhook sync, atomic cross-worker booking locks, reschedule/cancel propagation, and HR no-show reporting beyond local attendance endpoints remain incomplete.
- Authorized 518 attachment OCR ingestion, ChickPT/FB pagination and identity verification, FB group image/text posting, LINE retry-key verification against live APIs, OCR quality/resource-limit enforcement against real scanned files, and external backup/delete workflows require more implementation.

Still missing testing / acceptance evidence:

- Gate A: real PostgreSQL/Redis startup and upgrade were not run in Docker/CI here; only SQLite/Alembic and local unit regressions passed.
- Gate B: complete customer happy-path and exception-path orchestration across all modules has not been run end to end with customer-like data.
- Gate C: authorized platform E2E, 10,000-record load, 24-72 hour soak, backup restore, and deletion drills remain unrun in this environment.

## 2026-10-08 1703 Continuation

Implemented and locally tested:

- Added migration `0026_scope_event_repairs` after `0025`. It adds `business_events`, webhook `canonical_event_hash` and `raw_body_b64`, and repairs explicit global blacklist rows that were stored as job scope.
- Pending Scope Confirmation is now a blocking automation state: matching pending rules pause the conversation, create a `manual.blacklist_scope_confirmation` task, set outbound automation to `Manual Review Required`, and expose confirm/lift APIs plus UI hooks. Pending scope is not treated as a final rejection.
- Added independent business event logging with unique `event_key`; stale non-manual status regressions are logged as ignored and do not move applications backward.
- LINE/FB webhook dedupe now uses stable canonical event content while preserving raw body base64 for signature/audit. Redelivery markers and batch wrapper changes do not rerun side effects; content conflicts on the same event id still fail.
- Redis configured stream warnings are throttled/deduped, and stale DLQ index cleanup is bounded by explicit limits.
- Prepared Docker/CI smoke scripts for real Postgres/Redis: `scripts/postgres_redis_smoke.py` and `scripts/run_postgres_redis_smoke.ps1`.
- Added `docs/DELIVERY_REPORT_20261008_1703.md` with implementation locations, test evidence, environment blockers, and separate unimplemented/unvalidated lists.

Commands and results:

- `python -m pytest backend/tests/test_core.py -q` -> `128 passed`.
- `python -m pytest backend/tests/test_gapfix_20261008.py -q` -> `4 passed`.
- `python -m compileall backend -q` -> passed.
- `node --check frontend/app.js` -> passed.
- Temporary SQLite `alembic upgrade head` -> reached `0026_scope_event_repairs`; verified `business_events` table exists.
- `docker --version` and `docker compose version` -> failed because `docker` is not installed/on PATH on this host.

Still not implemented:

- True live STT/TTS media bridge, multi-turn phone runtime, and provider-level human transfer.
- Calendar OAuth install/refresh UX, webhook sync, provider event update propagation, and full reschedule/cancel E2E.
- Complete business orchestration for consultation clarification through regional notification/data pool beyond current local state handlers.
- Full operator pages for every workflow and worker maintenance action.
- Native long-term backup/archive/email purge adapters and restore protection workflows.
- Production-grade atomic quota controls and provider budget enforcement across all live providers.
- 518 attachment OCR ingestion and live ChickPT/FB pagination workflows.

Still not validated:

- Real PostgreSQL/Redis Docker smoke and Redis restart/failure recovery on this host.
- Authorized FB/LINE/518/ChickPT/phone/calendar/OCR E2E.
- 10,000-record complete workflow load, 24-72 hour soak, backup restore drill, external deletion drill.
- OCR quality/resource-limit acceptance against a real scanned Traditional Chinese/English resume corpus.
