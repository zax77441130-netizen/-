# 2026-10-08 Requirements / Code / Test Traceability

Baseline: `recruitops-20261008-061042-full-clean.zip` plus the customer flow checklist supplied on 2026-10-08.

## Status Legend

- Implemented: code changed or already present and covered by local tests.
- Partial: code path exists, but product or production acceptance is incomplete.
- Blocked: requires customer credentials, live platform access, Docker, or long-running environment not available on this host.
- Not done: not completed in this package.

## Matrix

1. Unified settings for scheduler/Redis/OCR: Implemented. Files: `backend/app/core/config.py`, `backend/app/workers/scheduler.py`, `backend/app/services/queue.py`, `backend/app/services/redis_queue.py`, `backend/app/services/ocr.py`, `backend/app/api/routes.py`. Test: `test_health_reports_worker_runtime_settings_from_pydantic`.
2. Atomic replay without stale Running overwrite and expected delivery revision: Implemented locally. `replay_task` accepts `expected_delivery_revision` and `replay_request_key`; the public replay API accepts those fields; `queue_replay_records` persists replay requests with a unique key and outbox evidence, so identical request retries return the existing result instead of incrementing revision/outbox. `replay_dlq_record` uses the same path, and recovery no longer treats an unrelated version bump as success. Files: `backend/app/services/redis_queue.py`, `backend/app/api/routes.py`, `backend/app/models/domain.py`, `alembic/versions/0024_queue_replay_records.py`. Tests: `test_replay_task_does_not_overwrite_concurrent_running_claim`, `test_replay_task_requires_expected_delivery_revision_and_is_idempotent`, `test_replay_api_accepts_expected_revision_and_persistent_request_key`, `test_dlq_replay_in_progress_requires_matching_replay_request_key`.
3. Legacy DLQ index continuity, missing index, restart, XADD crash: Partial. Current package uses exact Redis id validation and indexed atomic XADD for the current key; legacy key migration and no-duplicate proof against pre-index orphan entries still need real Redis integration coverage.
4. DLQ recovery cursor/fairness/backoff/claim: Partial. Existing bounded status selection and recovery exist, but persistent cursor/fair scheduling/retry_at per DLQ record is not complete.
5. PostgreSQL source of truth, Redis Lua, lease/fencing, bounded cleanup: Partial. PostgreSQL outbox, Lua-indexed DLQ write, lease/fencing and reclaim exist; safe stream/index cleanup still not complete.
6. Real Redis integration tests: Blocked locally. Docker CLI is unavailable; FakeRedis remains unit coverage only for this host.
7. Independent event log and legal transition table: Partial. `BusinessFlowState` and status history exist; append-only event table and transition matrix are not complete.
8. Structured blacklist job scope: Implemented locally. `RecruitmentBlacklist` has `scope_type`/`scope_id`; migration `0023_blacklist_replay_guards` backfills explicit legacy `job:<id>` entries and duplicates rows for multiple legacy job IDs instead of only taking the first; ambiguous legacy scope remains `legacy` and is not expanded to job-scoped rejection. API schemas and frontend privacy page expose the scope fields and validate job existence/scope combinations. Files: `backend/app/models/domain.py`, `backend/app/schemas/dto.py`, `backend/app/api/routes.py`, `backend/app/services/queue.py`, `frontend/app.js`, `alembic/versions/0023_blacklist_scope_and_replay_guards.py`. Tests: `test_structured_blacklist_job_scope_is_exact_not_substring`, `test_recruitment_blacklist_api_persists_and_validates_scope`.
9. Shared outbound preflight for contact paths: Implemented locally for normal message sends, FB private replies, phone dial, regional notify, and recall-created message sends through the common send path. It re-checks DNC, blacklist, account state, conversation pause/manual status, rate policy, quota/cooldown, and job scope before side effects. Files: `backend/app/services/queue.py`. Tests: `test_private_reply_runs_shared_preflight_for_blacklist`, `test_platform_rate_policy_suppresses_duplicate_candidate_message`, `test_paused_conversation_blocks_automatic_outbound_send`, phone callback/DNC tests.
10. Full orchestration: Partial. Many queue task families exist, but one durable end-to-end orchestrator with compensation state for every node is not complete.
11. FAQ/interruption/correction/multi-job clarification: Partial. Low-confidence manual review exists; automatic clarification/resume is not complete.
12. Phone STT/TTS/multi-turn/media bridge/handoff: Blocked/not done. Transcript classifier is local only and must not be described as a full voice agent.
13. Google/Outlook OAuth/atomic two-interviewer holds/sync: Blocked/not done. Adapter paths exist; live OAuth refresh/webhooks and atomic provider holds are not complete.
14. Interview reminders/no-show/reschedule evidence: Partial. Reminder/no-show tasks exist; HR evidence and full auto-reschedule loop need live messaging/calendar acceptance.
15. 518 attachments/OCR/UID ACK/dedupe: Partial. UID/UIDVALIDITY and OCR paths exist; authorized mailbox and attachment corpus E2E not run.
16. ChickPT/FB pagination and identity mapping: Partial. Adapter/browser fixture paths exist; authorized pagination/read-back E2E not run.
17. Browser submit verification and pause on CAPTCHA/403/429: Partial. Guard language and browser worker checks exist; live selector failure testing not run.
18. LINE retry key/read-back IDs: Partial. LINE mapping and send evidence paths exist; fixed retry key persistence before first send needs deeper connector work.
19. Platform quotas atomic reservation/Retry-After/timezone: Partial. Rate state exists; atomic reservation/settlement and template-recipient hash semantics need more work.
20. OCR page order/zero confidence/resource limits/rerun/delete races: Partial. Page order and size/page/time limits exist; zero-confidence averaging and orphan cleanup need more tests.
21. Frontend operations pages: Partial. Queue/flow UI exists; phone, OCR, calendar reschedule/cancel, attendance, alert, purge and DLQ workflow pages are incomplete.
22. Data retention/archive/backup purge and restore: Partial. Live DB retention and external purge records exist; real backup/object/email archive adapters and restore tombstone protection remain incomplete.
23. Model routing/cost/budget/schema/provider truth: Partial. Text routing, fallback, usage and schema checks exist; atomic budgets and non-text provider protocol implementations remain incomplete.
24. Async worker heartbeat/graceful shutdown/migration/prod config: Partial. Lease heartbeat exists; blocking calls, graceful shutdown and production config checks need more integration coverage.

## Local Verification This Package

- `python -m pytest backend/tests/test_core.py -k "health_reports_worker_runtime_settings_from_pydantic or replay_task_does_not_overwrite_concurrent_running_claim or redis or dlq" -q`
- `python -m pytest backend/tests/test_core.py -q -k "replay_task_requires_expected or dlq_replay_in_progress_requires or structured_blacklist or private_reply_runs_shared or replay_task_does_not_overwrite or redis_configured_streams"` -> 6 passed.
- `python -m pytest backend/tests/test_core.py -q -k "redis or dlq or sqlite_upgrade or misversioned or unversioned"` -> 28 passed.
- `python -m pytest backend/tests/test_core.py -q -k "redis or dlq or replay or blacklist or sqlite_upgrade or misversioned or unversioned or alembic_revision"` -> 37 passed.
- `python -m pytest backend/tests/test_core.py -q`
- Latest full run: 126 passed.

## Not Run Here

- PostgreSQL/Redis Docker integration, Redis restart, real Lua integration under Redis 7, two real workers, two real admin sessions, 10,000 full-flow load, 24-72 hour soak, backup restore/purge drill, and authorized FB/LINE/518/ChickPT/phone/calendar/OCR E2E.
- Reason: this host has no `docker` CLI and no customer platform credentials/session state.
