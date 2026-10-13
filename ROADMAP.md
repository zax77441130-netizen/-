# ROADMAP

## P0

- Harden authentication with CSRF, secure-cookie production toggle, and password reset
- Expand RBAC permission matrix and permission tests
- Alembic migrations for PostgreSQL
- Expand Alembic revisions from scaffold to strict production migration workflow
- Finish remaining Jobs/Candidates fields, bulk import, templates, and history versions
- Platform ingestion for Omnichannel inbox
- Resume file upload hardening, PDF text extraction/OCR parsing, and attachment storage
- Customer OCR provider/Tesseract deployment for scanned PDFs; empty or unconfigured OCR must remain manual review
- Kanban UI for application status transitions, including Suitable/Unsuitable source and status history
- Interview reminder worker delivery through configured channels; Google/Outlook calendar adapter paths still need customer OAuth verification
- AI Provider live HTTP calls, actual provider usage parsing, and richer fallback policies
- Token/cost tracking with actual vs estimated labels
- Prompt rollback, prompt A/B testing, and prompt test cases
- Redis-backed distributed queue processor
- Broaden audit log coverage for all important operations
- Backup/archive retention purge and restore drill evidence

## P1

- Legal platform connectors and OAuth setup screens
- Posting center publish/schedule/retry workflow
- AI reply suggestion and manual approval workflow
- Recruitment Pipeline Kanban
- Dynamic interview required-question engine
- Interview module and reminders
- Operations UI for manual review tasks, identity mapping ambiguity, and retention run results

## P2

- Talent Pool search
- Matching signals and HR review screen
- Regional dispatch workflow
- Analytics by platform, group, job, HR, and area
- Do-not-contact and restricted list governance
- Candidate data export/delete/retention tools beyond live DB masking, including external archives/backups

## P3

- Telephony adapter interface and vendor plugins
- AI voice with required disclosure and transfer-to-human
- STT/TTS phone AI intent extraction, consent strategy, callback signing, and human handoff
- Advanced cost optimization
- A/B prompt and copy analytics
- Forecasting and difficult-job detection
# 2026-10-04 Roadmap Status

- Implemented in second pass with local contract tests: candidate document purge safety, duplicate PDF document records, rasterize-then-Tesseract TSV OCR path, strict voice disclosure/DNC classification, calendar fail-closed free/busy/read-back checks, token-free calendar queue payloads, confirmed-vs-arrived attendance split, and Running-task API guard.
- Implemented locally with tests: phone provider payload disclosure consistency with consent/DNC/duplicate/replay/low-confidence gates, signed callback safety, calendar adapter/read-back gate with mock booking policy, streamed PDF upload validation, PDF text/scanned OCR split with manual review fallback, external purge dry-run/legal-hold/read-back status, operations metrics endpoint, Redis disabled guard.
- Still requires customer authorization before production acceptance: live telephony provider with STT/TTS/human transfer callbacks, Google/Outlook OAuth booking/read-back, real Traditional Chinese/English scanned resume OCR corpus, FB/LINE/518/ChickPT read-back and platform quota/error observation, real backup/object/email archive purge.
- Still incomplete in product surface: frontend operation screens, full business state-machine orchestration, OAuth refresh/webhook sync, PostgreSQL concurrency/exclusion constraints, worker soak, and backup restore/purge drill.
- Redis distributed queue is locally implemented with transactional outbox, leases, heartbeat, retry backoff, DLQ, stale DLQ pointer recovery before ACK, shutdown recovery helpers, manual replay and rollback guards. Remaining work is production acceptance: Docker Postgres+Redis smoke, Redis restart, multi-worker soak, alert thresholds, and rollback drill in the customer environment.
- 2026-10-08 full repair pass added unified Pydantic settings for worker/Redis/OCR runtime, health reporting of effective worker mode, atomic replay protection against stale manager sessions overwriting Running tasks, and a 24-item traceability matrix. Remaining high-risk gaps are structured blacklist job scope, append-only flow event transition table, full orchestration, live STT/TTS/media bridge, Google/Outlook OAuth sync/atomic holds, frontend operations pages, archive/backup purge adapters, real Redis 7 integration, and authorized platform E2E.
