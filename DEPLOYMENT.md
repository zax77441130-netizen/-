# Deployment

## Local

```powershell
Copy-Item .env.example .env
pip install -r requirements.txt
alembic upgrade head
python -m playwright install chromium
uvicorn backend.app.main:app --reload
```

SQLite is allowed only for local development and automated tests. A clean checkout must run migrations before startup.

## Docker Smoke

```powershell
Copy-Item .env.example .env
docker compose up --build
```

`backend` and `worker` both run `alembic upgrade head` before serving or processing tasks.

## Production

- `DATABASE_URL=postgresql+psycopg://...`
- `APP_ENV=production`
- `AUTO_CREATE_TABLES=false`
- strong `SECRET_KEY`
- restricted `CORS_ORIGINS`
- configured backup and restore scripts/process
- secret manager or protected runtime files for platform credentials

Do not ship or restore `recruitment.db` as an initial production database. The legacy SQLite file in this workspace is not a seed; it is only used to verify that old, unversioned local databases can be migrated without losing data.

## Health Checks

- Backend: `/api/health`
- AI provider probe: `/api/ai/providers/{id}/test`
- Worker: scheduler process plus queue depth, stale lock and Manual Review monitoring
- External platforms: successful send/read-back or webhook evidence, not just HTTP 200
- Retention: backlog count, oldest pending cleanup age, rows scanned/changed per batch, failure count, and continuation task delay.
- Platform automation: per-platform success rate plus 403, 429, CAPTCHA, login-expired, unknown-layout and read-back mismatch counts.
- AI providers: latency, timeout/error rate, estimated/actual cost, schema-validation failures, fallback rate, and manual-review gate count.
- Capacity: disk, database size, queue depth, oldest queued task delay, and worker heartbeat freshness.

## External Automation Setup

- Configure platform accounts in the UI/API.
- Store ChickPT/518 Playwright `session_state_path` outside this repository.
- Configure selector registry, allowed domains/actions, max steps, timeout and retry policy per browser account.
- Configure LINE channel secret/access token and Facebook app secret/verify token before enabling inbound automation. Webhook events are persisted in `platform_webhook_events` and replayed event IDs are rejected.
- Configure `PlatformAccount.config.rate_policy` per platform/account before enabling outbound automation. Supported controls include `quiet_hours`, `hourly_quota`, `daily_quota`, account/candidate/group cooldowns, duplicate suppression, and circuit breaker pause windows.
- Configure phone provider account with signed callback secret. Callback replay protection is persisted in `phone_webhook_events`.
- ChickPT/518 browser accounts must use session files or Playwright user data directories outside the repository, plus selector namespaces (`publish`, `inbox`, `message`) with a version that matches `selector_version`. Unknown layout, login expiry, CAPTCHA, 403/429 or permission loss must pause the account instead of retrying blindly.

## Retention And External Purge

- `privacy.retention_cleanup` only masks or deletes rows in the live application database.
- Do not claim backup, email archive, exported ZIP, or object-storage deletion from live DB cleanup alone.
- Configure provider lifecycle rules for object storage and external archives separately.
- Keep a deletion audit trail with object key, provider, requested_at, result, retry count, legal-hold status, and final purge evidence.
- Failed external purge attempts must retry with backoff and surface in operator alerts.
- Backup purge must be verified by restore/purge drills on the backup system's own retention schedule.

## Disabled Until Implemented

- Calendar auto-booking must stay disabled until Google/Outlook/free-busy adapters, atomic hold/book, two-interviewer constraints, duplicate webhook handling, read-back, reschedule/cancel, and real authorized tests are complete.
- OCR must stay disabled until scanned PDF OCR, Traditional Chinese/English language packs, confidence thresholds, encrypted/blank/low-confidence manual review, source retention, and purge behavior are tested.
- Redis distributed queue must stay disabled until PostgreSQL remains the source of truth through transactional outbox or equivalent reliable delivery, idempotent tasks, lease/heartbeat, retry backoff, DLQ, manual replay, worker shutdown recovery, migration, and rollback tests.
- AI voice calling must not be described as complete until outbound provider dialing, STT, TTS, voice-agent state, consent disclosure, DNC/timezone policy, signed callback, replay protection, and high-risk human handoff are verified with a real provider.

## Rollback

1. Stop worker processes first.
2. Stop backend traffic.
3. Restore the most recent verified PostgreSQL backup if schema/data rollback is required.
4. Deploy the prior application version.
5. Run the matching Alembic downgrade only after confirming it is safe for retained production data.
6. Restart backend, then worker, and verify `/api/health` plus queue depth.
