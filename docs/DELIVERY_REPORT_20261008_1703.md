# Delivery Report 2026-10-08 17:03

## Implemented In This Pass

- Pending Scope Confirmation is now a real automation gate. Matching pending scope rules pause the conversation, create a `manual.blacklist_scope_confirmation` task, set outbound tasks to `Manual Review Required`, and expose confirm/lift API/UI paths. It does not treat pending scope as a formal rejection.
- Business events are now persisted independently in `business_events` with unique `event_key`. Non-manual stale status regressions are logged as ignored instead of rolling an application backward.
- Platform webhook replay handling now stores raw body base64 for signature/audit and compares stable canonical event hashes. LINE/FB batch wrapper and redelivery marker differences no longer re-run side effects; true same event-id content conflicts are still rejected.
- Redis stream discovery warnings are throttled through one open `SystemError`, and stale DLQ index cleanup is bounded by caller-supplied limits.
- Added migration `0026_scope_event_repairs` after `0025` to preserve prior migration history while adding `business_events`, webhook canonical hash/raw body fields, and a repair for explicit global blacklist rows misclassified as job scope.
- Added Docker/CI runnable smoke scripts:
  - `scripts/postgres_redis_smoke.py`
  - `scripts/run_postgres_redis_smoke.ps1`

## Implementation Locations

- `backend/app/services/queue.py`: pending scope gate before outbound contact, manual confirmation queueing, Redis-safe message send ordering.
- `backend/app/services/flow.py`: business event recording, legal transition checks, stale non-manual transition ignore.
- `backend/app/api/routes.py`: blacklist confirm/lift APIs, canonical webhook event recording, raw body audit preservation.
- `backend/app/models/domain.py`: `BusinessEvent`, webhook raw/canonical fields.
- `backend/app/services/redis_queue.py`: warning throttling and bounded stale DLQ index cleanup.
- `alembic/versions/0026_scope_event_redis_repairs.py`: follow-up migration.
- `frontend/app.js`: operator controls for pending blacklist scope confirmation/lift and queue visibility.
- `backend/tests/test_gapfix_20261008.py`: focused cross-module regression coverage.

## Test Evidence

- `python -m pytest backend/tests/test_core.py -q` -> `128 passed`.
- `python -m pytest backend/tests/test_gapfix_20261008.py -q` -> `4 passed`.
- `python -m compileall backend -q` -> passed.
- `node --check frontend/app.js` -> passed.
- SQLite `alembic upgrade head` -> reached `0026_scope_event_repairs`; verified `business_events` table exists.

## PostgreSQL / Redis Result

- Prepared runnable Docker smoke via `scripts/run_postgres_redis_smoke.ps1`.
- Local execution is environment-blocked: `docker` is not installed/on PATH on this host.
- Because Docker is unavailable here, true PostgreSQL startup, Redis startup, Redis restart recovery, and Postgres-backed migration execution were not run locally.

## Platform Module Test Status

- Locally tested with deterministic module tests: LINE webhook canonical replay/dedup, pending scope confirmation, business event legal transition logging, Redis warning cleanup, existing FB/LINE/phone/calendar/OCR/queue regressions in `test_core`.
- Not customer-authorized tested: 518 mailbox/browser, ChickPT, live FB page/group, live LINE official account, live phone provider, Google/Outlook OAuth calendars, external backup/object stores.

## Still Not Implemented

- True live STT/TTS media bridge, multi-turn phone runtime, and provider-level human transfer.
- Calendar OAuth install/refresh UX, webhook sync, provider event update propagation, and full reschedule/cancel E2E.
- Complete business orchestration for consultation clarification through regional notification/data pool beyond current local state handlers.
- Full operator pages for every workflow and worker maintenance action.
- Native long-term backup/archive/email purge adapters and restore protection workflows.
- Production-grade atomic quota controls and provider budget enforcement across all live providers.
- 518 attachment OCR ingestion and live ChickPT/FB pagination workflows.

## Still Not Validated

- Real PostgreSQL/Redis Docker smoke and Redis restart/failure recovery on this host.
- Authorized FB/LINE/518/ChickPT/phone/calendar/OCR E2E.
- 10,000-record complete workflow load, 24-72 hour soak, backup restore drill, external deletion drill.
- OCR quality/resource-limit acceptance against a real scanned Traditional Chinese/English resume corpus.
