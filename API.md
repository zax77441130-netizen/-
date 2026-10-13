# API

OpenAPI is available at `/docs` when the backend is running.

## Current Endpoints

- `GET /api/health`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `GET /api/jobs`
- `POST /api/jobs`
- `GET /api/jobs/{job_id}`
- `PATCH /api/jobs/{job_id}`
- `POST /api/jobs/{job_id}/duplicate`
- `POST /api/ai/copywriting`
- `GET /api/candidates`
- `POST /api/candidates`
- `POST /api/imports/candidates`
- `GET /api/resumes`
- `POST /api/resumes/parse`
- `GET /api/candidates/{candidate_id}`
- `PATCH /api/candidates/{candidate_id}`
- `GET /api/candidates/{candidate_id}/export`
- `DELETE /api/candidates/{candidate_id}`
- `GET /api/applications`
- `POST /api/applications`
- `PATCH /api/applications/{application_id}/status`
- `GET /api/applications/{application_id}/history`
- `GET /api/platforms`
- `POST /api/platforms/{platform}/test`
- `GET /api/posts`
- `POST /api/posts`
- `GET /api/ai/providers`
- `POST /api/ai/providers`
- `POST /api/ai/providers/{provider_id}/test`
- `GET /api/ai/routes`
- `POST /api/ai/routes`
- `POST /api/ai/run`
- `GET /api/usage`
- `GET /api/prompts`
- `POST /api/prompts`
- `GET /api/prompts/{prompt_id}/versions`
- `GET /api/queue/tasks`
- `POST /api/queue/tasks`
- `POST /api/queue/tasks/{task_id}/run`
- `POST /api/queue/tasks/{task_id}/replay`
- `GET /api/queue/outbox`
- `GET /api/queue/dlq` returns `{ "total": number, "items": QueueDlqRecord[] }`; items include `stream_name`, `redis_stream_id`, nullable FK ids, raw-id evidence in metadata, `delivery_revision`, `reason`, replay status, DB/Redis/ACK step status, and `last_error`.
- `POST /api/queue/dlq/{record_id}/replay`
- `GET /api/queue/redis/metrics`
- `GET /api/conversations`
- `POST /api/conversations`
- `GET /api/messages?conversation_id={id}`
- `POST /api/messages`
- `GET /api/matches`
- `POST /api/matches/suggest`
- `GET /api/interviews`
- `POST /api/interviews`
- `GET /api/analytics/summary`
- `GET /api/do-not-contact`
- `POST /api/do-not-contact`
- `POST /api/do-not-contact/{entry_id}/lift`
- `GET /api/audit-logs`
- `GET /api/system/errors`

## Planned Resource Groups

- `/api/interviews`
- `/api/matches`
- `/api/settings`
- `/api/analytics`
- `/api/webhooks/meta`
- `/api/webhooks/line`
