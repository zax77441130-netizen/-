# ARCHITECTURE

## Stack

- Backend: FastAPI, SQLAlchemy, Pydantic
- Database: PostgreSQL for production, SQLite for local quick start
- Queue: Redis-backed worker planned; worker process included
- Frontend: Static enterprise dashboard served by FastAPI
- Docs: OpenAPI generated at `/docs`

## Core Modules

- `backend/app/models/domain.py`: business entities and audit/AI tables
- `backend/app/api/routes.py`: REST API layer
- `backend/app/connectors`: Platform Adapter interface and registry
- `backend/app/services/ai.py`: AI copywriting, token estimation, provider test scaffolding
- `backend/app/services/seed.py`: demo company, jobs, candidates, connector records

## Platform Connector Architecture

All platform integrations must implement:

- `connect`
- `disconnect`
- `test_connection`
- `createPost`
- `updatePost`
- `deletePost`
- `fetchMessages`
- `sendMessage`
- `fetchComments`
- `replyComment`
- `fetchApplicants`
- `fetchResume`
- `getStatus`
- `getRateLimit`

Current adapters are semi-automatic placeholders that return `Not Configured` or `Manual Review Required` until legal API credentials and platform terms are confirmed.

## AI Architecture

- AI providers are database entities, not hardcoded constants.
- Task routing is represented by `task_model_routes`.
- Every AI request writes provider, model, feature, tokens, cost, latency, status, masked prompt, and masked response.
- Fallback routing is planned in P0 hardening; local template generation is used for zero-cost first-run copywriting.

## Human-in-the-loop

AI output is stored and shown as draft/suggestion. Hiring, rejection, interview, and placement decisions remain HR-controlled.
