# AI_PROVIDER

## Supported Provider Architecture

The schema supports:

- OpenAI
- Anthropic
- Google Gemini
- OpenRouter
- Azure OpenAI
- Ollama
- LM Studio
- OpenAI-Compatible API
- Custom REST API

`supports_text`, `supports_vision`, `supports_json_schema`, and `supports_tools` are provider capability metadata. A provider advertising vision does not mean this application has screenshot understanding, OCR, or general computer-control automation implemented. Those require feature-specific code, prompts, safety policy, and tests.

## Task Routing

Example desired routing:

- FB文案: GPT-5.x or configured premium copywriting provider
- 簡單訊息分類: Local Qwen
- 履歷摘要: Claude / Gemini
- FAQ: Local Llama + RAG
- Primary failure: fallback model
- Budget exceeded: cheaper model or local model

Implemented API:

- `GET /api/ai/routes`
- `POST /api/ai/routes`
- `POST /api/ai/run`

If no enabled provider route succeeds, the system records a fallback `AIRequest` and creates a `SystemError` entry for operator visibility.

Important implemented routes:

- `RecruitCopywriting`
- `InitialInquiryClassification`
- `RequiredQuestionAnswerExtraction`
- `CandidateScoring`

`CandidateScoring` accepts only schema-valid, high-confidence, job-related output with no sensitive/non-job-related risk flags. Otherwise the matching service keeps the deterministic rules score and marks the AI result as fallback/manual review.

## Tracking

Every AI request should record:

- Provider
- Model
- Feature
- User
- Candidate
- Job
- Input/output/total/cached tokens
- Estimated or actual cost
- Latency
- Status/error
- Timestamp

## Not Implemented As Complete

- AI voice calling is not complete without a configured telephony provider, STT/TTS or phone-agent adapter, signed callbacks, consent/disclosure handling, retry/de-dupe, and human handoff.
- Local Ollama/OpenAI-compatible providers depend on the operator's hardware, model size, context window, JSON reliability, and latency. They must pass the same schema and confidence gates as hosted providers before being used for automation.
