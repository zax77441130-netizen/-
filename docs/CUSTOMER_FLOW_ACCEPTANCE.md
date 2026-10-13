# Customer Flow Acceptance Matrix

This document maps the requested recruiting automation flow to executable code, queues, connectors, tests, and remaining evidence. A node is not accepted as fully automated unless it has runnable implementation, automated tests, and authorized account E2E or production observation evidence.

## 2026-10-04 Second Acceptance Addendum

### Implemented And Locally Contract-Tested

- Candidate deletion preserves newly created purge tasks, does not mark `StoredDocument.deleted_at` before deletion proof, and records purge evidence after worker-confirmed local-file deletion.
- Duplicate PDF upload no longer fails on SHA uniqueness and no longer reuses another candidate's OCR queue task.
- OCR scanned-page path now uses PDF rasterization (`pdftoppm`) followed by Tesseract TSV confidence parsing. Mixed embedded-text/scanned documents are handled as mixed evidence. Local host lacks OCR binaries, so real binary execution is Docker/customer-environment pending.
- Phone callback disclosure proof is strict, negative/uncertain phrases are handled before positive availability, and DNC requests create active DNC records.
- Calendar availability fails closed for missing/errored Google or Microsoft Graph free/busy responses, validates scheduling parameters, rejects queue/API tokens, and verifies read-back by id/time/status/attendees before `Booked`.
- Calendar success is required before updating official interview time/status and reminders. Reminder/no-show tasks are idempotently rescheduled from true UTC interview start.
- Interview `Confirmed` and attendance are split; no-show remains active until HR/onsite verified `Arrived`.
- Running queue tasks cannot be re-run through the manual API.

### Local/Mock Passed

- Backend regression: `65 passed`.
- Targeted regression for this addendum: `12 passed`.
- Compile, frontend syntax, temporary SQLite migration to `0014_interview_attendance`, HTTP/UI smoke, and 10,000-row bulk import smoke passed.

### Not Verified Or Not Implemented

- Customer Google/Outlook OAuth refresh, webhook sync, live booking/read-back, and provider idempotency are not customer-environment verified.
- True installed OCR binary run on this host was not possible because `tesseract` and `pdftoppm` are not installed locally; Dockerfile installs them for container builds.
- Live phone STT/TTS, multi-turn voice agent, and provider human transfer are not implemented.
- PostgreSQL FK/migration test, Docker compose smoke, worker soak, backup restore/purge drill, and FB/LINE/518/ChickPT customer E2E were not run here.
- Frontend operational screens and the full end-to-end recruiting business state machine remain incomplete.

## 2026-10-04 Acceptance Addendum

### Implemented And Locally Tested

- Unified candidate inbound handling across API-created candidate messages, queue worker `flow.process_inbound_message`, and platform sync `platform.sync_messages`.
- Interview replies are classified before general resume/application question handling. Confirm, decline, cancel, reschedule, uncertain, low-confidence, and risk-flag replies are consumed by interview flow and are not saved as required-question answers.
- Unrelated replies still proceed to normal candidate question-answer classification.
- Reschedule and unclear/contradictory replies move the interview to a manual-review state, cancel reminder/no-show tasks, and create idempotent manual exceptions.
- No-show worker re-checks latest candidate replies and cancels without sending "no response" messaging when a candidate reply is present but unsafe to classify.
- No-show ignores candidate messages older than the interview and reuses prior interview-reply metadata to avoid duplicate AI cost.
- Retention cleanup now rejects unknown model/strategy combinations, filters cleanable `QueueTask`/`SystemError` rows before sort/limit, records backlog metrics, and creates bounded continuation tasks.
- AI phone flow now has a replaceable adapter path, consent disclosure requirement, DNC and duplicate-dial protection, signed callback replay protection, disclosure playback evidence gates, and low-confidence text-classification manual review. The actual provider request body is tested to include the same disclosure script stored in `PhoneCallLog`.
- Calendar booking now has replaceable mock/Google/Microsoft Graph adapter paths, configurable weekday/hourly/two-interviewer rules, candidate availability windows, full interval overlap checks, idempotent booking keys, and a read-back gate before `Booked`.
- OCR now streams upload reads with size limits, validates PDF magic/structure, stores PDF source files as `StoredDocument`, queues OCR by document id only, splits embedded-text PDF extraction from scanned-PDF OCR, and routes blank/encrypted/unconfigured/low-confidence/oversized PDFs to manual review.
- External purge requests now have auditable records, legal-hold handling, retryable queue tasks, dry-run `Not Deleted` status, and provider delete/read-back evidence before a live deletion can be marked complete.
- Redis queue now has local reliability implementation and tests for transactional outbox publish/consume, persistent recovery cursor, queryable DLQ records, DLQ write/ACK recovery, stale/trimmed DLQ pointer rewrite before ACK, guarded replay, multi-worker claim competition, cursor wrap, restart persistence, and bounded DB-driven recovery beyond earlier scan boundaries. Docker Postgres+Redis soak remains required before production acceptance.

### Explicitly Not Accepted As Fully Automated

- AI phone calling: local provider path and signed callback safety are implemented, but full live STT/TTS/provider operation and human transfer are not customer-environment verified. Transcript rules are text classification only.
- Calendar auto-booking: adapter paths and read-back gating are implemented, but Google/Outlook OAuth credentials, live atomic hold/book behavior, duplicate webhook handling, and customer read-back are not customer-environment verified.
- Scanned PDF OCR: adapter path, upload hardening, local PDF text extraction and low-confidence routing are implemented, but real scanned-image OCR quality for Traditional Chinese/English documents is not customer-environment verified.
- Redis distributed queue: PostgreSQL remains the source of truth while Redis Streams distributes work. Local tests cover reliable outbox delivery, leases/heartbeat, DLQ records, invalid FK evidence, duplicate-safe Redis DLQ recovery, stale Redis DLQ index/pointer repair before ACK, guarded replay, cursor wrap/restart recovery, and bounded recovery beyond earlier scan boundaries; production acceptance still requires Docker Postgres+Redis soak, shutdown recovery observation, alert thresholds, and rollback drill.
- Backup/archive purge: live DB retention does not prove backups, object storage, email archives, or external provider copies were deleted. Lifecycle rules, legal hold exceptions, deletion audit, retries, and purge drills remain required.

### Required Local Acceptance Before Production Claim

- Full `pytest` suite.
- `compileall`.
- Frontend syntax check.
- Alembic upgrade against a clean DB and migrated legacy DB.
- Postgres/Redis compose smoke with Redis queue enabled and multiple workers.
- Browser UI smoke.
- 10,000 representative load run.
- Worker soak with queue depth/oldest-delay/failure metrics.
- Backup restore and purge drill.

### Required Customer Authorized Evidence Before Production Claim

- FB/LINE webhook, outbound private messaging, and read-back with authorized accounts.
- 518 UID/UIDVALIDITY import and post-commit ACK.
- ChickPT authorized selector/session run with CAPTCHA/login-expiry/403/429 handling.
- Phone provider signed callbacks for connected, no-answer, rejected, failed, replay, and bad-signature cases.
- Calendar real hold/book/reschedule/cancel/read-back.
- OCR on real scanned resumes, including low-confidence and encrypted/blank files.
- Production quota, 403/429, CAPTCHA, login expiry, and rate-limit observations.
- External backup/object/archive purge drill with failure retry and legal hold exception evidence.

## Evidence Levels

- Unit: local function/API behavior is covered by automated tests.
- Mock: connector or platform behavior is covered by fixture-backed integration tests.
- Authorized E2E: a real authorized customer/platform test account completed the flow and read-back verification.
- Production Observation: live production logs show repeated successful execution under customer authorization.

## Acceptance Matrix

| Flow node | Trigger | State transition | Queue task | Connector | Success criteria | Failure criteria | Retry strategy | Required settings | Automated tests | Authorized E2E result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Job created | HR creates job | Job Draft/Recruiting | None | Internal API | Job persisted with required questions | Missing job fields | API validation | Company/branch/job config | Unit: jobs seeded/create | NOT FULLY AUTOMATED: no authorized platform E2E needed |
| AI copy generation | `/ai/copywriting` or job flow | Draft copy generated | AI route execution | AIProvider adapter | `RecruitCopywriting` route primary/fallback/local fallback used; AIRequest recorded | Provider/schema/budget error | Route fallback then Local Template | TaskModelRoute, provider, budget policy | Unit: copywriting usage; provider contract tests added | NOT FULLY AUTOMATED: no live provider account evidence |
| FB post / inbound | Post created/scheduled or FB webhook | Post Queued -> Accepted/Verified; inbound message stored -> inbound AI queue | `platform.create_post`, `post.verify`, `flow.process_inbound_message`, webhook ingest | FacebookAdapter | Graph API returns platform post id and read-back verifies; signed webhook stores Messenger text or comment text under `page + post + commenter`; post external ID maps back to job where available; echo/delivery/read/non-text events ignored | 4xx/5xx/no post id/bad signature/replay/wrong commenter mapping/no job mapping | Queue retry + verification retry; webhook replay rejected; AI worker retry/manual exception | Page/group id, page token, app secret, verify token, quotas, post identity map | Mock: fixture delivery/verification; Unit: FB two-commenter isolation, echo ignore, new comment to job/application AI queue | NOT FULLY AUTOMATED: no authorized FB account evidence |
| 518 email import | Scheduler creates poll task | Email Received -> Parsed/Pending Resume Fetch | `email.poll_518`, `email.ack_518`, `platform.fetch_resume` | EmailAdapter, Recruit518Adapter | BODY.PEEK import, DB commit, then ACK/Seen; Message-ID/synthetic dedupe | Bad domain, parse failure, duplicate, missing job | Queue retry, ack retry | IMAP account, mailbox, `auto_poll_518`, allowed 518 domains | Unit: UID/link/duplicate/unsafe link | NOT FULLY AUTOMATED: no authorized mailbox evidence |
| ChickPT import/post/message | Worker sync/send/post task | Candidate/message/resume imported; post/message Accepted or Verified | `platform.sync_messages`, `platform.fetch_resume`, `platform.create_post`, `message.send`, read-back verify | ChickPTAdapter/browser worker | Authorized session validates login, uses selectors, saves external ids/url/status/evidence | Login expired/CAPTCHA/unknown layout/403/429/selector mismatch | Pause to manual exception queue and account circuit state | Persistent session path or user_data_dir outside git, selector registry/version, allowed domains/actions | Mock: fixture fetch/sync; Unit: worker pauses without authorized selectors | NOT FULLY AUTOMATED: no authorized ChickPT E2E evidence |
| Platform limits | Before any external send/post | Allowed or blocked/paused | Guard in `message.send`, `platform.create_post`, regional/recall/fallback | Internal policy | Per account/candidate/group quota/cooldown/quiet hours/duplicate suppression enforced | 403/429/CAPTCHA/permission/duplicate/over quota | Retry-After/backoff or pause account/manual review | `PlatformAccount.config.rate_policy` | Unit: duplicate message suppression | NOT FULLY AUTOMATED: production quota observation required |
| Blacklist check | Before contact/match | Contact blocked or continues | `message.send`, `phone.dial`, matching guards | Internal | DNC blocks all contact; RecruitmentBlacklist blocks recruiting rules/job scope | Contact sent despite active block | No retry; manual review | DNC and recruitment blacklist entries | Unit: DNC messaging block; Unit: blacklist match/contact guard | NOT FULLY AUTOMATED: production observation pending |
| AI initial score/resume summary | Resume/application import | New -> HR Review or question flow | AI route execution | AIProvider adapter | `CandidateScoring` route can provide schema-valid score/reasons/missing/confidence; low confidence/risk/provider failure falls back to rules scoring | Invalid JSON/schema/provider failure/sensitive factor/low confidence | Rules fallback + manual review markers | Routes: ResumeSummary, CandidateScoring | Unit: route fallback and CandidateScoring integration path | NOT FULLY AUTOMATED: live provider E2E not run |
| Suitable / unsuitable | Match/status update | New -> Unsuitable/Suitable/Closed with status history | `flow.unsuitable_rejection_due`, cancellation paths, outbox tasks | Internal + platform message | Status source/history/event key stored; Unsuitable schedules distinct three-day rejection; status changes cancel pending rejection | Missing status evidence, duplicate event, candidate reply, DNC, missing private mapping | Idempotent status/rejection queue key; manual review on missing mapping | State policy | Unit: status update schedules unsuitable rejection | NOT FULLY AUTOMATED: production observation pending |
| Auto rejection | Explicit Unsuitable status event | Unsuitable -> Closed + approved rejection message queued after three days | `flow.unsuitable_rejection_due`, `message.send` | Messaging connector | Re-checks status at due time; DNC/candidate reply/status change/duplicate notice cancel; verified private recipient required | No mapping/DNC/provider fail/reply/status change | Queue retry then manual review; no duplicate notice | Message templates, platform account/private identity map | Unit: unsuitable rejection due and idempotent message | NOT FULLY AUTOMATED: authorized platform send/read-back not run |
| Required questions | Ask next question | Question Unanswered -> Pending Send -> Delivered/Asked -> Answered/Needs Clarification/manual exception | `message.send`, `message.verify`, `flow.follow_up_inactive` | Messaging connector + AI router | One question per task, answer evidence saved only against the latest actually delivered question; approved template metadata recorded | No conversation, mapping missing, reply without delivered question | Queue retry/manual review | Job required_questions, external conversation mapping, AI route `RequiredQuestionAnswerExtraction` | Unit/mock: required question flow, delivered-question guard, reply-without-asked-question manual exception | NOT FULLY AUTOMATED |
| Candidate reply classification | Candidate message import/webhook | Answer/message classified by real AI route; next question sent only when clear | AI route execution + deterministic question correlation | Platform message sync/webhook + AI | Reply stored, AIRequest recorded with provider/model/cost, pending follow-up for same question canceled, ambiguous/multi-value/low-confidence answers routed to review | Ambiguous/no conversation/wrong question mapping/template fallback/schema failure | Manual review for ambiguity or failed AI route | Message routes, webhook secrets, AI routes: RequiredQuestionAnswerExtraction plus supporting route definitions | Unit: real mocked AI route call, AIRequest assertion, LINE group member isolation, bad signature no-write | NOT FULLY AUTOMATED: live AI/platform evidence absent |
| Multi-job inquiry | Candidate asks multiple jobs | Conversation needs HR review | AI route execution | Messaging connector | Identifies multi-job intent | Ambiguous intent | Manual review | Job catalog | None | NOT FULLY AUTOMATED |
| 3-day no reply follow-up | Required question delivery verified | Waiting Reply -> per-question follow-up task due in 3 days -> follow-up sent | `message.verify`, `flow.follow_up_inactive`, `message.send` | Messaging connector | Follow-up task uses question ID + message ID and starts only after delivery/read-back evidence; only matching question reply cancels it | Candidate replied to same question/DNC/job closed/status advanced/missing delivery evidence | Cancel matching task; retry send; manual review on mapping/evidence failure | Follow-up template, conversation mapping, delivery evidence | Unit: scheduler creation after verify and cancellation through question flow | NOT FULLY AUTOMATED: production observation pending |
| Phone invite | Status transition | Contacting -> phone dial task | `phone.dial` | Phone PlatformAccount | Provider accepts dial and callback updates status | No provider/signature fail/DNC | Queue retry/manual review | Phone account config, signed webhook secret | Unit: not-configured guard; signed callback tests added | NOT FULLY AUTOMATED: no live phone provider |
| Connected / no answer / rejected / failed | Phone callback | Call status updated | Webhook callback | Phone provider | Signed callback, timestamp, nonce, idempotent call id | Bad signature/replay/unknown call | No retry for bad signatures; provider retries signed callback | `PHONE_WEBHOOK_SECRET` | Unit: signed callback | NOT FULLY AUTOMATED |
| Missed call fallback message | Phone result no answer/rejected/failed | Message queued on original conversation channel | `message.send` | Messaging connector | Conversation mapping verified before send; DNC checked; duplicate callback does not duplicate task | Missing conversation/mapping/DNC | Queue retry/manual review | Fallback platform/account mapping | Unit: signed callback fallback idempotency | NOT FULLY AUTOMATED |
| Interview accepted/created/reminded | Candidate accepts / HR creates interview | Interview Scheduled + timezone-normalized reminders + no-show check | `flow.interview_reminder`, `flow.interview_no_show_check` | Messaging connector | Reminder and 30-minute no-response tasks created idempotently from validated reminder policy and IANA timezone | Missing conversation/DNC/bad interview time/bad timezone/bad reminder policy | Queue retry/manual review/API validation | Reminder policy, IANA timezone/calendar policy | Unit: interview create schedules reminders/no-show; timezone validation and UTC conversion | NOT FULLY AUTOMATED: production observation pending |
| Arrival confirmation / 30-min no reply | No explicit confirmation by check time | Scheduled -> No Response Follow-up + reschedule message | `flow.interview_no_show_check`, `message.send` | Messaging/internal notification | Only fires for Scheduled/Pending Confirmation states; DNC checked; message queued for reschedule inquiry | Status already changed, DNC, missing conversation | Cancel if not applicable; manual review on missing mapping | Interview policy | Unit: no-show task creation; handler path implemented | NOT FULLY AUTOMATED |
| LINE official redirect / inbound | Flow event or LINE webhook | Candidate directed to LINE; inbound stored/classified | `message.send`, webhook ingest | LineAdapter | LINE request id/retry key logged; signed webhook creates inbound message keyed by destination + group/room + user; later 1:1 user reply can resolve back to the same candidate/job/conversation when unambiguous; delivery/read still requires webhook evidence | Missing token/mapping/bad signature/replay/non-text event/ambiguous job mapping | Queue retry/manual review; replay rejected | LINE channel token, channel secret, external ids | Mock: fixture message; Unit: signed webhook dedupe, two group-member isolation, group-to-1:1 continuity | NOT FULLY AUTOMATED |
| Regional group notification | Job/candidate area event | Group notified | `flow.regional_notify` | Platform adapter | Allowed group receives message; platform id logged | Quota/quiet hours/DNC | Backoff/manual review | Group ids, quota policy | Unit path exists | NOT FULLY AUTOMATED |
| Talent pool recall | Shortage event | Talent Pool candidates contacted | `flow.shortage_recall`, `message.send` | Messaging connector | Area-matched candidates queued excluding DNC | DNC/missing mapping | Queue retry/manual review | Talent pool rules | Unit path exists | NOT FULLY AUTOMATED |
| DNC blocking | Any outbound contact | Blocked/manual review | Guard in send/dial | Internal | No contact task reaches external adapter | Active DNC ignored | No retry | DNC table | Unit: DNC block | Unit/Mock only |
| RecruitmentBlacklist filtering | Recruiting-specific decision | Blocked for job/process only | Matching/flow guard | Internal | Candidate blocked for specific recruiting rule but not global DNC | Treated as DNC or ignored | Manual review | Rule code/job scope | API CRUD tests only | NOT FULLY AUTOMATED |

## Current Verification Summary

- Prior clean targeted backend run: `python -m pytest backend\tests\test_core.py -q` passed locally on 2026-10-02: 37 passed. The final delivery run is recorded in `docs/VALIDATION_REPORT.md`.
- This round adds mock/unit coverage for FB private reply using comment ID, Facebook Messenger PSID payload, LINE `userId` push payload, structured interview reply/no-show cancellation, retention scheduler idempotency, and portable package ZIP paths.
- Current evidence is Unit and Mock only.
- Authorized E2E and Production Observation are not available in this workspace because no real customer platform credentials, persistent browser session, phone provider, or mailbox have been supplied.

## Customer Flow Traceability Detail

| Customer node | Program mapping | Data fields | Error/retry path | Current status | Remaining work |
| --- | --- | --- | --- | --- | --- |
| 發文 | `Post`, `platform.create_post`, `post.verify`, platform adapter | `post.platform_post_id`, `post_url`, `verified_at`, `ExternalActionLog.evidence` | Queue retry; account pause on 403/429/CAPTCHA/permission; read-back required | 已實作但未實測 | Authorized FB/ChickPT read-back E2E |
| 518 Email 匯入 | `email.poll_518`, `InboundEmail`, `EmailCursor`, `email.ack_518` | `message_id`, `mailbox`, `imap_uid`, `uid_validity`, `candidate_id`, `application_id` | IMAP ACK only after DB commit; duplicate/synthetic key; unsafe links rejected | 已實作且測試 | Authorized mailbox E2E |
| 履歷讀取 | `platform.fetch_resume`, `Resume`, parser | `raw_text`, `parsed_json`, `platform_link`, `candidate_id` | Allowed 518 domain only; parse failure/manual review | 已實作且測試 | Real 518/ChickPT resume fetch evidence |
| 黑名單 | DNC guard and `RecruitmentBlacklist` guard | `candidate_id`, `phone`, `email`, `rule_code`, `evidence` | DNC blocks all contact; recruitment blacklist blocks scoped recruiting flow | 已實作且測試 | Production observation |
| 評分排序 | `build_match`, `MatchRecord`, AI route placeholders | `score`, `signals`, `missing_info`, `recruiter_notes` | Missing info/advisory; blacklist guard | 已實作但未實測 | Normalize regions and live scoring E2E |
| 合適/不合適 | Application status + message/outbox paths | `Application.status`, `ApplicationStatusHistory`, `Message.delivery_status` | Manual review on missing mapping/DNC | 已實作但未實測 | Full event table for every transition |
| 求職者問答 | `CandidateQuestionAnswer`, `enqueue_next_question`, `record_candidate_answer`, AI router | `question_key`, `asked_at`, `answered_at`, `evidence_message_id`, `delivery_evidence_message_id`, `ai_decision`, AIRequest provider/model/cost | Wrong/no delivered question, template fallback, schema error, low confidence and ambiguous answers create manual exception | 已實作且測試 | Authorized platform read-back E2E |
| 三天未回覆 | `flow.follow_up_inactive` scheduled after `message.verify` | `scheduled_at`, `question_answer_id`, `message_id`, `delivered_at`, `conversation_id` | Candidate reply cancels only matching question follow-up; DNC blocks send | 已實作且測試 | Production observation |
| 電話邀約 | `phone.dial`, `PhoneCallLog`, signed callback | `external_call_id`, `dial_status`, `transcript`, `recording_url` | Signed timestamp/nonce/event dedupe; retry/manual review | 已實作且測試 | Real phone provider E2E; AI voice not implemented |
| 面試安排與提醒 | `Interview`, `flow.interview_reminder` | `interview_date`, `interview_time`, `timezone`, `reminder_policy`, UTC `scheduled_at` | Bad timezone/policy returns validation error; bad time manual exception; DNC blocks reminder | 已實作且測試 | Calendar production policy |
| 未到場改期 | `flow.interview_no_show_check` | `interview.status`, `offset`, no-show message task | Cancels if status already changed; sends reschedule inquiry only for valid states | 已實作且測試 | Authorized messaging E2E |
| FB/LINE | `/webhooks/facebook`, `/webhooks/line`, `PlatformWebhookEvent`, `PlatformIdentityMap`, `flow.process_inbound_message` | `event_id`, `external_conversation_id`, `external_message_id`, `payload_hash`, `job_id`, `application_id`, platform recipient IDs, `private_recipient_verified` | Bad signature writes nothing; replay rejected; echo/non-text ignored; public/group mappings cannot be used for candidate-specific outbound; AI/job ambiguity goes to manual exception | 已實作且測試 | Authorized page/channel E2E |
| 區域通知 | `flow.regional_notify` | `target_group_id`, `body`, platform evidence | Rate policy/DNC/quota/backoff | 已實作但未實測 | Authorized group E2E |
| 人才回流 | `flow.shortage_recall` | `job_id`, talent-pool candidate, message task | DNC and RecruitmentBlacklist skipped | 已實作但未實測 | Scale/load and production observation |
| 共用資料 | `PlatformIdentityMap`, `ExternalActionLog`, `AuditLog` | external IDs, account IDs, candidate/conversation/job mapping | Refuse external action if mapping is missing | 已實作且測試 | Tenant/permission review |
| 資料保留 | Privacy delete/export, `privacy.retention_cleanup`, package exclusions | redacted candidate fields/messages/resumes/identity maps/queue payloads/AI text; `DataRetentionRun`; no DB/cache/session in ZIP | Live DB retention task masks/deletes configured rows; backups/external archives are explicitly out of scope | 已實作且測試 | Archive/export/backup purge drill |

## Required Authorized E2E Evidence Before Full Acceptance

- 518 mailbox poll with real IMAP UID/UIDVALIDITY and post-commit ACK evidence.
- ChickPT authorized persistent browser session for posting, applicant/resume/inbox/reply/read-back.
- Facebook authorized page/group post and read-back.
- LINE official account push/webhook evidence.
- Phone provider dial and signed callback evidence.
- Long-running worker soak with queue depth/error/latency metrics.

## Known Limitations

- `NOT FULLY AUTOMATED` nodes must not be represented as complete in customer reports.
- Fixture-backed tests prove local contracts only; they do not prove platform authorization or production reliability.
- State-machine coverage now includes required-question follow-up, unsuitable three-day rejection, and interview reminder/no-show tasks, but still needs authorized platform evidence and a fuller operations UI before full acceptance.
- Candidate-specific external messages now distinguish internal conversation keys from platform recipient IDs; verified PSID/userId is required for private candidate messages.
- Interview no-show checks re-read latest candidate reply and interview status before sending any no-response message.
- AI phone calling, full calendar auto-booking live OAuth/webhook acceptance, production Redis-backed queue soak, real scanned OCR acceptance, and backup/archive purge remain incomplete or unverified in a customer-authorized environment.

## 2026-10-08 Acceptance Delta

- Redis DLQ data-loss guard is locally implemented: stale Redis DLQ index values and trimmed/missing `dlq_stream_id` pointers are exact-read checked and rewritten before the failed source message can be ACKed.
- Evidence: `backend/app/services/redis_queue.py`, `alembic/versions/0022_queue_dlq_pointer_recovery.py`, and `backend/tests/test_core.py::test_dlq_stale_index_or_trimmed_pointer_rewrites_before_ack`.
- Local verification: Redis/DLQ targeted tests passed (`23 passed`); full backend test file passed (`117 passed`).
- Still unaccepted: Docker Postgres+Redis smoke, Redis restart, multi-worker soak, 10,000 complete business-flow load, backup restore/clear drill, and all authorized FB/LINE/518/ChickPT/phone/Google/Outlook customer-environment tests. Docker could not be run locally because the `docker` CLI is unavailable.

## 2026-10-08 Full Repair Delta

- Settings/health: worker, Redis, lease, retention and OCR settings are read through Pydantic `Settings`; `/api/health` exposes the effective runtime mode.
- Replay safety: stale admin replay cannot overwrite a concurrently claimed `Running` task; replay uses an atomic conditional task update before outbox creation.
- Traceability: `docs/REQUIREMENTS_TRACEABILITY_20261008.md` records the requested 24 items, matching files/tests, and remaining blockers.
- Local verification: focused Redis/settings/replay regression passed (`25 passed`); full backend test file passed (`119 passed`).
- Still not full customer acceptance: live Redis 7/Docker integration, long soak, 10,000 complete flow load, backup restore/purge, phone STT/TTS, Google/Outlook OAuth/sync, full operations UI, and authorized FB/LINE/518/ChickPT/OCR E2E remain incomplete or unverified.
