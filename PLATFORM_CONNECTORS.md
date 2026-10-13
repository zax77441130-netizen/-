# Platform Connectors

## Adapter Contract

Each connector implements `backend/app/connectors/base.py` and reports explicit capability states:

- `Not Configured`: credentials, endpoint, mailbox, or session state is missing.
- `Paused`: browser/session/login verification is required before the task may continue.
- `Delivery Unconfirmed`: an operation cannot be considered successful until platform evidence is checked.
- `Delivered`: a message or post has a platform id and verification evidence.
- `Failed`: the adapter knows the operation failed.

The system must never treat `Queued`, `Pending Send`, `Not Configured`, `Paused`, or `Delivery Unconfirmed` as a successful external operation.

## Current Implementations

- Facebook: `ConfiguredApiAdapter`; official API endpoint/scopes required for publish, message read/send, and verification.
- LINE: `ConfiguredApiAdapter`; official API endpoint/scopes required.
- Web: `ConfiguredApiAdapter`; endpoint/capability driven.
- Email: `EmailAdapter`; used for 518 inbound-first workflow and outbound email only when candidate reply address is confirmed.
- 518: `Recruit518Adapter`; Email is the primary applicant source. Browser resume fetch is paused until authorized session state is supplied.
- ChickPT: `ChickPTAdapter`; browser-worker boundary exists, but live actions are paused until an authorized persistent browser session and test account are supplied.

## Automation Worker Rules

- Verify account, job, candidate, and target before each external action.
- Resolve the platform recipient through `platform_identity_maps`; never use internal `conversation.id` as a platform conversation/recipient id.
- Candidate-specific messages must resolve to a verified private recipient. FB public comments and LINE group/room mappings are not valid private targets.
- Keep the internal conversation key and platform recipient ID separate. For Facebook Messenger send `recipient.id` must be the verified PSID; for LINE push `to` must be the verified `userId`; a Facebook comment ID is valid only for the private-reply endpoint.
- Facebook public comments may enqueue one idempotent private-reply invitation. If private recipient proof is still missing afterward, all candidate-specific automation pauses for Manual Review.
- After each external action, record platform id, URL, screenshot/evidence, and verification time.
- Separate send lifecycle states: local queue state, external send/read-back verification, and candidate read status are distinct fields.
- If login expires, page layout changes, verification is requested, or the result is uncertain, pause the task and notify an operator.
- Browser workers also pause on HTTP 403/429, selector version mismatch, unauthorized domains/actions, and missing logged-in evidence.
- If CAPTCHA appears, stop and alert. Do not bypass CAPTCHA or platform safety checks.
- Do not retry indefinitely. Queue retries are bounded and exhausted tasks move to `Manual Review Required`.
- Credentials, session files, and tokens belong in environment/secret storage or protected runtime paths, never in Git, logs, or frontend responses.

## 518 Mailbox Rules

- IMAP polling uses UID fetch. Messages are imported and flushed before the worker marks them `\Seen`.
- Duplicate `Message-ID` returns the existing inbound record.
- Link-only notifications are fetched only from allowed 518 domains.
- Notification sender addresses are not treated as candidate reply addresses.

## Phone Rule

Phone dialing requires a real provider with dial endpoint, callback status, call result, recording, and transcript evidence. Without that configuration, `phone.dial` creates an auditable Not Configured record and fails the queue task for manual review.

AI phone calling is not complete unless a provider adapter supplies STT/TTS or voice-agent capability, consent/disclosure handling, signed callback de-dupe, structured intent extraction, uncertain/refusal human handoff, and private-message fallback after failed calls.
