# SECURITY

## Implemented Baseline

- PII-aware domain model
- API key field is encrypted before persistence
- Candidate deduplication reduces duplicate personal data
- Platform statuses never fake a successful connection
- AI output is explicitly advisory
- HttpOnly session cookie login
- RBAC endpoint guards
- Audit logging for core mutations
- Candidate data export and anonymizing delete endpoint
- Candidate-linked identity maps, queue payloads, messages, resumes, and AI prompt/output text are included in anonymizing delete.
- Configurable live-database retention cleanup task with per-run audit records.
- Periodic retention cleanup scheduler with idempotent bucket keys and configurable batch size/retries.
- Do-not-contact governance list
- Application status history for pipeline traceability
- Candidate-specific external sends require verified private recipient mappings; public comments/groups are not used as silent fallbacks.
- Platform recipient IDs are separated from internal conversation keys. FB Messenger uses verified PSID, LINE uses verified `userId`, and FB comment IDs are restricted to the private-reply endpoint.
- Interview reply classification treats negated/contradictory confirmations as manual-review or non-confirmation, preventing no-show cancellation from unsafe substring matches.

## Required Before Production

- HTTPS everywhere
- Secure cookie production toggle and session rotation
- CSRF protection
- Strict CORS allowlist
- Argon2/bcrypt password hashing with password policy
- RBAC middleware and permission tests
- Rate limiting
- Audit logs for important operations
- Data access logs for candidate records
- Upload validation
- Sensitive data masking in AI traces
- Backup/archive deletion workflow. The live DB cleanup task does not purge backup media or external archives.
- Approval workflow for bulk export/delete
- OCR adapter and upload malware/content validation for scanned resume PDFs.
- Authorized E2E evidence for FB/LINE/phone/518/ChickPT before marking platform automation complete.
- Queue depth, oldest waiting task, error rate, worker heartbeat, table capacity, and index monitoring must be wired to production metrics/alerts before long-running operation is accepted.

## AI Recruiting Guardrails

AI must not use protected or sensitive personal traits for hiring decisions. AI may only analyze job-relevant signals such as skills, location fit, shift availability, work experience, certificates, and explicitly stated preferences. HR makes final decisions.

`CandidateScoring` output is accepted only when it is schema-valid, high confidence, and free of sensitive/non-job-related risk flags. AI cannot hire, reject, make salary promises, or override HR.
