import json

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.app.models.domain import Application, Candidate, Interview, Job, MatchRecord, TaskModelRoute
from backend.app.services.ai import run_task_with_router_sync
from backend.app.services.queue import has_active_recruitment_blacklist


def build_match(db: Session, candidate: Candidate, job: Job) -> MatchRecord:
    signals: dict[str, bool | str] = {}
    score = 0.0
    missing: list[str] = []

    if has_active_recruitment_blacklist(db, candidate, job.id):
        match = MatchRecord(
            candidate_id=candidate.id,
            job_id=job.id,
            score=0,
            signals={"recruitment_blacklist": True, "contact_allowed": False},
            missing_info=[],
            recruiter_notes="RecruitmentBlacklist blocks this candidate for this recruiting scope. DNC remains separate and blocks all contact.",
            status="Blocked",
        )
        db.add(match)
        return match

    if candidate.preferred_area:
        area_match = candidate.preferred_area == job.area or candidate.region == job.area
        signals["area_match"] = area_match
        score += 30 if area_match else 0
    else:
        missing.append("preferred_area")

    if candidate.preferred_shift:
        shift_match = candidate.preferred_shift in job.shift or job.shift in candidate.preferred_shift
        signals["shift_match"] = shift_match
        score += 25 if shift_match else 0
    else:
        missing.append("preferred_shift")

    candidate_skills = set(candidate.skills or [])
    requirement_text = (job.requirements or "") + " " + (job.description or "")
    skill_hits = [skill for skill in candidate_skills if skill and skill in requirement_text]
    signals["skill_hits"] = skill_hits
    score += min(25, len(skill_hits) * 10)

    if candidate.phone or candidate.email:
        signals["contactable"] = True
        score += 10
    else:
        missing.append("contact")

    if job.status == "Recruiting":
        signals["job_open"] = True
        score += 10
    else:
        signals["job_open"] = False

    notes = "Rules score fallback. AI analysis is advisory only. HR must confirm interview, hire, reject, and placement decisions."
    route = db.scalar(select(TaskModelRoute).where(TaskModelRoute.task_name == "CandidateScoring"))
    if route:
        ai_prompt = {
            "instruction": (
                "Score a recruiting candidate only from job-related, explainable signals. "
                "Return strict JSON with score, reasons, missing_info, confidence, and risk_flags. "
                "Do not use sensitive or non-job-related attributes. Do not make hiring, rejection, salary, or offer decisions."
            ),
            "candidate": {
                "id": candidate.id,
                "region": candidate.region,
                "skills": candidate.skills or [],
                "licenses": candidate.licenses or [],
                "preferred_area": candidate.preferred_area,
                "preferred_shift": candidate.preferred_shift,
                "has_phone": bool(candidate.phone),
                "has_email": bool(candidate.email),
            },
            "job": {
                "id": job.id,
                "title": job.title,
                "area": job.area,
                "job_type": job.job_type,
                "shift": job.shift,
                "hours": job.hours,
                "salary_text": job.salary_text,
                "description": job.description,
                "requirements": job.requirements,
                "status": job.status,
            },
            "required_schema": {
                "type": "object",
                "required": ["score", "reasons", "missing_info", "confidence", "risk_flags"],
                "properties": {
                    "score": {"type": "number"},
                    "reasons": {"type": "array"},
                    "missing_info": {"type": "array"},
                    "confidence": {"type": "number"},
                    "risk_flags": {"type": "array"},
                },
            },
        }
        result = run_task_with_router_sync(db, "CandidateScoring", json.dumps(ai_prompt, ensure_ascii=False), candidate_id=candidate.id, job_id=job.id, commit=False)
        if result["status"] == "success":
            try:
                decoded = json.loads(result["output"])
            except json.JSONDecodeError:
                decoded = {}
            confidence = float(decoded.get("confidence") or 0)
            risk_flags = decoded.get("risk_flags") or []
            if confidence >= 0.75 and not risk_flags:
                score = min(max(float(decoded.get("score") or score), 0), 100)
                missing = [str(item) for item in decoded.get("missing_info") or missing]
                signals["ai_scoring_source"] = f"{result['provider']}/{result['model']}"
                signals["ai_reasons"] = decoded.get("reasons") or []
                notes = "CandidateScoring AI route used structured job-related signals. HR final decision is required."
            else:
                signals["ai_scoring_source"] = "manual_review_required"
                signals["ai_scoring_rejected"] = {"confidence": confidence, "risk_flags": risk_flags}
        else:
            signals["ai_scoring_source"] = "rules_fallback"
    match = MatchRecord(
        candidate_id=candidate.id,
        job_id=job.id,
        score=min(score, 100),
        signals=signals,
        missing_info=missing,
        recruiter_notes=notes,
    )
    db.add(match)
    return match


def analytics_summary(db: Session) -> dict:
    by_platform = db.execute(select(Candidate.source_platform, func.count()).group_by(Candidate.source_platform)).all()
    by_area = db.execute(select(Job.area, func.count()).group_by(Job.area)).all()
    status_rows = db.execute(select(Application.status, func.count()).group_by(Application.status)).all()
    return {
        "jobs": db.scalar(select(func.count(Job.id))) or 0,
        "candidates": db.scalar(select(func.count(Candidate.id))) or 0,
        "applications": db.scalar(select(func.count(Application.id))) or 0,
        "interviews": db.scalar(select(func.count(Interview.id))) or 0,
        "hired": db.scalar(select(func.count(Candidate.id)).where(Candidate.status == "Hired")) or 0,
        "talent_pool": db.scalar(select(func.count(Candidate.id)).where(Candidate.status == "Talent Pool")) or 0,
        "by_platform": {row[0]: row[1] for row in by_platform},
        "by_area": {row[0]: row[1] for row in by_area},
        "funnel": {row[0]: row[1] for row in status_rows},
    }
