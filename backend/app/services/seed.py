from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.core.config import get_settings
from backend.app.models.domain import Candidate, Company, Job, PlatformAccount, User
from backend.app.services.security import hash_password


def seed_demo(db: Session) -> None:
    settings = get_settings()
    if db.scalar(select(Company).limit(1)):
        return
    company = Company(name="Demo Manufacturing Co.")
    db.add(company)
    db.flush()
    admin = User(email="admin@example.com", name="Demo Admin", role="Super Admin", password_hash=hash_password(settings.demo_admin_password or "demo-only-password"))
    db.add(admin)
    jobs = [
        ("Warehouse Operator", "Taoyuan Plant A", "桃園", "日班", "08:00-17:00", "NTD 210/hour"),
        ("Night Shift Operator", "Taoyuan Plant A", "桃園", "夜班", "20:00-05:00", "NTD 240/hour plus night bonus"),
        ("Packing Staff", "Hsinchu Logistics Center", "新竹", "中班", "09:00-18:00", "NTD 36,000/month"),
        ("QC Assistant", "Taichung Factory", "台中", "日班", "08:30-17:30", "NTD 34,000/month"),
    ]
    for title, location, area, shift, hours, salary in jobs:
        db.add(
            Job(
                title=title,
                company_id=company.id,
                location=location,
                area=area,
                job_type="Full-time",
                shift=shift,
                hours=hours,
                salary_text=salary,
                description="Manufacturing and logistics recruiting demo job. Duties vary by site and shift.",
                requirements="Basic attendance reliability, safety awareness, and willingness to follow site rules.",
                required_headcount=20 if "Night" in title else 8,
                urgency="High" if "Night" in title else "Normal",
                contact_name="HR Team",
                contact_method="LINE / Phone",
                status="Recruiting",
                required_questions=["preferred_area", "preferred_shift", "earliest_start_date", "transportation", "phone_contact_consent"],
            )
        )
    for index in range(1, 21):
        db.add(
            Candidate(
                name=f"Demo Candidate {index:02d}",
                phone=f"09{index:08d}",
                email=f"candidate{index}@example.com",
                region="桃園" if index % 2 else "台中",
                skills=["warehouse", "packing"] if index % 2 else ["QC", "assembly"],
                preferred_area="桃園" if index % 2 else "台中",
                preferred_shift="日班" if index % 3 else "夜班",
                source_platform=["facebook", "line", "518", "email"][index % 4],
                tags=["demo"],
            )
        )
    for platform in ["facebook", "518", "chickpt", "line", "email", "web"]:
        db.add(PlatformAccount(platform=platform, display_name=f"{platform} demo connector", status="Not Configured", capabilities=[], config={}))
    db.commit()
