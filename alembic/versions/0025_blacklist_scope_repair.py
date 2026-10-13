"""repair blacklist scope data

Revision ID: 0025_blacklist_scope_repair
Revises: 0024_queue_replay_records
Create Date: 2026-10-08 12:40:00
"""

from __future__ import annotations

import re

from alembic import op
import sqlalchemy as sa


revision = "0025_blacklist_scope_repair"
down_revision = "0024_queue_replay_records"
branch_labels = None
depends_on = None


def _tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _columns(table: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table)}


def _job_ids(*parts: object) -> list[str]:
    text = " ".join(str(part or "") for part in parts)
    ids = re.findall(r"\bjob\s*[:#-]\s*0*(\d+)\b", text, flags=re.IGNORECASE)
    return list(dict.fromkeys(str(int(item)) for item in ids if item.isdigit()))


def _numeric_id(value: object) -> str | None:
    text = str(value or "").strip()
    return str(int(text)) if text.isdigit() else None


def upgrade() -> None:
    if "recruitment_blacklist" not in _tables():
        return
    columns = _columns("recruitment_blacklist")
    if "scope_type" not in columns:
        op.add_column("recruitment_blacklist", sa.Column("scope_type", sa.String(length=40), nullable=False, server_default="candidate"))
    if "scope_id" not in columns:
        op.add_column("recruitment_blacklist", sa.Column("scope_id", sa.String(length=120), nullable=True))

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            select id, candidate_id, phone, email, reason, rule_code, scope_type, scope_id,
                   created_by, evidence, status, created_at, updated_at
            from recruitment_blacklist
            """
        )
    ).mappings().all()

    for row in rows:
        scope_type = str(row["scope_type"] or "candidate").strip().lower()
        scope_id = str(row["scope_id"] or "").strip() or None
        job_ids = _job_ids(row["rule_code"], row["evidence"])
        numeric_scope = _numeric_id(scope_id)
        if scope_type == "job" and numeric_scope:
            bind.execute(sa.text("update recruitment_blacklist set scope_type='job', scope_id=:scope_id where id=:id"), {"id": row["id"], "scope_id": numeric_scope})
            continue
        if scope_type == "job" and not numeric_scope:
            bind.execute(
                sa.text("update recruitment_blacklist set scope_type='legacy', scope_id=null, status='Pending Scope Confirmation' where id=:id"),
                {"id": row["id"]},
            )
            continue

        if numeric_scope and scope_type in {"candidate", "global", "legacy"}:
            job_ids = list(dict.fromkeys([numeric_scope, *job_ids]))

        if job_ids and scope_type in {"candidate", "global", "legacy", ""}:
            bind.execute(sa.text("update recruitment_blacklist set scope_type='job', scope_id=:scope_id where id=:id"), {"id": row["id"], "scope_id": job_ids[0]})
            for job_id in job_ids[1:]:
                bind.execute(
                    sa.text(
                        """
                        insert into recruitment_blacklist
                        (candidate_id, phone, email, reason, rule_code, scope_type, scope_id, created_by, evidence, status, created_at, updated_at)
                        values
                        (:candidate_id, :phone, :email, :reason, :rule_code, 'job', :scope_id, :created_by, :evidence, :status, :created_at, :updated_at)
                        """
                    ),
                    {**dict(row), "scope_id": job_id},
                )
            continue

        if scope_type == "legacy":
            bind.execute(
                sa.text("update recruitment_blacklist set scope_id=null, status='Pending Scope Confirmation' where id=:id"),
                {"id": row["id"]},
            )
            continue

        if scope_type not in {"candidate", "global"}:
            bind.execute(
                sa.text("update recruitment_blacklist set scope_type='legacy', scope_id=null, status='Pending Scope Confirmation' where id=:id"),
                {"id": row["id"]},
            )


def downgrade() -> None:
    pass
