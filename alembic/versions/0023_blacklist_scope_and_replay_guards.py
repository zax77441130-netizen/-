"""blacklist scope and replay guards

Revision ID: 0023_blacklist_scope_and_replay_guards
Revises: 0022_queue_dlq_pointer_recovery
Create Date: 2026-10-08 06:45:00
"""

from alembic import op
import sqlalchemy as sa
import re


revision = "0023_blacklist_replay_guards"
down_revision = "0022_queue_dlq_pointer_recovery"
branch_labels = None
depends_on = None


def _columns(table: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table)}


def _indexes(table: str) -> set[str]:
    return {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(table)}


def upgrade() -> None:
    if "recruitment_blacklist" not in sa.inspect(op.get_bind()).get_table_names():
        return
    columns = _columns("recruitment_blacklist")
    if "scope_type" not in columns:
        op.add_column("recruitment_blacklist", sa.Column("scope_type", sa.String(length=40), nullable=False, server_default="candidate"))
    if "scope_id" not in columns:
        op.add_column("recruitment_blacklist", sa.Column("scope_id", sa.String(length=120), nullable=True))
    bind = op.get_bind()
    bind.execute(sa.text("update recruitment_blacklist set scope_type = 'legacy' where scope_id is null"))
    rows = bind.execute(
        sa.text(
            """
            select id, candidate_id, phone, email, reason, rule_code, created_by, evidence, status, created_at, updated_at
            from recruitment_blacklist
            where scope_id is null
            """
        )
    ).mappings()
    for row in rows:
        job_ids = list(dict.fromkeys(re.findall(r"\bjob:(\d+)\b", f"{row['rule_code'] or ''} {row['evidence'] or ''}")))
        if not job_ids:
            continue
        bind.execute(sa.text("update recruitment_blacklist set scope_type = 'job', scope_id = :scope_id where id = :id"), {"scope_id": job_ids[0], "id": row["id"]})
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
    existing_indexes = _indexes("recruitment_blacklist")
    if "ix_recruitment_blacklist_scope_type" not in existing_indexes:
        op.create_index("ix_recruitment_blacklist_scope_type", "recruitment_blacklist", ["scope_type"])
    if "ix_recruitment_blacklist_scope_id" not in existing_indexes:
        op.create_index("ix_recruitment_blacklist_scope_id", "recruitment_blacklist", ["scope_id"])


def downgrade() -> None:
    if "recruitment_blacklist" not in sa.inspect(op.get_bind()).get_table_names():
        return
    existing_indexes = _indexes("recruitment_blacklist")
    if "ix_recruitment_blacklist_scope_id" in existing_indexes:
        op.drop_index("ix_recruitment_blacklist_scope_id", table_name="recruitment_blacklist")
    if "ix_recruitment_blacklist_scope_type" in existing_indexes:
        op.drop_index("ix_recruitment_blacklist_scope_type", table_name="recruitment_blacklist")
    columns = _columns("recruitment_blacklist")
    if "scope_id" in columns:
        op.drop_column("recruitment_blacklist", "scope_id")
    if "scope_type" in columns:
        op.drop_column("recruitment_blacklist", "scope_type")
