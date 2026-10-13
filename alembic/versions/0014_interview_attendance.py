"""interview attendance

Revision ID: 0014_interview_attendance
Revises: 0013_document_purge_queue_safety
Create Date: 2026-10-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0014_interview_attendance"
down_revision = "0013_document_purge_queue_safety"
branch_labels = None
depends_on = None


def _columns() -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns("interviews")}


def upgrade() -> None:
    columns = _columns()
    with op.batch_alter_table("interviews") as batch:
        if "attendance_status" not in columns:
            batch.add_column(sa.Column("attendance_status", sa.String(length=40), nullable=False, server_default="Not Arrived"))
        if "attended_at" not in columns:
            batch.add_column(sa.Column("attended_at", sa.String(length=40), nullable=True))
        if "attendance_evidence" not in columns:
            batch.add_column(sa.Column("attendance_evidence", sa.JSON(), nullable=False, server_default="{}"))
    if "attendance_status" not in columns:
        op.create_index("ix_interviews_attendance_status", "interviews", ["attendance_status"])


def downgrade() -> None:
    columns = _columns()
    if "attendance_status" in columns:
        op.drop_index("ix_interviews_attendance_status", table_name="interviews")
    with op.batch_alter_table("interviews") as batch:
        if "attendance_evidence" in columns:
            batch.drop_column("attendance_evidence")
        if "attended_at" in columns:
            batch.drop_column("attended_at")
        if "attendance_status" in columns:
            batch.drop_column("attendance_status")
