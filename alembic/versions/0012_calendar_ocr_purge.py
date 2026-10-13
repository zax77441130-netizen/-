"""calendar ocr purge records

Revision ID: 0012_calendar_ocr_purge
Revises: 0011_identity_status_retention
Create Date: 2026-10-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0012_calendar_ocr_purge"
down_revision = "0011_identity_status_retention"
branch_labels = None
depends_on = None


def _tables() -> set[str]:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return set(inspector.get_table_names())


def upgrade() -> None:
    tables = _tables()
    if "calendar_bookings" not in tables:
        op.create_table(
            "calendar_bookings",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("interview_id", sa.Integer(), sa.ForeignKey("interviews.id"), nullable=False),
            sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=False),
            sa.Column("job_id", sa.Integer(), sa.ForeignKey("jobs.id"), nullable=False),
            sa.Column("provider", sa.String(length=80), nullable=False, server_default="mock"),
            sa.Column("interviewer_ids", sa.JSON(), nullable=False, server_default="[]"),
            sa.Column("timezone", sa.String(length=80), nullable=False, server_default="Asia/Taipei"),
            sa.Column("starts_at", sa.String(length=40), nullable=True),
            sa.Column("ends_at", sa.String(length=40), nullable=True),
            sa.Column("hold_token", sa.String(length=255), nullable=True),
            sa.Column("external_event_id", sa.String(length=255), nullable=True),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Pending"),
            sa.Column("idempotency_key", sa.String(length=255), nullable=False),
            sa.Column("read_back", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("failure_reason", sa.Text(), nullable=True),
            sa.UniqueConstraint("idempotency_key", name="uq_calendar_booking_idempotency_key"),
        )
        for column in ["interview_id", "candidate_id", "job_id", "starts_at", "hold_token", "external_event_id", "status", "idempotency_key"]:
            op.create_index(f"ix_calendar_bookings_{column}", "calendar_bookings", [column])
    if "stored_documents" not in tables:
        op.create_table(
            "stored_documents",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=True),
            sa.Column("file_name", sa.String(length=255), nullable=False),
            sa.Column("storage_path", sa.String(length=500), nullable=False),
            sa.Column("content_type", sa.String(length=120), nullable=False, server_default="application/octet-stream"),
            sa.Column("sha256", sa.String(length=64), nullable=False),
            sa.Column("byte_size", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("page_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Stored"),
            sa.Column("retention_policy", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("deleted_at", sa.String(length=40), nullable=True),
            sa.Column("delete_audit", sa.JSON(), nullable=False, server_default="{}"),
        )
        for column in ["candidate_id", "sha256", "status"]:
            op.create_index(f"ix_stored_documents_{column}", "stored_documents", [column])
    if "external_retention_purges" not in tables:
        op.create_table(
            "external_retention_purges",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("provider", sa.String(length=80), nullable=False),
            sa.Column("object_key", sa.String(length=500), nullable=False),
            sa.Column("purge_type", sa.String(length=80), nullable=False, server_default="object"),
            sa.Column("legal_hold", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Pending"),
            sa.Column("requested_at", sa.String(length=40), nullable=False),
            sa.Column("confirmed_at", sa.String(length=40), nullable=True),
            sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("evidence", sa.JSON(), nullable=False, server_default="{}"),
            sa.UniqueConstraint("provider", "object_key", "purge_type", name="uq_external_purge_scope"),
        )
        for column in ["provider", "object_key", "status"]:
            op.create_index(f"ix_external_retention_purges_{column}", "external_retention_purges", [column])


def downgrade() -> None:
    tables = _tables()
    for table in ["external_retention_purges", "stored_documents", "calendar_bookings"]:
        if table in tables:
            op.drop_table(table)
