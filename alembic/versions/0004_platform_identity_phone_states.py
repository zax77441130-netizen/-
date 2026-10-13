"""platform identity maps and phone audit state

Revision ID: 0004_platform_identity_phone_states
Revises: 0003_flow_workers_and_evidence
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0004_platform_identity_phone"
down_revision = "0003_flow_workers_and_evidence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())
    message_columns = {column["name"] for column in inspector.get_columns("messages")} if "messages" in tables else set()
    if "candidate_read_status" not in message_columns:
        op.add_column("messages", sa.Column("candidate_read_status", sa.String(length=40), nullable=False, server_default="Unknown"))
        op.create_index("ix_messages_candidate_read_status", "messages", ["candidate_read_status"])
    if "candidate_read_at" not in message_columns:
        op.add_column("messages", sa.Column("candidate_read_at", sa.String(length=40), nullable=True))

    if "platform_identity_maps" not in tables:
        op.create_table(
            "platform_identity_maps",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("platform", sa.String(length=80), nullable=False),
            sa.Column("account_id", sa.Integer(), sa.ForeignKey("platform_accounts.id"), nullable=True),
            sa.Column("entity_type", sa.String(length=80), nullable=False),
            sa.Column("external_id", sa.String(length=255), nullable=False),
            sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=True),
            sa.Column("conversation_id", sa.Integer(), sa.ForeignKey("conversations.id"), nullable=True),
            sa.Column("job_id", sa.Integer(), sa.ForeignKey("jobs.id"), nullable=True),
            sa.Column("external_candidate_id", sa.String(length=255), nullable=True),
            sa.Column("external_conversation_id", sa.String(length=255), nullable=True),
            sa.Column("external_job_id", sa.String(length=255), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("platform", "account_id", "entity_type", "external_id", name="uq_platform_identity_external"),
        )
        for column in ["platform", "account_id", "entity_type", "external_id", "candidate_id", "conversation_id", "job_id", "external_candidate_id", "external_conversation_id", "external_job_id"]:
            op.create_index(f"ix_platform_identity_maps_{column}", "platform_identity_maps", [column])

    if "phone_call_logs" not in tables:
        op.create_table(
            "phone_call_logs",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=False),
            sa.Column("job_id", sa.Integer(), sa.ForeignKey("jobs.id"), nullable=True),
            sa.Column("conversation_id", sa.Integer(), sa.ForeignKey("conversations.id"), nullable=True),
            sa.Column("provider", sa.String(length=120), nullable=True),
            sa.Column("external_call_id", sa.String(length=255), nullable=True),
            sa.Column("dial_status", sa.String(length=40), nullable=False, server_default="Not Configured"),
            sa.Column("call_result", sa.String(length=120), nullable=True),
            sa.Column("script", sa.Text(), nullable=True),
            sa.Column("transcript", sa.Text(), nullable=True),
            sa.Column("recording_url", sa.String(length=500), nullable=True),
            sa.Column("evidence", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("started_at", sa.String(length=40), nullable=True),
            sa.Column("finished_at", sa.String(length=40), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        for column in ["candidate_id", "job_id", "conversation_id", "external_call_id", "dial_status"]:
            op.create_index(f"ix_phone_call_logs_{column}", "phone_call_logs", [column])


def downgrade() -> None:
    op.drop_table("phone_call_logs")
    op.drop_table("platform_identity_maps")
    op.drop_index("ix_messages_candidate_read_status", table_name="messages")
    op.drop_column("messages", "candidate_read_at")
    op.drop_column("messages", "candidate_read_status")
