"""external automation tracking

Revision ID: 0002_external_automation
Revises: 0001_initial
Create Date: 2026-09-27
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0002_external_automation"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = inspect(op.get_bind())

    def columns(table: str) -> set[str]:
        return {column["name"] for column in inspector.get_columns(table)}

    def indexes(table: str) -> set[str]:
        return {index["name"] for index in inspector.get_indexes(table)}

    message_columns = columns("messages")
    with op.batch_alter_table("messages") as batch:
        if "delivery_status" not in message_columns:
            batch.add_column(sa.Column("delivery_status", sa.String(length=40), nullable=False, server_default="Internal Record"))
        if "platform_message_id" not in message_columns:
            batch.add_column(sa.Column("platform_message_id", sa.String(length=160), nullable=True))
        if "external_status_checked_at" not in message_columns:
            batch.add_column(sa.Column("external_status_checked_at", sa.String(length=40), nullable=True))
        message_indexes = indexes("messages")
        if "ix_messages_delivery_status" not in message_indexes:
            batch.create_index("ix_messages_delivery_status", ["delivery_status"])
        if "ix_messages_platform_message_id" not in message_indexes:
            batch.create_index("ix_messages_platform_message_id", ["platform_message_id"])

    account_columns = columns("platform_accounts")
    with op.batch_alter_table("platform_accounts") as batch:
        if "capabilities" not in account_columns:
            batch.add_column(sa.Column("capabilities", sa.JSON(), nullable=False, server_default="[]"))
        if "work_windows" not in account_columns:
            batch.add_column(sa.Column("work_windows", sa.JSON(), nullable=False, server_default="[]"))
        if "last_success_at" not in account_columns:
            batch.add_column(sa.Column("last_success_at", sa.String(length=40), nullable=True))
        if "last_failure_reason" not in account_columns:
            batch.add_column(sa.Column("last_failure_reason", sa.Text(), nullable=True))

    post_columns = columns("posts")
    with op.batch_alter_table("posts") as batch:
        if "platform_post_id" not in post_columns:
            batch.add_column(sa.Column("platform_post_id", sa.String(length=160), nullable=True))
        if "verified_at" not in post_columns:
            batch.add_column(sa.Column("verified_at", sa.String(length=40), nullable=True))
        if "ix_posts_platform_post_id" not in indexes("posts"):
            batch.create_index("ix_posts_platform_post_id", ["platform_post_id"])

    if "usage_source" not in columns("ai_requests"):
        with op.batch_alter_table("ai_requests") as batch:
            batch.add_column(sa.Column("usage_source", sa.String(length=40), nullable=False, server_default="Estimated"))

    queue_columns = columns("queue_tasks")
    with op.batch_alter_table("queue_tasks") as batch:
        if "idempotency_key" not in queue_columns:
            batch.add_column(sa.Column("idempotency_key", sa.String(length=255), nullable=True))
        if "ix_queue_tasks_idempotency_key" not in indexes("queue_tasks"):
            batch.create_index("ix_queue_tasks_idempotency_key", ["idempotency_key"], unique=True)

    if "inbound_emails" not in inspector.get_table_names():
        op.create_table(
            "inbound_emails",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("message_id", sa.String(length=255), nullable=False),
            sa.Column("sender", sa.String(length=255), nullable=False),
            sa.Column("received_at", sa.String(length=40), nullable=False),
            sa.Column("subject", sa.String(length=500), nullable=False),
            sa.Column("source_platform", sa.String(length=80), nullable=False, server_default="518"),
            sa.Column("job_id", sa.Integer(), sa.ForeignKey("jobs.id"), nullable=True),
            sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=True),
            sa.Column("application_id", sa.Integer(), sa.ForeignKey("applications.id"), nullable=True),
            sa.Column("raw_text", sa.Text(), nullable=False),
            sa.Column("attachment_names", sa.JSON(), nullable=False, server_default="[]"),
            sa.Column("import_status", sa.String(length=40), nullable=False, server_default="Received"),
            sa.Column("parse_error", sa.Text(), nullable=True),
            sa.Column("platform_link", sa.String(length=500), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("message_id", name="uq_inbound_email_message_id"),
        )
        op.create_index("ix_inbound_emails_message_id", "inbound_emails", ["message_id"])
        op.create_index("ix_inbound_emails_received_at", "inbound_emails", ["received_at"])
        op.create_index("ix_inbound_emails_import_status", "inbound_emails", ["import_status"])

    if "platform_automation_runs" not in inspector.get_table_names():
        op.create_table(
            "platform_automation_runs",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("platform", sa.String(length=80), nullable=False),
            sa.Column("capability", sa.String(length=120), nullable=False),
            sa.Column("account_id", sa.Integer(), sa.ForeignKey("platform_accounts.id"), nullable=True),
            sa.Column("queue_task_id", sa.Integer(), sa.ForeignKey("queue_tasks.id"), nullable=True),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Queued"),
            sa.Column("started_at", sa.String(length=40), nullable=True),
            sa.Column("finished_at", sa.String(length=40), nullable=True),
            sa.Column("platform_entity_id", sa.String(length=160), nullable=True),
            sa.Column("evidence", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("error_reason", sa.Text(), nullable=True),
            sa.Column("screenshot_path", sa.String(length=500), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_platform_automation_runs_platform", "platform_automation_runs", ["platform"])
        op.create_index("ix_platform_automation_runs_capability", "platform_automation_runs", ["capability"])
        op.create_index("ix_platform_automation_runs_status", "platform_automation_runs", ["status"])

    if "candidate_merge_conflicts" not in inspector.get_table_names():
        op.create_table(
            "candidate_merge_conflicts",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=True),
            sa.Column("incoming_source", sa.String(length=80), nullable=False),
            sa.Column("conflict_type", sa.String(length=80), nullable=False),
            sa.Column("incoming_payload", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("existing_payload", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Open"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )


def downgrade() -> None:
    op.drop_table("candidate_merge_conflicts")
    op.drop_index("ix_platform_automation_runs_status", table_name="platform_automation_runs")
    op.drop_index("ix_platform_automation_runs_capability", table_name="platform_automation_runs")
    op.drop_index("ix_platform_automation_runs_platform", table_name="platform_automation_runs")
    op.drop_table("platform_automation_runs")
    op.drop_index("ix_inbound_emails_import_status", table_name="inbound_emails")
    op.drop_index("ix_inbound_emails_received_at", table_name="inbound_emails")
    op.drop_index("ix_inbound_emails_message_id", table_name="inbound_emails")
    op.drop_table("inbound_emails")
    with op.batch_alter_table("queue_tasks") as batch:
        batch.drop_index("ix_queue_tasks_idempotency_key")
        batch.drop_column("idempotency_key")
    with op.batch_alter_table("ai_requests") as batch:
        batch.drop_column("usage_source")
    with op.batch_alter_table("posts") as batch:
        batch.drop_index("ix_posts_platform_post_id")
        batch.drop_column("verified_at")
        batch.drop_column("platform_post_id")
    with op.batch_alter_table("platform_accounts") as batch:
        batch.drop_column("last_failure_reason")
        batch.drop_column("last_success_at")
        batch.drop_column("work_windows")
        batch.drop_column("capabilities")
    with op.batch_alter_table("messages") as batch:
        batch.drop_index("ix_messages_platform_message_id")
        batch.drop_index("ix_messages_delivery_status")
        batch.drop_column("external_status_checked_at")
        batch.drop_column("platform_message_id")
        batch.drop_column("delivery_status")
