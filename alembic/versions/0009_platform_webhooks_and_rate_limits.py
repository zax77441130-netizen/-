"""platform webhooks and rate limits

Revision ID: 0009_platform_webhooks_and_rate_limits
Revises: 0008_phone_webhook_events
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0009_platform_webhooks_rate"
down_revision = "0008_phone_webhook_events"
branch_labels = None
depends_on = None


def _indexes(table_name: str) -> set[str]:
    return {item["name"] for item in inspect(op.get_bind()).get_indexes(table_name)}


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "platform_webhook_events" not in tables:
        op.create_table(
            "platform_webhook_events",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("platform", sa.String(length=80), nullable=False),
            sa.Column("account_id", sa.Integer(), sa.ForeignKey("platform_accounts.id"), nullable=True),
            sa.Column("event_id", sa.String(length=255), nullable=False),
            sa.Column("external_conversation_id", sa.String(length=255), nullable=True),
            sa.Column("external_message_id", sa.String(length=255), nullable=True),
            sa.Column("received_at", sa.String(length=40), nullable=False),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Accepted"),
            sa.Column("payload_hash", sa.String(length=64), nullable=False),
            sa.Column("evidence", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("platform", "account_id", "event_id", name="uq_platform_webhook_event"),
        )
    existing = _indexes("platform_webhook_events")
    for column in ["platform", "account_id", "event_id", "external_conversation_id", "external_message_id", "received_at", "status"]:
        name = f"ix_platform_webhook_events_{column}"
        if name not in existing:
            op.create_index(name, "platform_webhook_events", [column])

    if "platform_rate_limit_state" not in tables:
        op.create_table(
            "platform_rate_limit_state",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("platform", sa.String(length=80), nullable=False),
            sa.Column("account_id", sa.Integer(), sa.ForeignKey("platform_accounts.id"), nullable=True),
            sa.Column("scope_type", sa.String(length=80), nullable=False),
            sa.Column("scope_id", sa.String(length=255), nullable=False),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Active"),
            sa.Column("hour_bucket", sa.String(length=40), nullable=True),
            sa.Column("hour_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("day_bucket", sa.String(length=40), nullable=True),
            sa.Column("day_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_sent_at", sa.String(length=40), nullable=True),
            sa.Column("last_content_hash", sa.String(length=64), nullable=True),
            sa.Column("next_allowed_at", sa.String(length=40), nullable=True),
            sa.Column("consecutive_errors", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("platform", "account_id", "scope_type", "scope_id", name="uq_platform_rate_limit_scope"),
        )
    existing = _indexes("platform_rate_limit_state")
    for column in ["platform", "account_id", "scope_type", "scope_id", "status", "next_allowed_at"]:
        name = f"ix_platform_rate_limit_state_{column}"
        if name not in existing:
            op.create_index(name, "platform_rate_limit_state", [column])


def downgrade() -> None:
    op.drop_table("platform_rate_limit_state")
    op.drop_table("platform_webhook_events")
