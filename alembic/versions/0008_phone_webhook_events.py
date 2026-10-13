"""persistent phone webhook replay records

Revision ID: 0008_phone_webhook_events
Revises: 0007_ai_provider_capabilities
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0008_phone_webhook_events"
down_revision = "0007_ai_provider_capabilities"
branch_labels = None
depends_on = None


def _indexes(table_name: str) -> set[str]:
    return {item["name"] for item in inspect(op.get_bind()).get_indexes(table_name)}


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "phone_webhook_events" not in tables:
        op.create_table(
            "phone_webhook_events",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("provider", sa.String(length=120), nullable=False, server_default="default"),
            sa.Column("event_id", sa.String(length=255), nullable=False),
            sa.Column("nonce", sa.String(length=255), nullable=False),
            sa.Column("external_call_id", sa.String(length=255), nullable=True),
            sa.Column("received_at", sa.String(length=40), nullable=False),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Accepted"),
            sa.Column("payload_hash", sa.String(length=64), nullable=False),
            sa.Column("fallback_task_id", sa.Integer(), sa.ForeignKey("queue_tasks.id"), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("provider", "nonce", name="uq_phone_webhook_provider_nonce"),
            sa.UniqueConstraint("provider", "event_id", name="uq_phone_webhook_provider_event_id"),
        )
    existing = _indexes("phone_webhook_events")
    for column in ["provider", "event_id", "nonce", "external_call_id", "received_at", "status"]:
        name = f"ix_phone_webhook_events_{column}"
        if name not in existing:
            op.create_index(name, "phone_webhook_events", [column])


def downgrade() -> None:
    op.drop_table("phone_webhook_events")
