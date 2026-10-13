"""scope confirmation, event log, webhook hashes

Revision ID: 0026_scope_event_repairs
Revises: 0025_blacklist_scope_repair
Create Date: 2026-10-08 13:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0026_scope_event_repairs"
down_revision = "0025_blacklist_scope_repair"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in sa.inspect(op.get_bind()).get_table_names()


def _columns(table: str) -> set[str]:
    return {column["name"] for column in sa.inspect(op.get_bind()).get_columns(table)}


def _indexes(table: str) -> set[str]:
    return {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(table)}


def upgrade() -> None:
    if _has_table("platform_webhook_events"):
        columns = _columns("platform_webhook_events")
        if "canonical_event_hash" not in columns:
            op.add_column("platform_webhook_events", sa.Column("canonical_event_hash", sa.String(length=64), nullable=True))
        if "raw_body_b64" not in columns:
            op.add_column("platform_webhook_events", sa.Column("raw_body_b64", sa.Text(), nullable=True))
        indexes = _indexes("platform_webhook_events")
        if "ix_platform_webhook_events_canonical_event_hash" not in indexes:
            op.create_index("ix_platform_webhook_events_canonical_event_hash", "platform_webhook_events", ["canonical_event_hash"])

    if not _has_table("business_events"):
        op.create_table(
            "business_events",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("event_key", sa.String(length=255), nullable=False),
            sa.Column("entity_type", sa.String(length=120), nullable=False),
            sa.Column("entity_id", sa.String(length=120), nullable=False),
            sa.Column("from_state", sa.String(length=80), nullable=True),
            sa.Column("to_state", sa.String(length=80), nullable=False),
            sa.Column("status", sa.String(length=40), nullable=False),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("evidence", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("event_key", name="uq_business_event_key"),
        )
    if _has_table("business_events"):
        indexes = _indexes("business_events")
        for name, columns in {
            "ix_business_events_event_key": ["event_key"],
            "ix_business_events_entity_type": ["entity_type"],
            "ix_business_events_entity_id": ["entity_id"],
            "ix_business_events_to_state": ["to_state"],
            "ix_business_events_status": ["status"],
        }.items():
            if name not in indexes:
                op.create_index(name, "business_events", columns)

    if _has_table("recruitment_blacklist"):
        bind = op.get_bind()
        bind.execute(
            sa.text(
                """
                update recruitment_blacklist
                set scope_type='global', scope_id=null
                where scope_type='job'
                  and (
                    lower(coalesce(rule_code, '')) like '%global%'
                    or lower(coalesce(reason, '')) like '%explicit global%'
                    or lower(coalesce(evidence, '')) like '%explicit global%'
                    or lower(coalesce(evidence, '')) like '%scope:global%'
                    or lower(coalesce(evidence, '')) like '%"scope_type":"global"%'
                    or lower(coalesce(evidence, '')) like '%"scope_type": "global"%'
                  )
                """
            )
        )


def downgrade() -> None:
    if _has_table("business_events"):
        op.drop_table("business_events")
    if _has_table("platform_webhook_events"):
        indexes = _indexes("platform_webhook_events")
        if "ix_platform_webhook_events_canonical_event_hash" in indexes:
            op.drop_index("ix_platform_webhook_events_canonical_event_hash", table_name="platform_webhook_events")
        columns = _columns("platform_webhook_events")
        if "raw_body_b64" in columns:
            op.drop_column("platform_webhook_events", "raw_body_b64")
        if "canonical_event_hash" in columns:
            op.drop_column("platform_webhook_events", "canonical_event_hash")
