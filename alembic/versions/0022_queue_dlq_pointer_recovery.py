"""queue dlq pointer recovery indexes

Revision ID: 0022_queue_dlq_pointer_recovery
Revises: 0021_queue_dlq_recovery_state
Create Date: 2026-10-08 05:55:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0022_queue_dlq_pointer_recovery"
down_revision = "0021_queue_dlq_recovery_state"
branch_labels = None
depends_on = None


def _create_index_if_missing(name: str, table: str, columns: list[str]) -> None:
    bind = op.get_bind()
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes(table)}
    if name not in indexes:
        op.create_index(name, table, columns)


def _drop_index_if_exists(name: str, table: str) -> None:
    bind = op.get_bind()
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes(table)}
    if name in indexes:
        op.drop_index(name, table_name=table)


def upgrade() -> None:
    _create_index_if_missing("ix_queue_dlq_records_dlq_stream_id", "queue_dlq_records", ["dlq_stream_id"])
    _create_index_if_missing("ix_queue_dlq_records_status_created_at", "queue_dlq_records", ["status", "created_at"])


def downgrade() -> None:
    _drop_index_if_exists("ix_queue_dlq_records_status_created_at", "queue_dlq_records")
    _drop_index_if_exists("ix_queue_dlq_records_dlq_stream_id", "queue_dlq_records")
