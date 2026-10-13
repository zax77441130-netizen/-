"""queue dlq recovery state

Revision ID: 0021_queue_dlq_recovery_state
Revises: 0020_queue_dlq_records
Create Date: 2026-10-06 16:45:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0021_queue_dlq_recovery_state"
down_revision = "0020_queue_dlq_records"
branch_labels = None
depends_on = None


def _columns(table: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table)}


def _add_column_if_missing(table: str, column: sa.Column) -> None:
    if column.name not in _columns(table):
        op.add_column(table, column)


def _create_index_if_missing(name: str, table: str, columns: list[str]) -> None:
    bind = op.get_bind()
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes(table)}
    if name not in indexes:
        op.create_index(name, table, columns)


def upgrade() -> None:
    _add_column_if_missing("queue_dlq_records", sa.Column("db_write_status", sa.String(length=40), nullable=False, server_default="Saved"))
    _add_column_if_missing("queue_dlq_records", sa.Column("redis_dlq_status", sa.String(length=40), nullable=False, server_default="Unknown"))
    _add_column_if_missing("queue_dlq_records", sa.Column("ack_status", sa.String(length=40), nullable=False, server_default="Unknown"))
    _add_column_if_missing("queue_dlq_records", sa.Column("last_error", sa.Text(), nullable=True))
    _add_column_if_missing("queue_dlq_records", sa.Column("replay_started_at", sa.String(length=40), nullable=True))
    _create_index_if_missing("ix_queue_dlq_records_db_write_status", "queue_dlq_records", ["db_write_status"])
    _create_index_if_missing("ix_queue_dlq_records_redis_dlq_status", "queue_dlq_records", ["redis_dlq_status"])
    _create_index_if_missing("ix_queue_dlq_records_ack_status", "queue_dlq_records", ["ack_status"])


def downgrade() -> None:
    for column in ["replay_started_at", "last_error", "ack_status", "redis_dlq_status", "db_write_status"]:
        if column in _columns("queue_dlq_records"):
            op.drop_column("queue_dlq_records", column)
