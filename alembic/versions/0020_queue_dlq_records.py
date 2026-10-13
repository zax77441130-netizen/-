"""queue dlq records

Revision ID: 0020_queue_dlq_records
Revises: 0019_queue_recovery_cursors
Create Date: 2026-10-06 11:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0020_queue_dlq_records"
down_revision = "0019_queue_recovery_cursors"
branch_labels = None
depends_on = None


def _table_exists(table: str) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return bind.exec_driver_sql(
            "select name from sqlite_master where type='table' and name=:table",
            {"table": table},
        ).fetchone() is not None
    return sa.inspect(bind).has_table(table)


def upgrade() -> None:
    if _table_exists("queue_dlq_records"):
        return
    op.create_table(
        "queue_dlq_records",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("stream_name", sa.String(length=160), nullable=False),
        sa.Column("redis_stream_id", sa.String(length=120), nullable=False),
        sa.Column("dlq_stream_id", sa.String(length=120), nullable=True),
        sa.Column("queue_task_id", sa.Integer(), nullable=True),
        sa.Column("outbox_id", sa.Integer(), nullable=True),
        sa.Column("delivery_revision", sa.Integer(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False, server_default="Open"),
        sa.Column("replayed_task_id", sa.Integer(), nullable=True),
        sa.Column("resolved_at", sa.String(length=40), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["outbox_id"], ["queue_outbox.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["queue_task_id"], ["queue_tasks.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["replayed_task_id"], ["queue_tasks.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stream_name", "redis_stream_id", name="uq_queue_dlq_stream_message"),
    )
    op.create_index("ix_queue_dlq_records_stream_name", "queue_dlq_records", ["stream_name"])
    op.create_index("ix_queue_dlq_records_redis_stream_id", "queue_dlq_records", ["redis_stream_id"])
    op.create_index("ix_queue_dlq_records_queue_task_id", "queue_dlq_records", ["queue_task_id"])
    op.create_index("ix_queue_dlq_records_outbox_id", "queue_dlq_records", ["outbox_id"])
    op.create_index("ix_queue_dlq_records_status", "queue_dlq_records", ["status"])


def downgrade() -> None:
    op.drop_table("queue_dlq_records")
