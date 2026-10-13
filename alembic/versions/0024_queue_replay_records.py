"""queue replay records

Revision ID: 0024_queue_replay_records
Revises: 0023_blacklist_replay_guards
Create Date: 2026-10-08 12:05:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0024_queue_replay_records"
down_revision = "0023_blacklist_replay_guards"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in sa.inspect(op.get_bind()).get_table_names()


def _indexes(table: str) -> set[str]:
    return {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(table)}


def upgrade() -> None:
    if not _has_table("queue_replay_records"):
        op.create_table(
            "queue_replay_records",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("replay_request_key", sa.String(length=255), nullable=False),
            sa.Column("queue_task_id", sa.Integer(), nullable=False),
            sa.Column("dlq_record_id", sa.Integer(), nullable=True),
            sa.Column("expected_delivery_revision", sa.Integer(), nullable=True),
            sa.Column("replayed_delivery_revision", sa.Integer(), nullable=True),
            sa.Column("replayed_outbox_id", sa.Integer(), nullable=True),
            sa.Column("status", sa.String(length=40), nullable=False),
            sa.Column("failure_reason", sa.Text(), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["dlq_record_id"], ["queue_dlq_records.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["queue_task_id"], ["queue_tasks.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["replayed_outbox_id"], ["queue_outbox.id"], ondelete="SET NULL"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("replay_request_key", name="uq_queue_replay_request_key"),
        )
    existing = _indexes("queue_replay_records")
    for name, columns in {
        "ix_queue_replay_records_replay_request_key": ["replay_request_key"],
        "ix_queue_replay_records_queue_task_id": ["queue_task_id"],
        "ix_queue_replay_records_dlq_record_id": ["dlq_record_id"],
        "ix_queue_replay_records_status": ["status"],
    }.items():
        if name not in existing:
            op.create_index(name, "queue_replay_records", columns)


def downgrade() -> None:
    if _has_table("queue_replay_records"):
        op.drop_table("queue_replay_records")
