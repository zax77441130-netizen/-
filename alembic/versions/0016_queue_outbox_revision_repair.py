"""queue outbox revision repair

Revision ID: 0016_queue_outbox_revision_repair
Revises: 0015_flow_redis_storage
Create Date: 2026-10-05 13:20:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0016_queue_outbox_rev_repair"
down_revision = "0015_flow_redis_storage"
branch_labels = None
depends_on = None


def _table_exists(table: str) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return bind.exec_driver_sql("select name from sqlite_master where type='table' and name=?", (table,)).fetchone() is not None
    return table in sa.inspect(bind).get_table_names()


def _column_exists(table: str, column: str) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return column in {row[1] for row in bind.exec_driver_sql(f"PRAGMA table_info({table})")}
    return column in {col["name"] for col in sa.inspect(bind).get_columns(table)}


def _index_exists(table: str, name: str) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return name in {row[1] for row in bind.exec_driver_sql(f"PRAGMA index_list({table})")}
    return name in {idx["name"] for idx in sa.inspect(bind).get_indexes(table)}


def _constraint_exists(table: str, name: str) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return False
    return name in {constraint["name"] for constraint in sa.inspect(bind).get_unique_constraints(table)}


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists("queue_tasks"):
        op.create_table(
            "queue_tasks",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("queue_name", sa.String(length=120), nullable=False),
            sa.Column("task_type", sa.String(length=120), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("idempotency_key", sa.String(length=255), nullable=True),
            sa.Column("status", sa.String(length=40), nullable=False),
            sa.Column("scheduled_at", sa.String(length=40), nullable=True),
            sa.Column("locked_at", sa.String(length=40), nullable=True),
            sa.Column("locked_by", sa.String(length=160), nullable=True),
            sa.Column("lease_expires_at", sa.String(length=40), nullable=True),
            sa.Column("fencing_token", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("delivery_revision", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("redis_stream_id", sa.String(length=120), nullable=True),
            sa.Column("redis_claimed_at", sa.String(length=40), nullable=True),
            sa.Column("completed_at", sa.String(length=40), nullable=True),
            sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("idempotency_key", name="uq_queue_tasks_idempotency_key"),
        )
    elif not _column_exists("queue_tasks", "delivery_revision"):
        op.add_column("queue_tasks", sa.Column("delivery_revision", sa.Integer(), nullable=False, server_default="1"))

    if not _table_exists("queue_outbox"):
        op.create_table(
            "queue_outbox",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("queue_task_id", sa.Integer(), nullable=False),
            sa.Column("event_type", sa.String(length=80), nullable=False),
            sa.Column("delivery_revision", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("stream_name", sa.String(length=160), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("status", sa.String(length=40), nullable=False),
            sa.Column("published_at", sa.String(length=40), nullable=True),
            sa.Column("redis_stream_id", sa.String(length=120), nullable=True),
            sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["queue_task_id"], ["queue_tasks.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("queue_task_id", "event_type", "delivery_revision", name="uq_queue_outbox_task_event_revision"),
        )
    elif not _column_exists("queue_outbox", "delivery_revision"):
        op.add_column("queue_outbox", sa.Column("delivery_revision", sa.Integer(), nullable=False, server_default="1"))

    bind.exec_driver_sql("update queue_tasks set delivery_revision = 1 where delivery_revision is null")
    if _table_exists("queue_outbox"):
        bind.exec_driver_sql("update queue_outbox set delivery_revision = 1 where delivery_revision is null")

    if bind.dialect.name != "sqlite" and _table_exists("queue_outbox"):
        for old_name in ("uq_queue_outbox_task_event", "uq_queue_outbox_queue_task_id_event_type"):
            if _constraint_exists("queue_outbox", old_name):
                op.drop_constraint(old_name, "queue_outbox", type_="unique")
        if not _constraint_exists("queue_outbox", "uq_queue_outbox_task_event_revision"):
            op.create_unique_constraint(
                "uq_queue_outbox_task_event_revision",
                "queue_outbox",
                ["queue_task_id", "event_type", "delivery_revision"],
            )

    for name, columns in {
        "ix_queue_outbox_queue_task_id": ["queue_task_id"],
        "ix_queue_outbox_status": ["status"],
        "ix_queue_outbox_stream_name": ["stream_name"],
    }.items():
        if _table_exists("queue_outbox") and not _index_exists("queue_outbox", name):
            op.create_index(name, "queue_outbox", columns)


def downgrade() -> None:
    if _constraint_exists("queue_outbox", "uq_queue_outbox_task_event_revision"):
        op.drop_constraint("uq_queue_outbox_task_event_revision", "queue_outbox", type_="unique")
