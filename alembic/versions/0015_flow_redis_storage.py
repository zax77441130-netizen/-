"""flow redis storage

Revision ID: 0015_flow_redis_storage
Revises: 0014_interview_attendance
Create Date: 2026-10-04 22:40:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0015_flow_redis_storage"
down_revision = "0014_interview_attendance"
branch_labels = None
depends_on = None


def _add_column_if_missing(table: str, column: sa.Column) -> None:
    bind = op.get_bind()
    existing = {row[1] for row in bind.exec_driver_sql(f"PRAGMA table_info({table})")} if bind.dialect.name == "sqlite" else set()
    if bind.dialect.name != "sqlite" or column.name not in existing:
        op.add_column(table, column)


def _create_index_if_missing(name: str, table: str, columns: list[str]) -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        existing = {row[1] for row in bind.exec_driver_sql(f"PRAGMA index_list({table})")}
        if name in existing:
            return
    op.create_index(name, table, columns, unique=False)


def _table_exists(table: str) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return bind.exec_driver_sql("select name from sqlite_master where type='table' and name=?", (table,)).fetchone() is not None
    inspector = sa.inspect(bind)
    return table in inspector.get_table_names()


def upgrade() -> None:
    _add_column_if_missing("queue_tasks", sa.Column("locked_by", sa.String(length=160), nullable=True))
    _add_column_if_missing("queue_tasks", sa.Column("lease_expires_at", sa.String(length=40), nullable=True))
    _add_column_if_missing("queue_tasks", sa.Column("fencing_token", sa.Integer(), nullable=False, server_default="0"))
    _add_column_if_missing("queue_tasks", sa.Column("delivery_revision", sa.Integer(), nullable=False, server_default="1"))
    _add_column_if_missing("queue_tasks", sa.Column("redis_stream_id", sa.String(length=120), nullable=True))
    _add_column_if_missing("queue_tasks", sa.Column("redis_claimed_at", sa.String(length=40), nullable=True))
    _create_index_if_missing("ix_queue_tasks_locked_by", "queue_tasks", ["locked_by"])
    _create_index_if_missing("ix_queue_tasks_lease_expires_at", "queue_tasks", ["lease_expires_at"])
    _create_index_if_missing("ix_queue_tasks_redis_stream_id", "queue_tasks", ["redis_stream_id"])

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
            sa.Column("retry_count", sa.Integer(), nullable=False),
            sa.Column("last_error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["queue_task_id"], ["queue_tasks.id"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("queue_task_id", "event_type", "delivery_revision", name="uq_queue_outbox_task_event_revision"),
        )
    else:
        _add_column_if_missing("queue_outbox", sa.Column("delivery_revision", sa.Integer(), nullable=False, server_default="1"))
    _create_index_if_missing("ix_queue_outbox_queue_task_id", "queue_outbox", ["queue_task_id"])
    _create_index_if_missing("ix_queue_outbox_status", "queue_outbox", ["status"])
    _create_index_if_missing("ix_queue_outbox_stream_name", "queue_outbox", ["stream_name"])

    if not _table_exists("business_flow_states"):
        op.create_table(
            "business_flow_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("application_id", sa.Integer(), nullable=True),
        sa.Column("conversation_id", sa.Integer(), nullable=True),
        sa.Column("flow_key", sa.String(length=120), nullable=False),
        sa.Column("stage", sa.String(length=80), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("rule_version", sa.String(length=80), nullable=False),
        sa.Column("config_version", sa.String(length=80), nullable=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("evidence", sa.JSON(), nullable=False),
        sa.Column("last_event_key", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["application_id"], ["applications.id"]),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidates.id"]),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"]),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("candidate_id", "job_id", "flow_key", name="uq_business_flow_scope"),
        )
    _create_index_if_missing("ix_business_flow_states_candidate_id", "business_flow_states", ["candidate_id"])
    _create_index_if_missing("ix_business_flow_states_job_id", "business_flow_states", ["job_id"])
    _create_index_if_missing("ix_business_flow_states_application_id", "business_flow_states", ["application_id"])
    _create_index_if_missing("ix_business_flow_states_conversation_id", "business_flow_states", ["conversation_id"])
    _create_index_if_missing("ix_business_flow_states_stage", "business_flow_states", ["stage"])
    _create_index_if_missing("ix_business_flow_states_status", "business_flow_states", ["status"])

    if not _table_exists("storage_objects"):
        op.create_table(
            "storage_objects",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("object_key", sa.String(length=500), nullable=False),
        sa.Column("version_id", sa.String(length=160), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("byte_size", sa.Integer(), nullable=False),
        sa.Column("storage_provider", sa.String(length=80), nullable=False),
        sa.Column("ref_count", sa.Integer(), nullable=False),
        sa.Column("legal_hold", sa.Boolean(), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("deleted_at", sa.String(length=40), nullable=True),
        sa.Column("evidence", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("object_key", "version_id", name="uq_storage_object_version"),
        )
    _create_index_if_missing("ix_storage_objects_object_key", "storage_objects", ["object_key"])
    _create_index_if_missing("ix_storage_objects_version_id", "storage_objects", ["version_id"])
    _create_index_if_missing("ix_storage_objects_sha256", "storage_objects", ["sha256"])
    _create_index_if_missing("ix_storage_objects_status", "storage_objects", ["status"])


def downgrade() -> None:
    op.drop_table("storage_objects")
    op.drop_table("business_flow_states")
    op.drop_table("queue_outbox")
    op.drop_index(op.f("ix_queue_tasks_redis_stream_id"), table_name="queue_tasks")
    op.drop_index(op.f("ix_queue_tasks_lease_expires_at"), table_name="queue_tasks")
    op.drop_index(op.f("ix_queue_tasks_locked_by"), table_name="queue_tasks")
    op.drop_column("queue_tasks", "redis_claimed_at")
    op.drop_column("queue_tasks", "redis_stream_id")
    op.drop_column("queue_tasks", "delivery_revision")
    op.drop_column("queue_tasks", "fencing_token")
    op.drop_column("queue_tasks", "lease_expires_at")
    op.drop_column("queue_tasks", "locked_by")
