"""queue outbox constraints and retention foreign keys

Revision ID: 0017_queue_retention_fk
Revises: 0016_queue_outbox_revision_repair
Create Date: 2026-10-05 13:40:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0017_queue_retention_fk"
down_revision = "0016_queue_outbox_rev_repair"
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


def _unique_exists(table: str, name: str) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return name in {row[1] for row in bind.exec_driver_sql(f"PRAGMA index_list({table})") if row[2]}
    return name in {item["name"] for item in sa.inspect(bind).get_unique_constraints(table)}


def _sqlite_unique_columns(table: str) -> set[tuple[str, ...]]:
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        return set()
    uniques: set[tuple[str, ...]] = set()
    for row in bind.exec_driver_sql(f"PRAGMA index_list({table})"):
        if not row[2]:
            continue
        columns = tuple(info[2] for info in bind.exec_driver_sql(f"PRAGMA index_info({row[1]})"))
        uniques.add(columns)
    return uniques


def _fk_exists(table: str, referred_table: str, constrained_columns: tuple[str, ...], ondelete: str | None) -> bool:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        for row in bind.exec_driver_sql(f"PRAGMA foreign_key_list({table})"):
            if row[2] == referred_table and (row[3],) == constrained_columns and (row[6] or "").upper() == (ondelete or "").upper():
                return True
        return False
    for fk in sa.inspect(bind).get_foreign_keys(table):
        if fk.get("referred_table") == referred_table and tuple(fk.get("constrained_columns") or ()) == constrained_columns:
            return (fk.get("options") or {}).get("ondelete", "").upper() == (ondelete or "").upper()
    return False


def _rebuild_queue_outbox_sqlite() -> None:
    bind = op.get_bind()
    if not _table_exists("queue_outbox"):
        return
    bind.exec_driver_sql("PRAGMA foreign_keys=OFF")
    op.create_table(
        "queue_outbox_new",
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
    columns = [
        "id",
        "queue_task_id",
        "event_type",
        "delivery_revision",
        "stream_name",
        "payload",
        "status",
        "published_at",
        "redis_stream_id",
        "retry_count",
        "last_error",
        "created_at",
        "updated_at",
    ]
    bind.exec_driver_sql(
        f"insert into queue_outbox_new ({', '.join(columns)}) "
        f"select {', '.join(columns)} from queue_outbox"
    )
    op.drop_table("queue_outbox")
    op.rename_table("queue_outbox_new", "queue_outbox")
    bind.exec_driver_sql("PRAGMA foreign_keys=ON")


def _set_null_fk(table: str, column: str, referred_table: str = "queue_tasks") -> None:
    if not _table_exists(table) or not _column_exists(table, column):
        return
    if op.get_bind().dialect.name == "sqlite":
        # SQLite unnamed/duplicated FK cleanup is handled by the follow-up
        # repair migration, which rebuilds the table from reflected schema.
        return
    with op.batch_alter_table(table) as batch:
        for fk in sa.inspect(op.get_bind()).get_foreign_keys(table):
            if fk.get("referred_table") == referred_table and column in (fk.get("constrained_columns") or []):
                batch.drop_constraint(fk["name"], type_="foreignkey")
        batch.create_foreign_key(f"fk_{table}_{column}_{referred_table}_set_null", referred_table, [column], ["id"], ondelete="SET NULL")


def upgrade() -> None:
    bind = op.get_bind()
    if not _table_exists("queue_tasks"):
        return
    if _table_exists("queue_outbox"):
        if bind.dialect.name == "sqlite":
            uniques = _sqlite_unique_columns("queue_outbox")
            if (
                ("queue_task_id", "event_type", "delivery_revision") not in uniques
                or ("queue_task_id", "event_type") in uniques
                or not _fk_exists("queue_outbox", "queue_tasks", ("queue_task_id",), "CASCADE")
            ):
                _rebuild_queue_outbox_sqlite()
        else:
            for old_name in ("uq_queue_outbox_task_event", "uq_queue_outbox_queue_task_id_event_type"):
                if _unique_exists("queue_outbox", old_name):
                    op.drop_constraint(old_name, "queue_outbox", type_="unique")
            if not _unique_exists("queue_outbox", "uq_queue_outbox_task_event_revision"):
                op.create_unique_constraint("uq_queue_outbox_task_event_revision", "queue_outbox", ["queue_task_id", "event_type", "delivery_revision"])
            if not _fk_exists("queue_outbox", "queue_tasks", ("queue_task_id",), "CASCADE"):
                with op.batch_alter_table("queue_outbox") as batch:
                    for fk in sa.inspect(bind).get_foreign_keys("queue_outbox"):
                        if fk.get("referred_table") == "queue_tasks" and "queue_task_id" in (fk.get("constrained_columns") or []):
                            batch.drop_constraint(fk["name"], type_="foreignkey")
                    batch.create_foreign_key("fk_queue_outbox_queue_task_id_queue_tasks_cascade", "queue_tasks", ["queue_task_id"], ["id"], ondelete="CASCADE")
        for name, columns in {
            "ix_queue_outbox_queue_task_id": ["queue_task_id"],
            "ix_queue_outbox_status": ["status"],
            "ix_queue_outbox_stream_name": ["stream_name"],
        }.items():
            if not _index_exists("queue_outbox", name):
                op.create_index(name, "queue_outbox", columns)
    _set_null_fk("external_action_logs", "queue_task_id")
    _set_null_fk("platform_automation_runs", "queue_task_id")
    _set_null_fk("phone_webhook_events", "fallback_task_id")


def downgrade() -> None:
    pass
