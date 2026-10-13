"""repair queue schema after legacy 0017 markings

Revision ID: 0018_repair_queue_schema
Revises: 0017_queue_retention_fk
Create Date: 2026-10-06 09:45:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0018_repair_queue_schema"
down_revision = "0017_queue_retention_fk"
branch_labels = None
depends_on = None


SQLITE_NAMING_CONVENTION = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
}
SQLITE_SYSTEM_INDEX_PREFIXES = ("sqlite_autoindex_",)


def _bind():
    return op.get_bind()


def _table_exists(table: str) -> bool:
    bind = _bind()
    if bind.dialect.name == "sqlite":
        return bind.exec_driver_sql(
            "select name from sqlite_master where type='table' and name=:table",
            {"table": table},
        ).fetchone() is not None
    return sa.inspect(bind).has_table(table)


def _column_exists(table: str, column: str) -> bool:
    bind = _bind()
    if bind.dialect.name == "sqlite":
        return column in {row[1] for row in bind.exec_driver_sql(f"PRAGMA table_info({table})")}
    return column in {col["name"] for col in sa.inspect(bind).get_columns(table)}


def _index_exists(table: str, name: str) -> bool:
    bind = _bind()
    if bind.dialect.name == "sqlite":
        return name in {row[1] for row in bind.exec_driver_sql(f"PRAGMA index_list({table})")}
    return name in {idx["name"] for idx in sa.inspect(bind).get_indexes(table)}


def _unique_columns(table: str) -> set[tuple[str, ...]]:
    bind = _bind()
    if bind.dialect.name == "sqlite":
        uniques: set[tuple[str, ...]] = set()
        for row in bind.exec_driver_sql(f"PRAGMA index_list({table})"):
            if row[2]:
                uniques.add(tuple(info[2] for info in bind.exec_driver_sql(f"PRAGMA index_info({row[1]})")))
        return uniques
    return {tuple(item["column_names"]) for item in sa.inspect(bind).get_unique_constraints(table)}


def _fk_rows(table: str) -> list[dict]:
    bind = _bind()
    if bind.dialect.name == "sqlite":
        return [
            {
                "name": f"fk_{table}_{row[3]}_{row[2]}",
                "referred_table": row[2],
                "constrained_columns": [row[3]],
                "referred_columns": [row[4]],
                "ondelete": (row[6] or "").upper(),
            }
            for row in bind.exec_driver_sql(f"PRAGMA foreign_key_list({table})")
        ]
    return [
        {
            "name": fk.get("name"),
            "referred_table": fk.get("referred_table"),
            "constrained_columns": fk.get("constrained_columns") or [],
            "referred_columns": fk.get("referred_columns") or ["id"],
            "ondelete": ((fk.get("options") or {}).get("ondelete") or "").upper(),
        }
        for fk in sa.inspect(bind).get_foreign_keys(table)
    ]


def _has_fk(table: str, column: str, referred_table: str, ondelete: str) -> bool:
    return any(
        fk["referred_table"] == referred_table
        and fk["constrained_columns"] == [column]
        and fk["ondelete"] == ondelete.upper()
        for fk in _fk_rows(table)
    )


def _rebuild_queue_outbox_sqlite() -> None:
    bind = _bind()
    op.create_table(
        "queue_outbox_repaired",
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
        f"insert into queue_outbox_repaired ({', '.join(columns)}) "
        f"select {', '.join(columns)} from queue_outbox"
    )
    op.drop_table("queue_outbox")
    op.rename_table("queue_outbox_repaired", "queue_outbox")


def _drop_matching_fks(batch, table: str, column: str, referred_table: str) -> None:
    seen: set[str] = set()
    for fk in _fk_rows(table):
        if fk["referred_table"] == referred_table and fk["constrained_columns"] == [column]:
            name = fk["name"] or f"fk_{table}_{column}_{referred_table}"
            if name not in seen:
                batch.drop_constraint(name, type_="foreignkey")
                seen.add(name)


def _repair_set_null_fk(table: str, column: str, referred_table: str = "queue_tasks") -> None:
    if not _table_exists(table) or not _column_exists(table, column):
        return
    fks = [
        fk
        for fk in _fk_rows(table)
        if fk["referred_table"] == referred_table and fk["constrained_columns"] == [column]
    ]
    if len(fks) == 1 and fks[0]["ondelete"] == "SET NULL":
        return
    if _bind().dialect.name == "sqlite":
        _sqlite_rebuild_table_with_single_set_null_fk(table, column, referred_table)
        return
    kwargs = {"recreate": "always", "naming_convention": SQLITE_NAMING_CONVENTION} if _bind().dialect.name == "sqlite" else {}
    with op.batch_alter_table(table, **kwargs) as batch:
        _drop_matching_fks(batch, table, column, referred_table)
        batch.create_foreign_key(
            f"fk_{table}_{column}_{referred_table}_set_null",
            referred_table,
            [column],
            ["id"],
            ondelete="SET NULL",
        )


def _copy_column(column: sa.Column) -> sa.Column:
    return column.copy()


def _constraint_columns(constraint) -> list[str]:
    return [col.name for col in constraint.columns]


def _fk_referred_table_name(fk_constraint: sa.ForeignKeyConstraint) -> str | None:
    elements = list(fk_constraint.elements)
    if not elements:
        return None
    return elements[0].column.table.name


def _fk_referred_columns(fk_constraint: sa.ForeignKeyConstraint) -> list[str]:
    return [element.column.name for element in fk_constraint.elements]


def _sqlite_rebuild_table_with_single_set_null_fk(table: str, column: str, referred_table: str) -> None:
    bind = _bind()
    metadata = sa.MetaData()
    old_table = sa.Table(table, metadata, autoload_with=bind)
    temp_name = f"{table}_fk_repair"
    temp_metadata = sa.MetaData()
    referenced_tables = {referred_table}
    for constraint in old_table.constraints:
        if isinstance(constraint, sa.ForeignKeyConstraint) and _fk_referred_table_name(constraint):
            referenced_tables.add(_fk_referred_table_name(constraint))
    for referenced in referenced_tables:
        if referenced != table and _table_exists(referenced):
            sa.Table(referenced, temp_metadata, autoload_with=bind)
    copied_columns = [_copy_column(col) for col in old_table.columns]
    constraints: list[sa.Constraint] = []

    for constraint in old_table.constraints:
        if isinstance(constraint, sa.PrimaryKeyConstraint):
            constraints.append(sa.PrimaryKeyConstraint(*_constraint_columns(constraint), name=constraint.name))
        elif isinstance(constraint, sa.UniqueConstraint):
            constraints.append(sa.UniqueConstraint(*_constraint_columns(constraint), name=constraint.name))
        elif isinstance(constraint, sa.ForeignKeyConstraint):
            local_columns = _constraint_columns(constraint)
            if local_columns == [column] and _fk_referred_table_name(constraint) == referred_table:
                continue
            constraints.append(
                sa.ForeignKeyConstraint(
                    local_columns,
                    [f"{_fk_referred_table_name(constraint)}.{remote}" for remote in _fk_referred_columns(constraint)],
                    name=constraint.name,
                    ondelete=constraint.ondelete,
                    onupdate=constraint.onupdate,
                )
            )
        elif isinstance(constraint, sa.CheckConstraint):
            constraints.append(sa.CheckConstraint(constraint.sqltext, name=constraint.name))

    constraints.append(
        sa.ForeignKeyConstraint(
            [column],
            [f"{referred_table}.id"],
            name=f"fk_{table}_{column}_{referred_table}_set_null",
            ondelete="SET NULL",
        )
    )
    new_table = sa.Table(temp_name, temp_metadata, *copied_columns, *constraints)
    indexes = [
        index
        for index in sa.inspect(bind).get_indexes(table)
        if index.get("name") and not index["name"].startswith(SQLITE_SYSTEM_INDEX_PREFIXES)
    ]
    new_table.create(bind)
    quoted_columns = [col.name for col in old_table.columns]
    column_list = ", ".join(quoted_columns)
    bind.exec_driver_sql(f"insert into {temp_name} ({column_list}) select {column_list} from {table}")
    op.drop_table(table)
    op.rename_table(temp_name, table)

    for index in indexes:
        name = index.get("name")
        op.create_index(name, table, index.get("column_names") or [], unique=bool(index.get("unique")))


def upgrade() -> None:
    bind = _bind()
    if not _table_exists("queue_tasks"):
        return
    if _table_exists("queue_outbox"):
        unique_ok = ("queue_task_id", "event_type", "delivery_revision") in _unique_columns("queue_outbox")
        stale_unique = ("queue_task_id", "event_type") in _unique_columns("queue_outbox")
        cascade_ok = _has_fk("queue_outbox", "queue_task_id", "queue_tasks", "CASCADE")
        if bind.dialect.name == "sqlite":
            if not unique_ok or stale_unique or not cascade_ok:
                _rebuild_queue_outbox_sqlite()
        else:
            with op.batch_alter_table("queue_outbox") as batch:
                inspector = sa.inspect(bind)
                for uq in inspector.get_unique_constraints("queue_outbox"):
                    if tuple(uq.get("column_names") or ()) == ("queue_task_id", "event_type"):
                        batch.drop_constraint(uq["name"], type_="unique")
                if not unique_ok:
                    batch.create_unique_constraint(
                        "uq_queue_outbox_task_event_revision",
                        ["queue_task_id", "event_type", "delivery_revision"],
                    )
                if not cascade_ok:
                    _drop_matching_fks(batch, "queue_outbox", "queue_task_id", "queue_tasks")
                    batch.create_foreign_key(
                        "fk_queue_outbox_queue_task_id_queue_tasks_cascade",
                        "queue_tasks",
                        ["queue_task_id"],
                        ["id"],
                        ondelete="CASCADE",
                    )
        for name, columns in {
            "ix_queue_outbox_queue_task_id": ["queue_task_id"],
            "ix_queue_outbox_status": ["status"],
            "ix_queue_outbox_stream_name": ["stream_name"],
        }.items():
            if not _index_exists("queue_outbox", name):
                op.create_index(name, "queue_outbox", columns)
    _repair_set_null_fk("external_action_logs", "queue_task_id")
    _repair_set_null_fk("platform_automation_runs", "queue_task_id")
    _repair_set_null_fk("phone_webhook_events", "fallback_task_id")


def downgrade() -> None:
    pass
