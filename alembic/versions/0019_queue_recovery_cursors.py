"""queue recovery cursors

Revision ID: 0019_queue_recovery_cursors
Revises: 0018_repair_queue_schema
Create Date: 2026-10-06 10:35:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0019_queue_recovery_cursors"
down_revision = "0018_repair_queue_schema"
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
    if _table_exists("queue_recovery_cursors"):
        return
    op.create_table(
        "queue_recovery_cursors",
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("cursor_id", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("wrapped_at", sa.String(length=40), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("name"),
    )


def downgrade() -> None:
    op.drop_table("queue_recovery_cursors")
