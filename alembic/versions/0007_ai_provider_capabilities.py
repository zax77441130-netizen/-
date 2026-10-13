"""ai provider capabilities and circuit state

Revision ID: 0007_ai_provider_capabilities
Revises: 0006_recruitment_blacklist
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0007_ai_provider_capabilities"
down_revision = "0006_recruitment_blacklist"
branch_labels = None
depends_on = None


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    inspector = inspect(op.get_bind())
    existing = {item["name"] for item in inspector.get_columns(table_name)}
    if column.name not in existing:
        op.add_column(table_name, column)


def upgrade() -> None:
    _add_column_if_missing("ai_providers", sa.Column("supports_text", sa.Boolean(), nullable=False, server_default=sa.true()))
    _add_column_if_missing("ai_providers", sa.Column("supports_vision", sa.Boolean(), nullable=False, server_default=sa.false()))
    _add_column_if_missing("ai_providers", sa.Column("supports_json_schema", sa.Boolean(), nullable=False, server_default=sa.false()))
    _add_column_if_missing("ai_providers", sa.Column("supports_tools", sa.Boolean(), nullable=False, server_default=sa.false()))
    _add_column_if_missing("ai_providers", sa.Column("retry_policy", sa.JSON(), nullable=False, server_default="{}"))
    _add_column_if_missing("ai_providers", sa.Column("cost_policy", sa.JSON(), nullable=False, server_default="{}"))
    _add_column_if_missing("ai_providers", sa.Column("circuit_open_until", sa.String(length=40), nullable=True))
    _add_column_if_missing("ai_providers", sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    for column in [
        "consecutive_failures",
        "circuit_open_until",
        "cost_policy",
        "retry_policy",
        "supports_tools",
        "supports_json_schema",
        "supports_vision",
        "supports_text",
    ]:
        op.drop_column("ai_providers", column)
