"""interview timezone

Revision ID: 0010_interview_timezone
Revises: 0009_platform_webhooks_and_rate_limits
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0010_interview_timezone"
down_revision = "0009_platform_webhooks_rate"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("interviews")}
    if "timezone" not in columns:
        with op.batch_alter_table("interviews") as batch:
            batch.add_column(sa.Column("timezone", sa.String(length=80), nullable=False, server_default="Asia/Taipei"))


def downgrade() -> None:
    columns = {column["name"] for column in inspect(op.get_bind()).get_columns("interviews")}
    if "timezone" in columns:
        with op.batch_alter_table("interviews") as batch:
            batch.drop_column("timezone")
