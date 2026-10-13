"""recruitment blacklist

Revision ID: 0006_recruitment_blacklist
Revises: 0005_email_cursor_uid_reliability
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0006_recruitment_blacklist"
down_revision = "0005_email_cursor_uid_rel"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    if "recruitment_blacklist" in set(inspector.get_table_names()):
        return
    op.create_table(
        "recruitment_blacklist",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=True),
        sa.Column("phone", sa.String(length=40), nullable=True),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("rule_code", sa.String(length=120), nullable=False, server_default="customer_rule"),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("evidence", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=40), nullable=False, server_default="Active"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    for column in ["candidate_id", "phone", "email", "status"]:
        op.create_index(f"ix_recruitment_blacklist_{column}", "recruitment_blacklist", [column])


def downgrade() -> None:
    op.drop_table("recruitment_blacklist")
