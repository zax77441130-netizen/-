"""email cursor and uid reliability

Revision ID: 0005_email_cursor_uid_reliability
Revises: 0004_platform_identity_phone_states
Create Date: 2026-09-30
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0005_email_cursor_uid_rel"
down_revision = "0004_platform_identity_phone"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    columns = {column["name"] for column in inspector.get_columns("inbound_emails")} if "inbound_emails" in tables else set()
    if "mailbox" not in columns:
        op.add_column("inbound_emails", sa.Column("mailbox", sa.String(length=160), nullable=True))
        op.create_index("ix_inbound_emails_mailbox", "inbound_emails", ["mailbox"])
    if "imap_uid" not in columns:
        op.add_column("inbound_emails", sa.Column("imap_uid", sa.String(length=80), nullable=True))
        op.create_index("ix_inbound_emails_imap_uid", "inbound_emails", ["imap_uid"])
    if "uid_validity" not in columns:
        op.add_column("inbound_emails", sa.Column("uid_validity", sa.String(length=80), nullable=True))
        op.create_index("ix_inbound_emails_uid_validity", "inbound_emails", ["uid_validity"])

    if "email_cursors" not in tables:
        op.create_table(
            "email_cursors",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("account_id", sa.Integer(), sa.ForeignKey("platform_accounts.id"), nullable=True),
            sa.Column("mailbox", sa.String(length=160), nullable=False, server_default="INBOX"),
            sa.Column("uid_validity", sa.String(length=80), nullable=False, server_default="unknown"),
            sa.Column("last_imported_uid", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_ack_uid", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Active"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("account_id", "mailbox", "uid_validity", name="uq_email_cursor_scope"),
        )
        for column in ["account_id", "mailbox", "uid_validity"]:
            op.create_index(f"ix_email_cursors_{column}", "email_cursors", [column])


def downgrade() -> None:
    op.drop_table("email_cursors")
    for column in ["uid_validity", "imap_uid", "mailbox"]:
        op.drop_index(f"ix_inbound_emails_{column}", table_name="inbound_emails")
        op.drop_column("inbound_emails", column)
