"""document purge queue safety

Revision ID: 0013_document_purge_queue_safety
Revises: 0012_calendar_ocr_purge
Create Date: 2026-10-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0013_document_purge_queue_safety"
down_revision = "0012_calendar_ocr_purge"
branch_labels = None
depends_on = None


def _constraint_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {item["name"] for item in inspector.get_unique_constraints(table_name) if item.get("name")}


def upgrade() -> None:
    if "uq_stored_documents_sha256" in _constraint_names("stored_documents"):
        with op.batch_alter_table("stored_documents") as batch:
            batch.drop_constraint("uq_stored_documents_sha256", type_="unique")


def downgrade() -> None:
    with op.batch_alter_table("stored_documents") as batch:
        batch.create_unique_constraint("uq_stored_documents_sha256", ["sha256"])
