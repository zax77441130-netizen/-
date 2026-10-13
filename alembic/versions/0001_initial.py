"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-09-25
"""

from alembic import op

from backend.app.core.db import Base
from backend.app.models import domain  # noqa: F401


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
