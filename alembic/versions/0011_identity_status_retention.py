"""identity status retention

Revision ID: 0011_identity_status_retention
Revises: 0010_interview_timezone
Create Date: 2026-10-02
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0011_identity_status_retention"
down_revision = "0010_interview_timezone"
branch_labels = None
depends_on = None


def _columns(table: str) -> set[str]:
    return {column["name"] for column in inspect(op.get_bind()).get_columns(table)}


def _indexes(table: str) -> set[str]:
    return {index["name"] for index in inspect(op.get_bind()).get_indexes(table)}


def upgrade() -> None:
    application_columns = _columns("applications")
    with op.batch_alter_table("applications") as batch:
        if "decision_source" not in application_columns:
            batch.add_column(sa.Column("decision_source", sa.String(length=40), nullable=False, server_default="manual"))
        if "decision_event_key" not in application_columns:
            batch.add_column(sa.Column("decision_event_key", sa.String(length=255), nullable=True))
    app_indexes = _indexes("applications")
    if "ix_applications_decision_source" not in app_indexes:
        op.create_index("ix_applications_decision_source", "applications", ["decision_source"])
    if "ix_applications_decision_event_key" not in app_indexes:
        op.create_index("ix_applications_decision_event_key", "applications", ["decision_event_key"])

    history_columns = _columns("application_status_history")
    with op.batch_alter_table("application_status_history") as batch:
        if "source" not in history_columns:
            batch.add_column(sa.Column("source", sa.String(length=40), nullable=False, server_default="manual"))
        if "event_key" not in history_columns:
            batch.add_column(sa.Column("event_key", sa.String(length=255), nullable=True))
    history_indexes = _indexes("application_status_history")
    if "ix_application_status_history_source" not in history_indexes:
        op.create_index("ix_application_status_history_source", "application_status_history", ["source"])
    if "ix_application_status_history_event_key" not in history_indexes:
        op.create_index("ix_application_status_history_event_key", "application_status_history", ["event_key"], unique=True)

    tables = inspect(op.get_bind()).get_table_names()
    if "data_retention_runs" not in tables:
        op.create_table(
            "data_retention_runs",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("policy_name", sa.String(length=120), nullable=False),
            sa.Column("target_table", sa.String(length=120), nullable=False),
            sa.Column("strategy", sa.String(length=40), nullable=False, server_default="delete"),
            sa.Column("cutoff_at", sa.String(length=40), nullable=False),
            sa.Column("rows_scanned", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("rows_changed", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Completed"),
            sa.Column("details", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index("ix_data_retention_runs_policy_name", "data_retention_runs", ["policy_name"])
        op.create_index("ix_data_retention_runs_target_table", "data_retention_runs", ["target_table"])
        op.create_index("ix_data_retention_runs_cutoff_at", "data_retention_runs", ["cutoff_at"])
        op.create_index("ix_data_retention_runs_status", "data_retention_runs", ["status"])


def downgrade() -> None:
    tables = inspect(op.get_bind()).get_table_names()
    if "data_retention_runs" in tables:
        op.drop_table("data_retention_runs")
    columns = _columns("application_status_history")
    with op.batch_alter_table("application_status_history") as batch:
        if "event_key" in columns:
            batch.drop_column("event_key")
        if "source" in columns:
            batch.drop_column("source")
    columns = _columns("applications")
    with op.batch_alter_table("applications") as batch:
        if "decision_event_key" in columns:
            batch.drop_column("decision_event_key")
        if "decision_source" in columns:
            batch.drop_column("decision_source")
