"""flow workers and external evidence

Revision ID: 0003_flow_workers_and_evidence
Revises: 0002_external_automation
Create Date: 2026-09-27
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0003_flow_workers_and_evidence"
down_revision = "0002_external_automation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    if "candidate_question_answers" not in tables:
        op.create_table(
            "candidate_question_answers",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("candidate_id", sa.Integer(), sa.ForeignKey("candidates.id"), nullable=False),
            sa.Column("job_id", sa.Integer(), sa.ForeignKey("jobs.id"), nullable=False),
            sa.Column("application_id", sa.Integer(), sa.ForeignKey("applications.id"), nullable=True),
            sa.Column("conversation_id", sa.Integer(), sa.ForeignKey("conversations.id"), nullable=True),
            sa.Column("question_key", sa.String(length=120), nullable=False),
            sa.Column("question_text", sa.Text(), nullable=False),
            sa.Column("answer_text", sa.Text(), nullable=True),
            sa.Column("answer_status", sa.String(length=40), nullable=False, server_default="Unanswered"),
            sa.Column("evidence_message_id", sa.Integer(), sa.ForeignKey("messages.id"), nullable=True),
            sa.Column("asked_at", sa.String(length=40), nullable=True),
            sa.Column("answered_at", sa.String(length=40), nullable=True),
            sa.Column("metadata_json", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("application_id", "question_key", name="uq_application_question_key"),
        )
        op.create_index("ix_candidate_question_answers_candidate_id", "candidate_question_answers", ["candidate_id"])
        op.create_index("ix_candidate_question_answers_job_id", "candidate_question_answers", ["job_id"])
        op.create_index("ix_candidate_question_answers_application_id", "candidate_question_answers", ["application_id"])
        op.create_index("ix_candidate_question_answers_answer_status", "candidate_question_answers", ["answer_status"])

    if "external_action_logs" not in tables:
        op.create_table(
            "external_action_logs",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("action_type", sa.String(length=120), nullable=False),
            sa.Column("platform", sa.String(length=80), nullable=False),
            sa.Column("account_id", sa.Integer(), sa.ForeignKey("platform_accounts.id"), nullable=True),
            sa.Column("target_account", sa.String(length=255), nullable=True),
            sa.Column("platform_object_id", sa.String(length=160), nullable=True),
            sa.Column("queue_task_id", sa.Integer(), sa.ForeignKey("queue_tasks.id"), nullable=True),
            sa.Column("status", sa.String(length=40), nullable=False, server_default="Pending"),
            sa.Column("attempted_at", sa.String(length=40), nullable=False),
            sa.Column("verified_at", sa.String(length=40), nullable=True),
            sa.Column("result", sa.Text(), nullable=True),
            sa.Column("evidence", sa.JSON(), nullable=False, server_default="{}"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_external_action_logs_action_type", "external_action_logs", ["action_type"])
        op.create_index("ix_external_action_logs_platform", "external_action_logs", ["platform"])
        op.create_index("ix_external_action_logs_platform_object_id", "external_action_logs", ["platform_object_id"])
        op.create_index("ix_external_action_logs_status", "external_action_logs", ["status"])


def downgrade() -> None:
    op.drop_index("ix_external_action_logs_status", table_name="external_action_logs")
    op.drop_index("ix_external_action_logs_platform_object_id", table_name="external_action_logs")
    op.drop_index("ix_external_action_logs_platform", table_name="external_action_logs")
    op.drop_index("ix_external_action_logs_action_type", table_name="external_action_logs")
    op.drop_table("external_action_logs")
    op.drop_index("ix_candidate_question_answers_answer_status", table_name="candidate_question_answers")
    op.drop_index("ix_candidate_question_answers_application_id", table_name="candidate_question_answers")
    op.drop_index("ix_candidate_question_answers_job_id", table_name="candidate_question_answers")
    op.drop_index("ix_candidate_question_answers_candidate_id", table_name="candidate_question_answers")
    op.drop_table("candidate_question_answers")
