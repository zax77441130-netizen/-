from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, inspect, pool, text

from backend.app.core.config import get_settings
from backend.app.core.db import Base
from backend.app.models import domain  # noqa: F401


config = context.config
config.set_main_option("sqlalchemy.url", get_settings().database_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


LEGACY_LONG_REVISIONS = {
    "0004_platform_identity_phone_states": "0004_platform_identity_phone",
    "0005_email_cursor_uid_reliability": "0005_email_cursor_uid_rel",
    "0009_platform_webhooks_and_rate_limits": "0009_platform_webhooks_rate",
    "0016_queue_outbox_revision_repair": "0016_queue_outbox_rev_repair",
    "0017_queue_outbox_constraints_and_retention_fk": "0017_queue_retention_fk",
    "0023_blacklist_scope_and_replay_guards": "0023_blacklist_replay_guards",
}
POST_MIGRATION_REVISION_FIXUPS = {
}


def normalize_legacy_revision_ids(connection) -> None:
    """Map only renamed revision identifiers; do not advance migration state."""
    if not inspect(connection).has_table("alembic_version"):
        return
    rows = connection.execute(text("select version_num from alembic_version")).fetchall()
    if not rows:
        return
    for long_revision, short_revision in LEGACY_LONG_REVISIONS.items():
        connection.execute(
            text("update alembic_version set version_num = :short where version_num = :long"),
            {"short": short_revision, "long": long_revision},
        )


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.begin() as connection:
        normalize_legacy_revision_ids(connection)
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
