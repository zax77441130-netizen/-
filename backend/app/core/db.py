from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from backend.app.core.config import get_settings


settings = get_settings()
connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


if settings.database_url.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def enable_sqlite_foreign_keys(dbapi_connection, connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class Base(DeclarativeBase):
    pass


@event.listens_for(Session, "after_flush")
def add_transactional_queue_outbox(session: Session, flush_context) -> None:
    from backend.app.models.domain import QueueOutbox, QueueTask

    new_tasks = [obj for obj in session.new if isinstance(obj, QueueTask)]
    if not new_tasks:
        return
    pending_outbox_task_ids = {
        obj.queue_task_id
        for obj in session.new
        if isinstance(obj, QueueOutbox) and obj.event_type == "queue.publish"
    }
    for task in new_tasks:
        if not task.id or task.id in pending_outbox_task_ids:
            continue
        revision = int(task.delivery_revision or 1)
        session.add(
            QueueOutbox(
                queue_task_id=task.id,
                event_type="queue.publish",
                delivery_revision=revision,
                stream_name=f"recruitment:{task.queue_name}",
                payload={
                    "queue_task_id": task.id,
                    "queue_name": task.queue_name,
                    "task_type": task.task_type,
                    "idempotency_key": task.idempotency_key,
                    "delivery_revision": revision,
                },
                status="Pending",
            )
        )


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
