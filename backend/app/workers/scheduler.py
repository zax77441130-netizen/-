import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select

from backend.app.core.config import get_settings
from backend.app.core.db import SessionLocal
from backend.app.models.domain import PlatformAccount, QueueTask
from backend.app.services.queue import enqueue_outbox_for_task, process_task, reserve_next_task
from backend.app.services.queue_utils import utc_now
from backend.app.services.redis_queue import publish_pending_outbox, reconcile_outbox, run_redis_worker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("recruitment-worker")


def ensure_periodic_email_poll_tasks(db) -> None:
    accounts = db.execute(
        select(PlatformAccount)
        .where(PlatformAccount.platform.in_(["email", "518-email"]))
        .where(PlatformAccount.status.in_(["Configured", "Connected"]))
    ).scalars().all()
    changed = False
    now = datetime.now(UTC)
    for account in accounts:
        config = account.config or {}
        if not config.get("auto_poll_518"):
            continue
        mailbox = config.get("mailbox", "INBOX")
        interval_seconds = int(config.get("poll_interval_seconds") or get_settings().email_518_poll_interval_seconds)
        bucket = int(now.timestamp()) // max(interval_seconds, 60)
        idempotency_key = f"periodic-email-518:{account.id}:{mailbox}:{bucket}"
        existing = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == idempotency_key))
        if existing:
            continue
        running = db.scalar(
            select(QueueTask)
            .where(QueueTask.task_type == "email.poll_518")
            .where(QueueTask.status.in_(["Queued", "Retry", "Running"]))
            .where(QueueTask.payload["account_id"].as_integer() == account.id)
            .limit(1)
        )
        if running:
            continue
        task = QueueTask(
            queue_name="email",
            task_type="email.poll_518",
            payload={"account_id": account.id, "mailbox": mailbox},
            idempotency_key=idempotency_key,
            scheduled_at=utc_now(),
            max_retries=int(config.get("poll_max_retries") or 5),
        )
        db.add(task)
        db.flush()
        enqueue_outbox_for_task(db, task)
        changed = True
    if changed:
        db.commit()


def ensure_periodic_retention_cleanup_tasks(db) -> None:
    settings = get_settings()
    interval_seconds = int(settings.retention_cleanup_interval_seconds)
    batch_size = int(settings.retention_cleanup_batch_size)
    now = datetime.now(UTC)
    bucket = int(now.timestamp()) // max(interval_seconds, 3600)
    idempotency_key = f"periodic-retention-cleanup:{bucket}"
    existing = db.scalar(select(QueueTask).where(QueueTask.idempotency_key == idempotency_key))
    if existing:
        return
    running = db.scalar(
        select(QueueTask)
        .where(QueueTask.task_type == "privacy.retention_cleanup")
        .where(QueueTask.status.in_(["Queued", "Retry", "Running"]))
        .limit(1)
    )
    if running:
        return
    task = QueueTask(
        queue_name="privacy",
        task_type="privacy.retention_cleanup",
        payload={"batch_size": batch_size, "source": "periodic_scheduler"},
        idempotency_key=idempotency_key,
        scheduled_at=utc_now(),
        max_retries=int(settings.retention_cleanup_max_retries),
    )
    db.add(task)
    db.flush()
    enqueue_outbox_for_task(db, task)
    db.commit()


async def main() -> None:
    settings = get_settings()
    queue_names = settings.worker_queue_set
    logger.info("Worker started. Queues=%s", ",".join(sorted(queue_names)) if queue_names else "all")
    if settings.queue_backend.lower() == "redis":
        logger.info("Redis Streams worker mode enabled.")
        def periodic_hook(db):
            ensure_periodic_email_poll_tasks(db)
            ensure_periodic_retention_cleanup_tasks(db)

        await run_redis_worker(SessionLocal, queue_names=queue_names, worker_id=settings.worker_id, periodic_hook=periodic_hook)
        return
    while True:
        with SessionLocal() as db:
            ensure_periodic_email_poll_tasks(db)
            ensure_periodic_retention_cleanup_tasks(db)
            try:
                reconcile_outbox(db)
                publish_pending_outbox(db, limit=100)
            except Exception as exc:
                logger.warning("Redis outbox publish failed; continuing with PostgreSQL source-of-truth queue: %s", exc)
            task = reserve_next_task(db, queue_names=queue_names)
            if task:
                logger.info("Processing task %s %s", task.id, task.task_type)
                await process_task(db, task)
                continue
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
