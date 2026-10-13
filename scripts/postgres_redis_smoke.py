"""PostgreSQL + Redis integration smoke.

This script is intentionally small and deterministic. It assumes DATABASE_URL and
REDIS_URL point at real services, then verifies Alembic head, inserts a DB-backed
queue task, publishes its transactional outbox row to Redis, and confirms the
Redis stream entry exists.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid

from sqlalchemy import select, text

from backend.app.core.db import SessionLocal
from backend.app.models.domain import BusinessEvent, QueueOutbox, QueueTask
from backend.app.services.queue import enqueue_outbox_for_task
from backend.app.services.redis_queue import configured_streams, publish_pending_outbox, redis_client


def run_alembic() -> None:
    result = subprocess.run(["alembic", "upgrade", "head"], text=True, capture_output=True, timeout=120)
    if result.returncode:
        sys.stderr.write(result.stdout + result.stderr)
        raise SystemExit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-alembic", action="store_true")
    args = parser.parse_args()
    if not args.skip_alembic:
        run_alembic()

    redis = redis_client()
    assert redis.ping() is True
    key = f"pg-redis-smoke:{uuid.uuid4().hex}"
    with SessionLocal() as db:
        version = db.execute(text("select version_num from alembic_version")).scalar_one()
        assert version == "0026_scope_event_repairs", version
        db.add(BusinessEvent(event_key=key, entity_type="smoke", entity_id=key, from_state=None, to_state="created", status="Applied", reason="postgres redis smoke", evidence={}))
        task = QueueTask(queue_name="integration-smoke", task_type="smoke.noop", payload={"event_key": key}, idempotency_key=key, status="Queued")
        db.add(task)
        db.flush()
        outbox = enqueue_outbox_for_task(db, task)
        db.commit()
        published = publish_pending_outbox(db, client=redis)
        db.refresh(outbox)
        assert published >= 1
        assert outbox.status == "Published"
        assert outbox.redis_stream_id
        streams = configured_streams(db, {"integration-smoke"})
        assert streams == ["recruitment:integration-smoke"]
        rows = redis.xrange("recruitment:integration-smoke", min=outbox.redis_stream_id, max=outbox.redis_stream_id)
        assert rows and rows[0][0] == outbox.redis_stream_id
        assert db.scalar(select(QueueOutbox).where(QueueOutbox.id == outbox.id)).redis_stream_id == outbox.redis_stream_id
    print("postgres_redis_smoke=passed")


if __name__ == "__main__":
    main()
