from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from backend.app.core.db import SessionLocal
from backend.app.models.domain import User
from backend.app.services.security import hash_password


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the first RecruitOps administrator.")
    parser.add_argument("--email", required=True)
    parser.add_argument("--name", default="Administrator")
    parser.add_argument("--password", default=None)
    args = parser.parse_args()

    password = args.password or getpass.getpass("Password: ")
    if len(password) < 8:
        raise SystemExit("Password must be at least 8 characters.")

    with SessionLocal() as db:
        existing = db.scalar(select(User).where(User.email == args.email))
        if existing:
            existing.name = args.name
            existing.role = "Super Admin"
            existing.password_hash = hash_password(password)
            existing.active = True
            action = "updated"
        else:
            db.add(User(email=args.email, name=args.name, role="Super Admin", password_hash=hash_password(password), active=True))
            action = "created"
        db.commit()
    print(f"Super Admin {action}: {args.email}")


if __name__ == "__main__":
    main()
