from __future__ import annotations

import argparse
import re
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path


EXCLUDED_DIR_PARTS = {
    "__pycache__",
    ".pytest_cache",
    "postgres_data",
    "redis_data",
    "node_modules",
    "dist",
    "local_private",
}
EXCLUDED_DIR_PATHS = {
    ("data", "inbound_mail"),
    ("data", "resumes"),
    ("data", "screenshots"),
    ("data", "sessions"),
}
EXCLUDED_NAME_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"^recruitment\.db$",
        r".*\.(db|sqlite|sqlite3|pyc|pyo|log|zip)$",
        r".*session.*\.(json|zip|txt)$",
        r".*cookie.*\.(json|txt)$",
        r".*token.*\.(json|txt|env)$",
        r".*resume.*\.(pdf|docx|doc)$",
        r"^\.env$",
    ]
]


def is_excluded(path: Path, source: Path) -> bool:
    rel = path.relative_to(source)
    parts = rel.parts
    if any(part in EXCLUDED_DIR_PARTS for part in parts[:-1]):
        return True
    for dir_path in EXCLUDED_DIR_PATHS:
        if len(parts) > len(dir_path) and tuple(parts[: len(dir_path)]) == dir_path:
            return True
    return any(pattern.match(path.name) for pattern in EXCLUDED_NAME_PATTERNS)


def build_package(source: Path, destination: Path) -> Path:
    source = source.resolve()
    destination = destination.resolve()
    with tempfile.TemporaryDirectory(prefix="recruitops-package-") as _:
        if destination.exists():
            destination.unlink()
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in source.rglob("*"):
                if not path.is_file() or path.resolve() == destination or is_excluded(path, source):
                    continue
                archive.write(path, path.relative_to(source).as_posix())
        with zipfile.ZipFile(destination) as archive:
            bad = [name for name in archive.namelist() if "\\" in name or is_excluded(source / name, source)]
        if bad:
            destination.unlink(missing_ok=True)
            raise SystemExit(f"Package contains forbidden delivery artifacts or non-portable paths: {', '.join(bad)}")
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a cross-platform clean delivery zip.")
    parser.add_argument("--source", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--destination", default="")
    args = parser.parse_args()
    source = Path(args.source)
    destination = Path(args.destination) if args.destination else source.parent / f"recruitops-{datetime.now().strftime('%Y%m%d-%H%M%S')}-full-clean.zip"
    print(build_package(source, destination))


if __name__ == "__main__":
    main()
