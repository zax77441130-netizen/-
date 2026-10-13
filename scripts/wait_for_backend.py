from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser(description="Wait until RecruitOps backend is ready.")
    parser.add_argument("--url", default="http://backend:8000/api/health")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    deadline = time.time() + args.timeout
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(args.url, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if payload.get("database") == "Healthy":
                print(f"Backend ready: {args.url}")
                return
            last_error = f"Health response was not ready: {payload}"
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(3)
    raise SystemExit(f"Backend did not become ready within {args.timeout}s: {last_error}")


if __name__ == "__main__":
    main()
