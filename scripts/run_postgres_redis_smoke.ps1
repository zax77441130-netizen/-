param(
  [string]$DatabaseUrl = "postgresql+psycopg://recruit:recruit_dev_password@127.0.0.1:5432/recruitment",
  [string]$RedisUrl = "redis://:recruit_redis_dev_password@127.0.0.1:6379/0"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  throw "docker command is not installed or not on PATH."
}

docker compose up -d postgres redis
$env:DATABASE_URL = $DatabaseUrl
$env:REDIS_URL = $RedisUrl
$env:QUEUE_BACKEND = "redis"

python scripts/postgres_redis_smoke.py
docker compose restart redis
Start-Sleep -Seconds 5
python scripts/postgres_redis_smoke.py --skip-alembic
python -m pytest backend/tests/test_gapfix_20261008.py -q
