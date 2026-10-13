from functools import lru_cache
import json

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "AI Recruitment Operations"
    app_env: str = "development"
    secret_key: str = "change-me"
    database_url: str = "sqlite:///./recruitment.db"
    redis_url: str = "redis://localhost:6379/0"
    queue_backend: str = "db"
    redis_dlq_warn_threshold: int = 1000
    redis_consumer_group: str = "recruitment-workers"
    redis_pending_idle_ms: int = 60_000
    redis_dlq_max_deliveries: int = 5
    worker_queues: str = ""
    worker_id: str | None = None
    queue_lease_seconds: int = 900
    email_518_poll_interval_seconds: int = 300
    retention_cleanup_interval_seconds: int = 86400
    retention_cleanup_batch_size: int = 500
    retention_cleanup_max_retries: int = 3
    ocr_engine: str = ""
    ocr_languages: str = "chi_tra+eng"
    ocr_raster_dpi: int = 200
    ocr_timeout_seconds: int = 60
    cors_origins: str = "http://localhost:8000"
    demo_mode: bool = False
    demo_admin_password: str | None = None
    cookie_secure: bool = False
    auto_create_tables: bool = True
    session_ttl_seconds: int = 60 * 60 * 12
    login_rate_limit_window_seconds: int = 60
    login_rate_limit_max_attempts: int = 5
    ai_price_json: str = "{}"
    phone_webhook_secret: str | None = None
    webhook_timestamp_tolerance_seconds: int = 300

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def is_production(self) -> bool:
        return self.app_env.lower() in {"production", "prod"}

    @property
    def model_prices(self) -> dict[str, dict[str, float]]:
        try:
            decoded = json.loads(self.ai_price_json or "{}")
            return decoded if isinstance(decoded, dict) else {}
        except json.JSONDecodeError:
            return {}

    @property
    def worker_queue_set(self) -> set[str] | None:
        queues = {item.strip() for item in (self.worker_queues or "").split(",") if item.strip()}
        return queues or None

    def validate_runtime_safety(self) -> None:
        if self.is_production and self.secret_key == "change-me":
            raise RuntimeError("Production refuses to start with SECRET_KEY=change-me.")
        if self.is_production and self.demo_mode:
            raise RuntimeError("Production refuses to start with DEMO_MODE=true.")
        if self.is_production and self.auto_create_tables:
            raise RuntimeError("Production refuses to start with AUTO_CREATE_TABLES=true. Run Alembic migrations instead.")
        if self.demo_mode and not self.demo_admin_password:
            raise RuntimeError("DEMO_MODE=true requires DEMO_ADMIN_PASSWORD.")


@lru_cache
def get_settings() -> Settings:
    return Settings()
