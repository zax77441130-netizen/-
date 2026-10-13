from backend.app.connectors.adapters import ChickPTAdapter, ConfiguredApiAdapter, EmailAdapter, FacebookAdapter, LineAdapter, Recruit518Adapter
from backend.app.connectors.base import PlatformAdapter


SUPPORTED_PLATFORMS = {
    "facebook": "ConfiguredApiAdapter",
    "518": "Recruit518Adapter",
    "chickpt": "ChickPTAdapter",
    "email": "EmailAdapter",
    "line": "ConfiguredApiAdapter",
    "web": "ConfiguredApiAdapter",
}


def get_adapter(platform: str, config: dict | None = None) -> PlatformAdapter:
    key = platform.lower()
    if key == "chickpt":
        return ChickPTAdapter(config)
    if key == "518":
        return Recruit518Adapter(config)
    if key == "email":
        return EmailAdapter(config)
    if key == "facebook":
        return FacebookAdapter(config)
    if key == "line":
        return LineAdapter(config)
    if key in {"web"}:
        return ConfiguredApiAdapter(key, config)
    return ConfiguredApiAdapter(platform, config)
