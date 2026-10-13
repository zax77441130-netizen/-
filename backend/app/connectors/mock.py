from backend.app.connectors.base import ConnectorResult, PlatformAdapter


class SemiAutoAdapter(PlatformAdapter):
    def __init__(self, platform_name: str):
        self.platform_name = platform_name

    async def connect(self) -> ConnectorResult:
        return ConnectorResult(True, "Not Configured", f"{self.platform_name} adapter is ready for credentials.")

    async def disconnect(self) -> ConnectorResult:
        return ConnectorResult(True, "Disconnected", f"{self.platform_name} credentials removed from runtime session.")

    async def test_connection(self) -> ConnectorResult:
        return ConnectorResult(False, "Not Configured", f"{self.platform_name} needs API/OAuth settings before live tests.")

    async def create_post(self, content: str, target: dict) -> ConnectorResult:
        return ConnectorResult(
            False,
            "Manual Review Required",
            "No legal live connector is configured. Content is prepared for staff-assisted posting.",
            {"content": content, "target": target},
        )
