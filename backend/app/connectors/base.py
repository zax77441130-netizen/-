from abc import ABC, abstractmethod
from dataclasses import dataclass


STATUS_NOT_CONFIGURED = "Not Configured"
STATUS_PAUSED = "Paused"
STATUS_DELIVERED = "Delivered"
STATUS_ACCEPTED = "Accepted"
STATUS_SENT = "Sent"
STATUS_VERIFIED = "Verified"
STATUS_UNKNOWN = "Unknown"
STATUS_DELIVERY_UNCONFIRMED = "Delivery Unconfirmed"
STATUS_FAILED = "Failed"
STATUS_UNSUPPORTED = "Unsupported"


@dataclass
class ConnectorResult:
    ok: bool
    status: str
    message: str
    data: dict | None = None


class PlatformAdapter(ABC):
    platform_name: str
    capabilities: set[str] = set()

    @abstractmethod
    async def connect(self) -> ConnectorResult: ...

    @abstractmethod
    async def disconnect(self) -> ConnectorResult: ...

    @abstractmethod
    async def test_connection(self) -> ConnectorResult: ...

    @abstractmethod
    async def create_post(self, content: str, target: dict) -> ConnectorResult: ...

    async def update_post(self, post_id: str, content: str) -> ConnectorResult:
        return ConnectorResult(False, "Unsupported", "This adapter does not support updating posts yet.")

    async def delete_post(self, post_id: str) -> ConnectorResult:
        return ConnectorResult(False, "Unsupported", "This adapter does not support deleting posts yet.")

    async def fetch_messages(self) -> ConnectorResult:
        return ConnectorResult(True, "Not Configured", "Credentials are required before syncing messages.", {"items": []})

    async def send_message(self, conversation_id: str, body: str) -> ConnectorResult:
        return ConnectorResult(False, "Not Configured", "Messaging is not configured.")

    async def fetch_comments(self, post_id: str) -> ConnectorResult:
        return ConnectorResult(True, "Not Configured", "Credentials are required before syncing comments.", {"items": []})

    async def reply_comment(self, comment_id: str, body: str) -> ConnectorResult:
        return ConnectorResult(False, "Not Configured", "Comment replies are not configured.")

    async def fetch_applicants(self) -> ConnectorResult:
        return ConnectorResult(True, "Not Configured", "Applicant sync is not configured.", {"items": []})

    async def fetch_resume(self, applicant_id: str) -> ConnectorResult:
        return ConnectorResult(False, "Not Configured", "Resume sync is not configured.")

    async def get_status(self) -> ConnectorResult:
        return ConnectorResult(True, "Not Configured", "Adapter exists, credentials not configured.")

    async def get_rate_limit(self) -> ConnectorResult:
        return ConnectorResult(True, "Unknown", "No live API rate-limit data available.", {"limit": None})

    async def verify_post(self, platform_post_id: str) -> ConnectorResult:
        return ConnectorResult(False, STATUS_UNSUPPORTED, "This adapter cannot verify posts.")

    async def verify_message(self, platform_message_id: str) -> ConnectorResult:
        return ConnectorResult(False, STATUS_UNSUPPORTED, "This adapter cannot verify messages.")
