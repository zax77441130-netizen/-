from __future__ import annotations

import json
import uuid
from pathlib import Path

import httpx

from backend.app.connectors.base import (
    STATUS_ACCEPTED,
    STATUS_DELIVERED,
    STATUS_DELIVERY_UNCONFIRMED,
    STATUS_FAILED,
    STATUS_NOT_CONFIGURED,
    STATUS_PAUSED,
    STATUS_SENT,
    STATUS_UNKNOWN,
    STATUS_VERIFIED,
    ConnectorResult,
    PlatformAdapter,
)


class ConfiguredApiAdapter(PlatformAdapter):
    """Adapter for official API integrations configured per platform account.

    This class intentionally refuses to report success without an endpoint and
    verifiable platform id returned by the upstream service.
    """

    def __init__(self, platform_name: str, config: dict | None = None):
        self.platform_name = platform_name
        self.config = config or {}
        self.capabilities = set(self.config.get("capabilities") or [])

    def _configured(self, capability: str) -> bool:
        return capability in self.capabilities and bool(self.config.get("base_url") or self.config.get(f"{capability}_url"))

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = self.config.get("api_token")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _post(self, capability: str, payload: dict) -> ConnectorResult:
        endpoint = self.config.get(f"{capability}_url") or f"{self.config.get('base_url', '').rstrip('/')}/{capability}"
        if self.config.get("fixture_path"):
            fixture = json.loads(Path(self.config["fixture_path"]).read_text(encoding="utf-8"))
            result = fixture.get(capability) or {}
            data = {**(result.get("data", result) or {}), "request_payload": payload}
            return ConnectorResult(bool(result.get("ok", True)), result.get("status", STATUS_DELIVERED), result.get("message", "Fixture action completed."), data)
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.post(endpoint, json=payload, headers=self._headers())
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_FAILED, f"HTTP {response.status_code}: {response.text[:500]}")
        data = response.json()
        platform_id = data.get("platform_post_id") or data.get("platform_message_id") or data.get("id")
        if not platform_id:
            return ConnectorResult(False, STATUS_DELIVERY_UNCONFIRMED, "Upstream accepted the request but did not return a verifiable platform id.", data)
        return ConnectorResult(True, data.get("status", STATUS_DELIVERED), data.get("message", "Upstream action completed."), data)

    async def _get(self, capability: str, params: dict | None = None) -> ConnectorResult:
        endpoint = self.config.get(f"{capability}_url") or f"{self.config.get('base_url', '').rstrip('/')}/{capability}"
        if self.config.get("fixture_path"):
            fixture = json.loads(Path(self.config["fixture_path"]).read_text(encoding="utf-8"))
            result = fixture.get(capability) or {}
            return ConnectorResult(bool(result.get("ok", True)), result.get("status", STATUS_DELIVERED), result.get("message", "Fixture query completed."), result.get("data", result))
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.get(endpoint, params=params or {}, headers=self._headers())
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_FAILED, f"HTTP {response.status_code}: {response.text[:500]}")
        return ConnectorResult(True, STATUS_DELIVERED, "Upstream query completed.", response.json())

    async def connect(self) -> ConnectorResult:
        if not self.config:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, f"{self.platform_name} account config is missing.")
        return ConnectorResult(True, "Configured", f"{self.platform_name} account configuration is present.")

    async def disconnect(self) -> ConnectorResult:
        return ConnectorResult(True, "Disconnected", f"{self.platform_name} runtime session disconnected.")

    async def test_connection(self) -> ConnectorResult:
        if not self.config.get("base_url"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Official API base_url is required for live checks.")
        if self.config.get("fixture_path"):
            return ConnectorResult(True, "Configured", "Fixture-backed connector is available for integration tests.")
        result = await self._get("status")
        return result

    async def create_post(self, content: str, target: dict) -> ConnectorResult:
        if not self._configured("publish"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Publishing endpoint and capability are not configured.")
        return await self._post("publish", {"content": content, "target": target})

    async def send_message(self, conversation_id: str, body: str) -> ConnectorResult:
        if not self._configured("send_message"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Messaging endpoint and capability are not configured.")
        return await self._post("send_message", {"conversation_id": conversation_id, "body": body})

    async def fetch_messages(self) -> ConnectorResult:
        if not self._configured("read_messages"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Message reading is not configured.")
        return await self._get("read_messages")

    async def verify_post(self, platform_post_id: str) -> ConnectorResult:
        if not self._configured("verify_post"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Post verification is not configured.")
        return await self._get("verify_post", {"platform_post_id": platform_post_id})

    async def verify_message(self, platform_message_id: str) -> ConnectorResult:
        if not self._configured("verify_message"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Message verification is not configured.")
        return await self._get("verify_message", {"platform_message_id": platform_message_id})

    async def get_status(self) -> ConnectorResult:
        if not self.config:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "No account configuration is present.")
        return ConnectorResult(True, "Configured", "Configured, but live credentials must be tested before use.", {"capabilities": sorted(self.capabilities)})


class ChickPTAdapter(PlatformAdapter):
    platform_name = "chickpt"
    capabilities = {"publish", "read_messages", "fetch_resume", "send_message", "verify_post", "verify_message"}

    def __init__(self, config: dict | None = None):
        self.config = config or {}

    def _fixture(self, capability: str) -> ConnectorResult | None:
        if not self.config.get("fixture_path"):
            return None
        fixture = json.loads(Path(self.config["fixture_path"]).read_text(encoding="utf-8"))
        result = fixture.get(capability) or {}
        return ConnectorResult(bool(result.get("ok", True)), result.get("status", STATUS_DELIVERED), result.get("message", "ChickPT fixture action completed."), result.get("data", result))

    async def connect(self) -> ConnectorResult:
        if not self.config.get("session_state_path"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Persistent browser session_state_path is required.")
        return ConnectorResult(True, "Configured", "Persistent browser profile configured.")

    async def disconnect(self) -> ConnectorResult:
        return ConnectorResult(True, "Disconnected", "ChickPT browser session disconnected.")

    async def test_connection(self) -> ConnectorResult:
        fixture = self._fixture("status")
        if fixture:
            return fixture
        if not self.config.get("session_state_path"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "ChickPT persistent browser session is missing.")
        return ConnectorResult(False, STATUS_PAUSED, "Login/session validation requires an authorized browser worker run.")

    async def create_post(self, content: str, target: dict) -> ConnectorResult:
        fixture = self._fixture("publish")
        if fixture:
            return fixture
        from backend.app.workers.browser_worker import BrowserAutomationPaused, create_chickpt_post

        try:
            result = await create_chickpt_post(self.config, content, target)
        except BrowserAutomationPaused as exc:
            return ConnectorResult(False, STATUS_PAUSED, str(exc), {"target": target})
        return ConnectorResult(result.ok, result.status, result.message, result.data)

    async def send_message(self, conversation_id: str, body: str) -> ConnectorResult:
        fixture = self._fixture("send_message")
        if fixture:
            return fixture
        from backend.app.workers.browser_worker import BrowserAutomationPaused, send_chickpt_message

        try:
            result = await send_chickpt_message(self.config, conversation_id, body)
        except BrowserAutomationPaused as exc:
            return ConnectorResult(False, STATUS_PAUSED, str(exc), {"conversation_id": conversation_id})
        return ConnectorResult(result.ok, result.status, result.message, result.data)

    async def fetch_messages(self) -> ConnectorResult:
        fixture = self._fixture("read_messages")
        if fixture:
            return fixture
        from backend.app.workers.browser_worker import BrowserAutomationPaused, read_chickpt_inbox

        try:
            result = await read_chickpt_inbox(self.config)
        except BrowserAutomationPaused as exc:
            return ConnectorResult(False, STATUS_PAUSED, str(exc))
        return ConnectorResult(result.ok, result.status, result.message, result.data)

    async def fetch_resume(self, applicant_id: str) -> ConnectorResult:
        fixture = self._fixture("fetch_resume")
        if fixture:
            return fixture
        if not self.config.get("session_state_path"):
            return ConnectorResult(False, STATUS_PAUSED, "ChickPT resume fetch requires authorized worker session verification.", {"applicant_id": applicant_id})
        from backend.app.workers.browser_worker import BrowserAutomationPaused, fetch_resume_page

        try:
            result = await fetch_resume_page(self.config, applicant_id)
        except BrowserAutomationPaused as exc:
            return ConnectorResult(False, STATUS_PAUSED, str(exc), {"applicant_id": applicant_id})
        return ConnectorResult(result.ok, result.status, result.message, result.data)

    async def verify_post(self, platform_post_id: str) -> ConnectorResult:
        fixture = self._fixture("verify_post")
        if fixture:
            return fixture
        from backend.app.workers.browser_worker import BrowserAutomationPaused, verify_chickpt_post

        try:
            result = await verify_chickpt_post(self.config, platform_post_id)
        except BrowserAutomationPaused as exc:
            return ConnectorResult(False, STATUS_PAUSED, str(exc), {"platform_post_id": platform_post_id})
        return ConnectorResult(result.ok, result.status, result.message, result.data)

    async def verify_message(self, platform_message_id: str) -> ConnectorResult:
        fixture = self._fixture("verify_message")
        if fixture:
            return fixture
        from backend.app.workers.browser_worker import BrowserAutomationPaused, verify_chickpt_message

        try:
            result = await verify_chickpt_message(self.config, platform_message_id)
        except BrowserAutomationPaused as exc:
            return ConnectorResult(False, STATUS_PAUSED, str(exc), {"platform_message_id": platform_message_id})
        return ConnectorResult(result.ok, result.status, result.message, result.data)

    async def get_status(self) -> ConnectorResult:
        if not self.config.get("session_state_path"):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "ChickPT session storage is not configured.")
        return ConnectorResult(True, STATUS_PAUSED, "Ready for isolated browser worker validation.")


class FacebookAdapter(ConfiguredApiAdapter):
    def __init__(self, config: dict | None = None):
        super().__init__("facebook", config)

    async def create_post(self, content: str, target: dict) -> ConnectorResult:
        if self.config.get("fixture_path"):
            return await self._post("publish", {"content": content, "target": target})
        page_id = target.get("external_page_id") or target.get("page_id") or self.config.get("page_id")
        group_id = target.get("external_group_id") or target.get("group_id")
        access_token = self.config.get("page_access_token") or self.config.get("api_token")
        if not access_token or not (page_id or group_id):
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Facebook publishing requires page/group id and access token.")
        graph_base = self.config.get("graph_base_url", "https://graph.facebook.com/v20.0").rstrip("/")
        node_id = group_id or page_id
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.post(f"{graph_base}/{node_id}/feed", data={"message": content, "access_token": access_token})
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_FAILED, f"Facebook Graph API HTTP {response.status_code}: {response.text[:500]}")
        data = response.json()
        post_id = data.get("id")
        if not post_id:
            return ConnectorResult(False, STATUS_DELIVERY_UNCONFIRMED, "Facebook accepted response without post id.", data)
        return ConnectorResult(True, STATUS_ACCEPTED, "Facebook post accepted; verification task must read it back.", {"platform_post_id": post_id, "raw": data})

    async def send_message(self, conversation_id: str, body: str) -> ConnectorResult:
        if self.config.get("fixture_path"):
            return await self._post("send_message", {"recipient_id": conversation_id, "body": body})
        page_token = self.config.get("page_access_token") or self.config.get("api_token")
        if not page_token:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Facebook messaging requires a page access token.")
        graph_base = self.config.get("graph_base_url", "https://graph.facebook.com/v20.0").rstrip("/")
        payload = {"recipient": {"id": conversation_id}, "message": {"text": body}, "messaging_type": "RESPONSE"}
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.post(f"{graph_base}/me/messages", params={"access_token": page_token}, json=payload)
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_FAILED, f"Facebook Messenger API HTTP {response.status_code}: {response.text[:500]}")
        data = response.json()
        message_id = data.get("message_id") or data.get("mid")
        if not message_id:
            return ConnectorResult(False, STATUS_DELIVERY_UNCONFIRMED, "Facebook accepted response without message id.", data)
        return ConnectorResult(True, STATUS_SENT, "Facebook message sent; delivery/read remains Unknown unless webhook evidence arrives.", {"platform_message_id": message_id, "candidate_read_status": STATUS_UNKNOWN, "raw": data})

    async def send_private_reply(self, comment_id: str, body: str) -> ConnectorResult:
        if self.config.get("fixture_path"):
            return await self._post("private_reply", {"comment_id": comment_id, "body": body})
        page_token = self.config.get("page_access_token") or self.config.get("api_token")
        if not page_token:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Facebook private replies require a page access token.")
        graph_base = self.config.get("graph_base_url", "https://graph.facebook.com/v20.0").rstrip("/")
        payload = {"message": body}
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.post(f"{graph_base}/{comment_id}/private_replies", params={"access_token": page_token}, json=payload)
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_FAILED, f"Facebook private reply HTTP {response.status_code}: {response.text[:500]}")
        data = response.json()
        message_id = data.get("id") or data.get("message_id") or data.get("mid")
        if not message_id:
            return ConnectorResult(False, STATUS_DELIVERY_UNCONFIRMED, "Facebook private reply accepted without message id.", data)
        return ConnectorResult(True, STATUS_SENT, "Facebook private reply sent; later Messenger continuity requires PSID webhook evidence.", {"platform_message_id": message_id, "candidate_read_status": STATUS_UNKNOWN, "raw": data})

    async def verify_post(self, platform_post_id: str) -> ConnectorResult:
        if self.config.get("fixture_path"):
            return await self._get("verify_post", {"platform_post_id": platform_post_id})
        token = self.config.get("page_access_token") or self.config.get("api_token")
        if not token:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Facebook verification requires an access token.")
        graph_base = self.config.get("graph_base_url", "https://graph.facebook.com/v20.0").rstrip("/")
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.get(f"{graph_base}/{platform_post_id}", params={"fields": "id,permalink_url,created_time", "access_token": token})
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_DELIVERY_UNCONFIRMED, f"Facebook post read-back HTTP {response.status_code}: {response.text[:500]}")
        data = response.json()
        if data.get("id"):
            return ConnectorResult(True, STATUS_VERIFIED, "Facebook post read back.", {"platform_post_id": data["id"], "post_url": data.get("permalink_url"), "raw": data})
        return ConnectorResult(False, STATUS_DELIVERY_UNCONFIRMED, "Facebook post was not readable.", data)

    async def fetch_messages(self) -> ConnectorResult:
        if self.config.get("fixture_path"):
            return await self._get("read_messages")
        token = self.config.get("page_access_token") or self.config.get("api_token")
        page_id = self.config.get("page_id")
        if not token or not page_id:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "Facebook read_messages requires page_id and page access token, or Messenger webhook events.")
        graph_base = self.config.get("graph_base_url", "https://graph.facebook.com/v20.0").rstrip("/")
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.get(f"{graph_base}/{page_id}/conversations", params={"fields": "participants,messages.limit(5){id,message,from,created_time}", "access_token": token})
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_FAILED, f"Facebook conversation sync HTTP {response.status_code}: {response.text[:500]}")
        items = []
        for conversation in (response.json().get("data") or []):
            for msg in ((conversation.get("messages") or {}).get("data") or []):
                items.append(
                    {
                        "external_conversation_id": conversation.get("id"),
                        "platform_message_id": msg.get("id"),
                        "body": msg.get("message") or "",
                        "candidate_platform_id": ((msg.get("from") or {}).get("id")),
                    }
                )
        return ConnectorResult(True, STATUS_DELIVERED, "Facebook conversations synced; webhook remains preferred for live inbound.", {"items": items})


class LineAdapter(ConfiguredApiAdapter):
    def __init__(self, config: dict | None = None):
        super().__init__("line", config)

    def _line_headers(self) -> dict[str, str]:
        token = self.config.get("channel_access_token") or self.config.get("api_token")
        return {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    async def send_message(self, conversation_id: str, body: str) -> ConnectorResult:
        if self.config.get("fixture_path"):
            return await self._post("send_message", {"to": conversation_id, "body": body})
        token = self.config.get("channel_access_token") or self.config.get("api_token")
        if not token:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, "LINE Messaging API requires channel_access_token.")
        base = self.config.get("line_base_url", "https://api.line.me").rstrip("/")
        payload = {"to": conversation_id, "messages": [{"type": "text", "text": body}]}
        headers = self._line_headers()
        retry_key = self.config.get("line_retry_key") or str(uuid.uuid4())
        headers["X-Line-Retry-Key"] = retry_key
        async with httpx.AsyncClient(timeout=float(self.config.get("timeout_seconds") or 30)) as client:
            response = await client.post(f"{base}/v2/bot/message/push", json=payload, headers=headers)
        if response.status_code == 409:
            request_id = response.headers.get("x-line-request-id") or response.headers.get("x-line-accepted-request-id")
            evidence = {"candidate_read_status": STATUS_UNKNOWN, "retry_key": retry_key, "accepted_duplicate": True, "response_body": response.text[:500]}
            if request_id:
                evidence["platform_message_id"] = request_id
            return ConnectorResult(True, STATUS_ACCEPTED, "LINE accepted a duplicate retry key; treating the logical send as accepted.", evidence)
        if response.status_code >= 400:
            return ConnectorResult(False, STATUS_FAILED, f"LINE Messaging API HTTP {response.status_code}: {response.text[:500]}")
        request_id = response.headers.get("x-line-request-id")
        if not request_id:
            return ConnectorResult(True, STATUS_SENT, "LINE accepted push request; no delivery/read proof is provided by this response.", {"candidate_read_status": STATUS_UNKNOWN, "retry_key": retry_key})
        return ConnectorResult(True, STATUS_SENT, "LINE push request sent; delivery/read remains Unknown unless webhook evidence arrives.", {"platform_message_id": request_id, "candidate_read_status": STATUS_UNKNOWN, "retry_key": retry_key})

    async def verify_message(self, platform_message_id: str) -> ConnectorResult:
        if self.config.get("fixture_path"):
            return await self._get("verify_message", {"platform_message_id": platform_message_id})
        return ConnectorResult(True, STATUS_UNKNOWN, "LINE push responses do not provide delivery/read verification; wait for webhook evidence.", {"platform_message_id": platform_message_id, "candidate_read_status": STATUS_UNKNOWN})


class Recruit518Adapter(PlatformAdapter):
    platform_name = "518"
    capabilities = {"fetch_resume"}

    def __init__(self, config: dict | None = None):
        self.config = config or {}

    def _fixture(self, capability: str) -> ConnectorResult | None:
        if not self.config.get("fixture_path"):
            return None
        fixture = json.loads(Path(self.config["fixture_path"]).read_text(encoding="utf-8"))
        result = fixture.get(capability) or {}
        return ConnectorResult(bool(result.get("ok", True)), result.get("status", STATUS_DELIVERED), result.get("message", "518 fixture action completed."), result.get("data", result))

    async def connect(self) -> ConnectorResult:
        return ConnectorResult(False, STATUS_NOT_CONFIGURED, "518 applicants are imported from Email first; page fetch needs browser credentials.")

    async def disconnect(self) -> ConnectorResult:
        return ConnectorResult(True, "Disconnected", "518 runtime session disconnected.")

    async def test_connection(self) -> ConnectorResult:
        return ConnectorResult(False, STATUS_NOT_CONFIGURED, "518 page credentials are not configured.")

    async def create_post(self, content: str, target: dict) -> ConnectorResult:
        return ConnectorResult(False, STATUS_FAILED, "518 posting is outside this adapter; use authorized 518 job management credentials if provided.")

    async def fetch_resume(self, applicant_id: str) -> ConnectorResult:
        fixture = self._fixture("fetch_resume")
        if fixture:
            return fixture
        if not self.config.get("session_state_path"):
            return ConnectorResult(False, STATUS_PAUSED, "518 resume page fetch needs an authorized browser session.")
        from backend.app.workers.browser_worker import BrowserAutomationPaused, fetch_resume_page

        try:
            result = await fetch_resume_page(self.config, applicant_id)
        except BrowserAutomationPaused as exc:
            return ConnectorResult(False, STATUS_PAUSED, str(exc), {"applicant_id": applicant_id})
        return ConnectorResult(result.ok, result.status, result.message, result.data)

    async def get_status(self) -> ConnectorResult:
        return ConnectorResult(False, STATUS_NOT_CONFIGURED, "518 is currently Email-inbound only unless browser credentials are added.")


class EmailAdapter(ConfiguredApiAdapter):
    def __init__(self, config: dict | None = None):
        super().__init__("email", config)
        self.capabilities = {"read_messages", "send_message"}

    async def test_connection(self) -> ConnectorResult:
        required = ["inbound_protocol", "host", "username"]
        missing = [key for key in required if not self.config.get(key)]
        if missing:
            return ConnectorResult(False, STATUS_NOT_CONFIGURED, f"Email config missing: {', '.join(missing)}.")
        return ConnectorResult(False, STATUS_DELIVERY_UNCONFIRMED, "Email credentials are configured but no live mailbox test ran.")
