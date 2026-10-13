from __future__ import annotations

import asyncio
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class BrowserWorkerResult:
    ok: bool
    status: str
    message: str
    data: dict


class BrowserAutomationPaused(RuntimeError):
    pass


REQUIRED_SELECTORS = {
    "publish": {"title", "content", "submit", "success_marker", "platform_post_id"},
    "inbox": {"message_row", "message_id_attribute", "conversation_id_attribute"},
    "message": {"message_box", "send", "sent_message", "message_id_attribute"},
}


async def _with_page(config: dict):
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise BrowserAutomationPaused("Playwright is not installed. Run `python -m playwright install chromium` after installing requirements.") from exc

    session_state_path = config.get("session_state_path")
    user_data_dir = config.get("user_data_dir")
    if not session_state_path and not user_data_dir:
        raise BrowserAutomationPaused("session_state_path or user_data_dir is required. Store it outside version control.")
    browser = None
    context = None
    playwright = await async_playwright().start()
    try:
        if user_data_dir:
            context = await playwright.chromium.launch_persistent_context(user_data_dir, headless=bool(config.get("headless", True)))
            page = context.pages[0] if context.pages else await context.new_page()
            return playwright, context, page
        browser = await playwright.chromium.launch(headless=bool(config.get("headless", True)))
        context = await browser.new_context(storage_state=session_state_path)
        page = await context.new_page()
        return playwright, browser, page
    except Exception:
        if context and not browser:
            await context.close()
        if browser:
            await browser.close()
        await playwright.stop()
        raise


def _selectors(config: dict, namespace: str) -> dict:
    selectors = (config.get("selectors") or {}).get(namespace) or {}
    if not selectors:
        raise BrowserAutomationPaused(f"Selector registry missing namespace: {namespace}.")
    required_version = config.get("selector_version")
    if not required_version or not selectors.get("version"):
        raise BrowserAutomationPaused("selector_version and selector namespace version are required.")
    if selectors["version"] != required_version:
        raise BrowserAutomationPaused("Selector version mismatch; pause for layout review.")
    missing = [key for key in REQUIRED_SELECTORS.get(namespace, set()) if not selectors.get(key)]
    if missing:
        raise BrowserAutomationPaused(f"Required selector(s) missing for {namespace}: {', '.join(sorted(missing))}.")
    return selectors


def _allowed(config: dict, action: str, url: str | None = None) -> None:
    if action not in set(config.get("allowed_actions") or []):
        raise BrowserAutomationPaused(f"Action {action} is not in allowed_actions.")
    if url:
        hostname = urlparse(url).hostname
        allowed_domains = set(config.get("allowed_domains") or [])
        if not allowed_domains:
            raise BrowserAutomationPaused("allowed_domains must be explicitly configured for browser automation.")
        if hostname not in allowed_domains:
            raise BrowserAutomationPaused(f"Host {hostname or '(missing)'} is not in allowed_domains.")


async def _guard_page(config: dict, page) -> None:
    if config.get("captcha_selector") and await page.locator(config["captcha_selector"]).count():
        raise BrowserAutomationPaused("Captcha or verification challenge detected; paused for human review.")
    if config.get("login_required_selector") and await page.locator(config["login_required_selector"]).count():
        raise BrowserAutomationPaused("Login expired; refresh the authorized browser session.")
    for selector in config.get("unknown_layout_selectors") or []:
        if await page.locator(selector).count():
            raise BrowserAutomationPaused("Unknown or unsupported page layout detected.")


def _guard_response(response, action: str) -> None:
    if response and response.status in {403, 429}:
        raise BrowserAutomationPaused(f"{action} received HTTP {response.status}; paused for manual review and platform limit/session check.")


async def _fill_if_present(page, selector: str | None, value: str | None, timeout: int) -> None:
    if selector and value is not None:
        await page.locator(selector).fill(value, timeout=timeout)


async def _click_if_present(page, selector: str | None, timeout: int) -> None:
    if selector:
        await page.locator(selector).click(timeout=timeout)


async def validate_login(config: dict) -> BrowserWorkerResult:
    if not config.get("logged_in_selector"):
        raise BrowserAutomationPaused("logged_in_selector is required for authorized browser session validation.")
    if not config.get("allowed_domains"):
        raise BrowserAutomationPaused("allowed_domains must be explicitly configured for browser automation.")
    playwright, browser, page = await _with_page(config)
    try:
        response = await page.goto(config["home_url"], wait_until="domcontentloaded", timeout=int(config.get("timeout_ms") or 30000))
        _guard_response(response, "login validation")
        captcha = config.get("captcha_selector")
        login_selector = config.get("login_required_selector")
        if captcha and await page.locator(captcha).count():
            raise BrowserAutomationPaused("Captcha or verification challenge detected; paused for human login.")
        if login_selector and await page.locator(login_selector).count():
            raise BrowserAutomationPaused("Login expired; refresh the authorized browser session.")
        evidence_selector = config.get("logged_in_selector")
        if evidence_selector and not await page.locator(evidence_selector).count():
            raise BrowserAutomationPaused("Logged-in marker was not found; selectors may have changed.")
        return BrowserWorkerResult(True, "Connected", "Authorized browser session is valid.", {"url": page.url})
    finally:
        await browser.close()
        await playwright.stop()


async def fetch_resume_page(config: dict, resume_url: str) -> BrowserWorkerResult:
    allowed_domains = set(config.get("allowed_domains") or ["www.518.com.tw", "518.com.tw", "m.518.com.tw"])
    hostname = urlparse(resume_url).hostname
    if hostname not in allowed_domains:
        raise BrowserAutomationPaused(f"Resume URL host {hostname or '(missing)'} is not in the allowed domain list.")
    playwright, browser, page = await _with_page(config)
    try:
        response = await page.goto(resume_url, wait_until="domcontentloaded", timeout=int(config.get("timeout_ms") or 30000))
        _guard_response(response, "resume fetch")
        if config.get("captcha_selector") and await page.locator(config["captcha_selector"]).count():
            raise BrowserAutomationPaused("Captcha or verification challenge detected; paused for human review.")
        if config.get("login_required_selector") and await page.locator(config["login_required_selector"]).count():
            raise BrowserAutomationPaused("Login expired while fetching resume; refresh the authorized browser session.")
        container = config.get("resume_selector") or "body"
        text = await page.locator(container).inner_text(timeout=int(config.get("timeout_ms") or 30000))
        if not text.strip():
            raise BrowserAutomationPaused("Resume page loaded but no resume text was found.")
        return BrowserWorkerResult(True, "Delivered", "Resume page fetched and verified.", {"raw_text": text, "resume_url": resume_url, "source_url": page.url})
    finally:
        await browser.close()
        await playwright.stop()


async def create_chickpt_post(config: dict, content: str, target: dict) -> BrowserWorkerResult:
    _allowed(config, "publish", config.get("post_url"))
    selectors = _selectors(config, "publish")
    timeout = int(config.get("timeout_ms") or 30000)
    playwright, browser, page = await _with_page(config)
    try:
        response = await page.goto(config["post_url"], wait_until="domcontentloaded", timeout=timeout)
        _guard_response(response, "post creation")
        await _guard_page(config, page)
        await _fill_if_present(page, selectors.get("title"), target.get("title"), timeout)
        await _fill_if_present(page, selectors.get("content"), content, timeout)
        await _fill_if_present(page, selectors.get("salary"), target.get("salary"), timeout)
        await _click_if_present(page, selectors.get("submit"), timeout)
        await page.wait_for_load_state("domcontentloaded", timeout=timeout)
        await _guard_page(config, page)
        platform_post_id = None
        if selectors.get("platform_post_id"):
            platform_post_id = (await page.locator(selectors["platform_post_id"]).first.inner_text(timeout=timeout)).strip()
        post_url = page.url
        if selectors.get("success_marker") and not await page.locator(selectors["success_marker"]).count():
            raise BrowserAutomationPaused("Post submit finished but success marker was not found.")
        return BrowserWorkerResult(True, "Accepted", "ChickPT post submitted; read-back verification is queued.", {"platform_post_id": platform_post_id or post_url, "post_url": post_url, "selector_version": selectors.get("version")})
    finally:
        await browser.close()
        await playwright.stop()


async def read_chickpt_inbox(config: dict) -> BrowserWorkerResult:
    _allowed(config, "read_messages", config.get("inbox_url"))
    selectors = _selectors(config, "inbox")
    timeout = int(config.get("timeout_ms") or 30000)
    playwright, browser, page = await _with_page(config)
    try:
        response = await page.goto(config["inbox_url"], wait_until="domcontentloaded", timeout=timeout)
        _guard_response(response, "inbox read")
        await _guard_page(config, page)
        rows = page.locator(selectors.get("message_row", "body"))
        count = min(await rows.count(), int(config.get("max_items") or 20))
        items = []
        for index in range(count):
            row = rows.nth(index)
            body = await row.inner_text(timeout=timeout)
            external_id = await row.get_attribute(selectors.get("message_id_attribute") or "data-id")
            conversation_id = await row.get_attribute(selectors.get("conversation_id_attribute") or "data-conversation-id")
            items.append({"body": body, "platform_message_id": external_id, "external_conversation_id": conversation_id or external_id})
        return BrowserWorkerResult(True, "Delivered", "ChickPT inbox read.", {"items": items, "source_url": page.url, "selector_version": selectors.get("version")})
    finally:
        await browser.close()
        await playwright.stop()


async def send_chickpt_message(config: dict, conversation_id: str, body: str) -> BrowserWorkerResult:
    message_url = (config.get("message_url_template") or "").format(conversation_id=conversation_id)
    _allowed(config, "send_message", message_url)
    selectors = _selectors(config, "message")
    timeout = int(config.get("timeout_ms") or 30000)
    playwright, browser, page = await _with_page(config)
    try:
        response = await page.goto(message_url, wait_until="domcontentloaded", timeout=timeout)
        _guard_response(response, "message send")
        await _guard_page(config, page)
        await _fill_if_present(page, selectors.get("message_box"), body, timeout)
        await _click_if_present(page, selectors.get("send"), timeout)
        await page.wait_for_load_state("domcontentloaded", timeout=timeout)
        await _guard_page(config, page)
        verify = await verify_chickpt_message_on_page(page, selectors, body, timeout)
        return BrowserWorkerResult(True, "Sent", "ChickPT message sent and read back on conversation page.", {"platform_message_id": verify, "conversation_id": conversation_id, "source_url": page.url, "selector_version": selectors.get("version")})
    finally:
        await browser.close()
        await playwright.stop()


async def verify_chickpt_message_on_page(page, selectors: dict, body: str, timeout: int) -> str:
    sent = page.locator(selectors.get("sent_message", "body")).filter(has_text=body)
    if not await sent.count():
        raise BrowserAutomationPaused("Sent message was not visible after submit; result is unconfirmed.")
    return (await sent.last.get_attribute(selectors.get("message_id_attribute") or "data-id")) or page.url


async def verify_chickpt_post(config: dict, platform_post_id: str) -> BrowserWorkerResult:
    post_url = platform_post_id if platform_post_id.startswith("http") else (config.get("verify_post_url_template") or "").format(platform_post_id=platform_post_id)
    _allowed(config, "verify_post", post_url)
    timeout = int(config.get("timeout_ms") or 30000)
    playwright, browser, page = await _with_page(config)
    try:
        response = await page.goto(post_url, wait_until="domcontentloaded", timeout=timeout)
        _guard_response(response, "post verification")
        await _guard_page(config, page)
        return BrowserWorkerResult(True, "Verified", "ChickPT post read back.", {"platform_post_id": platform_post_id, "post_url": page.url})
    finally:
        await browser.close()
        await playwright.stop()


async def verify_chickpt_message(config: dict, platform_message_id: str) -> BrowserWorkerResult:
    message_url = platform_message_id if platform_message_id.startswith("http") else (config.get("verify_message_url_template") or "").format(platform_message_id=platform_message_id)
    _allowed(config, "verify_message", message_url)
    timeout = int(config.get("timeout_ms") or 30000)
    playwright, browser, page = await _with_page(config)
    try:
        response = await page.goto(message_url, wait_until="domcontentloaded", timeout=timeout)
        _guard_response(response, "message verification")
        await _guard_page(config, page)
        return BrowserWorkerResult(True, "Verified", "ChickPT message read back.", {"platform_message_id": platform_message_id, "source_url": page.url})
    finally:
        await browser.close()
        await playwright.stop()


async def run_once() -> None:
    raise BrowserAutomationPaused("Use queue tasks with platform account config to run browser automation.")


if __name__ == "__main__":
    asyncio.run(run_once())
