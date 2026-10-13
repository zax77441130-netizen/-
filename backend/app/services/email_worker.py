from __future__ import annotations

import io
import imaplib
import re
from dataclasses import dataclass
from email import message_from_bytes
from email.message import Message as EmailMessage
from email.policy import default
from html import unescape
from pathlib import Path


@dataclass
class ParsedInboundEmail:
    message_id: str
    sender: str
    received_at: str
    subject: str
    raw_text: str
    attachment_names: list[str]
    imap_uid: str | None = None
    uid_validity: str | None = None
    mailbox: str | None = None


def _decode_payload(part: EmailMessage) -> str:
    payload = part.get_payload(decode=True)
    if not payload:
        return ""
    charset = part.get_content_charset() or "utf-8"
    return payload.decode(charset, errors="replace")


def _html_to_text(raw: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", raw)
    text = re.sub(r'(?is)<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', lambda m: f"{m.group(2)} {m.group(1)}", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _extract_attachment_text(filename: str | None, content_type: str, payload: bytes) -> str:
    suffix = Path(filename or "").suffix.lower()
    if content_type == "text/html" or suffix in {".html", ".htm"}:
        return _html_to_text(payload.decode("utf-8", errors="replace"))
    if content_type.startswith("text/") or suffix == ".txt":
        return payload.decode("utf-8", errors="replace")
    if content_type == "application/pdf" or suffix == ".pdf":
        try:
            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(payload))
            return "\n".join(page.extract_text() or "" for page in reader.pages).strip()
        except Exception as exc:
            return f"[PDF attachment could not be parsed: {exc}]"
    if suffix == ".docx" or content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        try:
            from docx import Document

            document = Document(io.BytesIO(payload))
            return "\n".join(paragraph.text for paragraph in document.paragraphs).strip()
        except Exception as exc:
            return f"[Word attachment could not be parsed: {exc}]"
    return ""


def parse_email_bytes(raw: bytes) -> ParsedInboundEmail:
    msg = message_from_bytes(raw, policy=default)
    text_parts: list[str] = []
    attachment_names: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            disposition = part.get_content_disposition()
            filename = part.get_filename()
            if filename:
                attachment_names.append(filename)
            content_type = part.get_content_type()
            if disposition == "attachment":
                payload = part.get_payload(decode=True) or b""
                attachment_text = _extract_attachment_text(filename, content_type, payload)
                if attachment_text:
                    text_parts.append(f"[Attachment: {filename or content_type}]\n{attachment_text}")
                continue
            if content_type in {"text/plain", "text/html"}:
                decoded = _decode_payload(part)
                text_parts.append(_html_to_text(decoded) if content_type == "text/html" else decoded)
    else:
        text_parts.append(_decode_payload(msg))
    return ParsedInboundEmail(
        message_id=str(msg.get("Message-ID") or msg.get("X-Message-ID") or ""),
        sender=str(msg.get("From") or ""),
        received_at=str(msg.get("Date") or ""),
        subject=str(msg.get("Subject") or ""),
        raw_text="\n\n".join(part.strip() for part in text_parts if part.strip()),
        attachment_names=attachment_names,
    )


def parse_fixture_mailbox(path: str) -> list[ParsedInboundEmail]:
    fixture = Path(path)
    if fixture.is_file():
        return [parse_email_bytes(fixture.read_bytes())]
    return [parse_email_bytes(item.read_bytes()) for item in sorted(fixture.glob("*.eml"))]


def _connect_imap(config: dict):
    host = config.get("host")
    username = config.get("username")
    password = config.get("password")
    if not host or not username or not password:
        raise ValueError("IMAP polling requires host, username, and password.")
    client_cls = imaplib.IMAP4_SSL if config.get("ssl", True) else imaplib.IMAP4
    client = client_cls(host, int(config.get("port") or (993 if config.get("ssl", True) else 143)))
    client.login(username, password)
    status, data = client.select(config.get("mailbox", "INBOX"))
    if status != "OK":
        raise RuntimeError(f"IMAP select failed for mailbox {config.get('mailbox', 'INBOX')}: {data}")
    return client


def _uid_validity(client) -> str:
    status, validity = client.response("UIDVALIDITY")
    return (validity[0].decode() if status == "OK" and validity else None) or "unknown"


def poll_imap(config: dict, limit: int = 20, mark_seen: bool = False, min_uid: int | None = None) -> list[ParsedInboundEmail]:
    with _connect_imap(config) as client:
        uid_validity = _uid_validity(client)
        if config.get("uid_validity") and str(config["uid_validity"]) != uid_validity:
            min_uid = 0
        search = config.get("search") or (f"UID {max(int(min_uid or 0) + 1, 1)}:*")
        status, data = client.uid("search", None, search)
        if status != "OK":
            raise RuntimeError(f"IMAP UID search failed: {data}")
        ids = sorted((data[0] or b"").split(), key=lambda item: int(item))[:limit]
        parsed: list[ParsedInboundEmail] = []
        for uid in ids:
            status, body = client.uid("fetch", uid, "(BODY.PEEK[])")
            if status != "OK":
                raise RuntimeError(f"IMAP fetch failed for UID {uid.decode()}: {body}")
            for item in body:
                if isinstance(item, tuple):
                    message = parse_email_bytes(item[1])
                    message.imap_uid = uid.decode()
                    message.uid_validity = uid_validity
                    message.mailbox = config.get("mailbox", "INBOX")
                    parsed.append(message)
            if mark_seen:
                status, data = client.uid("store", uid, "+FLAGS", "\\Seen")
                if status != "OK":
                    raise RuntimeError(f"IMAP mark-seen failed for UID {uid.decode()}: {data}")
        return parsed


def acknowledge_imap_messages(config: dict, uids: list[str], uid_validity: str | None = None) -> None:
    if not uids:
        return
    with _connect_imap(config) as client:
        current_uid_validity = _uid_validity(client)
        if uid_validity and current_uid_validity != uid_validity:
            raise RuntimeError(f"IMAP UIDVALIDITY changed before ACK: expected {uid_validity}, got {current_uid_validity}.")
        for uid in uids:
            status, data = client.uid("store", str(uid).encode(), "+FLAGS", "\\Seen")
            if status != "OK":
                raise RuntimeError(f"IMAP ACK failed for UID {uid}: {data}")
