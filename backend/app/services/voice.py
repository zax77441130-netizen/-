from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import StrEnum


TERMINAL_FAILURES = {"No Answer", "Rejected", "Failed", "no_answer", "rejected", "failed", "Low Confidence", "Human Review Required"}
SUCCESS_STATUSES = {"Completed", "completed"}
DISCLOSURE_CONFIRMED_KEYS = {"disclosure_played", "disclosure_completed", "ai_disclosure_played", "ai_disclosure_completed"}


class VoiceCallState(StrEnum):
    CREATED = "Created"
    DISCLOSURE_PENDING = "Disclosure Pending"
    DISCLOSURE_CONFIRMED = "Disclosure Confirmed"
    LISTENING = "Listening"
    TRANSCRIBING = "Transcribing"
    CLASSIFYING_TEXT = "Classifying Text"
    HUMAN_REVIEW = "Human Review Required"
    COMPLETED = "Completed"


@dataclass
class VoiceDecision:
    intent: str
    confidence: float
    answers: dict
    risk_flags: list[str]
    next_state: str


def synthesize_disclosure(script: str, company_name: str | None = None) -> str:
    company = company_name or "the recruiting team"
    return (
        f"This is an automated recruiting call from {company}. "
        "The call may use AI transcription and response assistance. "
        "You may ask for a human recruiter at any time. "
        f"{script}"
    )


def disclosure_was_confirmed(payload: dict | None) -> bool:
    data = payload or {}
    if any(data.get(key) is True for key in DISCLOSURE_CONFIRMED_KEYS):
        return True
    events = data.get("events") or data.get("playback_events") or []
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            name = str(event.get("name") or event.get("type") or "").lower()
            status = str(event.get("status") or event.get("result") or "").lower()
            if "disclosure" in name and status in {"played", "completed", "done", "success"}:
                return True
    return False


def voice_agent_state(*, provider_accepted: bool, disclosure_confirmed: bool, transcript: str | None, decision: VoiceDecision | None = None) -> VoiceCallState:
    if not provider_accepted:
        return VoiceCallState.CREATED
    if not disclosure_confirmed:
        return VoiceCallState.DISCLOSURE_PENDING
    if not transcript:
        return VoiceCallState.LISTENING
    if decision is None:
        return VoiceCallState.TRANSCRIBING
    if decision.next_state == "Completed" and decision.confidence >= 0.8 and not decision.risk_flags:
        return VoiceCallState.COMPLETED
    return VoiceCallState.HUMAN_REVIEW


def classify_voice_transcript(transcript: str | None) -> VoiceDecision:
    text = (transcript or "").strip().lower()
    if not text:
        return VoiceDecision("unclear", 0.0, {}, ["empty_transcript"], "Human Review Required")
    compact = re.sub(r"\s+", " ", text)
    explicit_negations = [
        "不確認",
        "沒有確認",
        "無法確認",
        "不能確認",
        "i am unavailable",
        "i'm unavailable",
        "i do not confirm",
        "i don't confirm",
        "do not confirm",
        "don't confirm",
    ]
    negates_positive = re.search(r"\b(no|not|never|cannot|can't|unavailable|decline)\b.{0,24}\b(confirm|available|interested|yes)\b", compact)
    positive_then_negated = re.search(r"\b(confirm|available|interested)\b.{0,24}\b(no|not|never|cannot|can't|unavailable)\b", compact)
    if positive_then_negated and re.search(r"\bconfirm\b.{0,16}\bno problem\b", compact):
        positive_then_negated = None
    if any(term in compact for term in explicit_negations) or negates_positive or positive_then_negated:
        return VoiceDecision("decline_or_unavailable", 0.9, {}, ["negative_or_contradictory"], "Human Review Required")

    positive_phrases = [
        "yes no problem",
        "yes, no problem",
        "no problem",
        "yes i can",
        "yes available",
        "confirm",
        "confirmed",
        "available",
        "可以",
        "沒問題",
        "可配合",
        "確認",
    ]
    hard_negative_phrases = [
        "not available",
        "cannot confirm",
        "can't confirm",
        "i cannot confirm",
        "i have no time",
        "i don't have time",
        "no time",
        "not free",
        "cannot",
        "can't",
        "not interested",
        "\u4e0d\u9858\u610f\u9762\u8a66",
        "\u4e0d\u9858\u610f",
        "do not call",
        "don't call",
        "沒有空",
        "沒空",
        "不可以",
        "尚未確認",
        "不太可以",
        "不要聯絡",
        "請勿聯絡",
    ]
    uncertain_terms = ["maybe", "not sure", "unclear", "possibly", "可能", "不確定", "再看看"]

    bare_no = re.search(r"(^|[^\w])no([^\w]|$)", text) is not None
    if any(term in text for term in hard_negative_phrases) or (bare_no and not any(term in text for term in positive_phrases)):
        risk_flags = ["dnc_request"] if any(term in text for term in ["do not call", "don't call", "不要聯絡", "請勿聯絡"]) else ["negative_or_contradictory"]
        intent = "do_not_call" if "dnc_request" in risk_flags else "decline_or_unavailable"
        return VoiceDecision(intent, 0.9, {}, risk_flags, "Human Review Required")

    if any(term in text for term in uncertain_terms):
        return VoiceDecision("unclear", 0.45, {}, ["low_confidence"], "Human Review Required")
    if any(term in text for term in ["human", "recruiter", "person", "真人", "專員", "人工"]):
        return VoiceDecision("human_transfer", 0.95, {}, [], "Human Review Required")
    if any(term in text for term in positive_phrases) or re.search(r"(^|[^\w])yes([^\w]|$)", text):
        return VoiceDecision("confirm_interest", 0.86, {"availability": transcript}, [], "Completed")
    return VoiceDecision("candidate_reply", 0.65, {"transcript": transcript}, ["needs_review"], "Human Review Required")


def callback_body_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()
