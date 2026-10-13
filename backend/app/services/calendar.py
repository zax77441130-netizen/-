from __future__ import annotations

import hashlib
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.models.domain import CalendarBooking, Interview, SystemError


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(hour=int(hour), minute=int(minute))


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def calendar_policy(config: dict | None) -> dict:
    policy = dict(config or {})
    policy.setdefault("provider", "mock")
    policy.setdefault("workdays", [0, 1, 2, 3, 4])
    policy.setdefault("work_start", "09:00")
    policy.setdefault("work_end", "18:00")
    policy.setdefault("slot_minutes", 60)
    policy.setdefault("interview_minutes", 60)
    policy.setdefault("required_interviewer_count", 2)
    policy.setdefault("timezone", "Asia/Taipei")
    policy.setdefault("busy", {})
    policy.setdefault("candidate_available", [])
    if int(policy["slot_minutes"]) <= 0 or int(policy["interview_minutes"]) <= 0:
        raise ValueError("slot_minutes and interview_minutes must be positive.")
    if int(policy.get("search_days") or 21) > 60:
        raise ValueError("calendar search_days may not exceed 60.")
    return policy


def find_available_slot(policy: dict, *, after: datetime | None = None) -> tuple[datetime, datetime] | None:
    zone = ZoneInfo(policy["timezone"])
    current = (after or datetime.now(UTC)).astimezone(zone)
    current = current.replace(minute=0, second=0, microsecond=0)
    if current < datetime.now(zone):
        current = datetime.now(zone).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    work_start = _parse_hhmm(policy["work_start"])
    work_end = _parse_hhmm(policy["work_end"])
    slot_minutes = int(policy["slot_minutes"])
    interview_minutes = int(policy["interview_minutes"])
    busy = policy.get("busy") or {}
    for day_offset in range(0, int(policy.get("search_days") or 21)):
        day = (current + timedelta(days=day_offset)).date()
        if day.weekday() not in set(int(item) for item in policy["workdays"]):
            continue
        slot = datetime.combine(day, work_start, tzinfo=zone)
        day_end = datetime.combine(day, work_end, tzinfo=zone)
        while slot + timedelta(minutes=interview_minutes) <= day_end:
            end = slot + timedelta(minutes=interview_minutes)
            if slot >= current and _candidate_available(policy, slot, end) and _interviewers_available(policy, busy, slot, end):
                return slot, end
            slot += timedelta(minutes=slot_minutes)
    return None


def _candidate_available(policy: dict, slot: datetime, end: datetime) -> bool:
    windows = policy.get("candidate_available") or []
    if not windows:
        return True
    return any(_contains(slot, end, item, policy["timezone"]) for item in windows)


def _contains(slot: datetime, end: datetime, item, timezone: str) -> bool:
    zone = ZoneInfo(timezone)
    start = _parse_calendar_time(item.get("start"), zone)
    available_end = _parse_calendar_time(item.get("end"), zone)
    return slot >= start.astimezone(slot.tzinfo) and end <= available_end.astimezone(slot.tzinfo)


def _parse_calendar_time(value, default_zone: ZoneInfo) -> datetime:
    if isinstance(value, dict):
        raw = value.get("dateTime") or value.get("date")
        zone_name = value.get("timeZone")
    else:
        raw = value
        zone_name = None
    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(zone_name) if zone_name else default_zone)
    return parsed


def _interviewers_available(policy: dict, busy: dict, slot: datetime, end: datetime) -> bool:
    interviewers = policy.get("interviewer_ids") or []
    if len(interviewers) < int(policy.get("required_interviewer_count") or 2):
        return False
    available = 0
    for interviewer in interviewers:
        blocked = busy.get(str(interviewer), [])
        if not any(_overlaps(slot, end, item, policy["timezone"]) for item in blocked):
            available += 1
    return available >= int(policy.get("required_interviewer_count") or 2)


def _overlaps(slot: datetime, end: datetime, item, timezone: str) -> bool:
    zone = ZoneInfo(timezone)
    if isinstance(item, str):
        start = datetime.fromisoformat(item.replace("Z", "+00:00"))
        busy_end = start + timedelta(minutes=60)
    else:
        start = _parse_calendar_time(item.get("start"), zone)
        busy_end = _parse_calendar_time(item.get("end"), zone)
    if start.tzinfo is None:
        start = start.replace(tzinfo=zone)
    if busy_end.tzinfo is None:
        busy_end = busy_end.replace(tzinfo=zone)
    return slot < busy_end.astimezone(slot.tzinfo) and end > start.astimezone(slot.tzinfo)


class CalendarAdapter:
    def __init__(self, config: dict):
        self.config = config

    def free_busy(self, interviewer_ids: list[str], start: datetime, end: datetime) -> dict[str, list[dict]]:
        return {}

    def create_event(self, *, interview: Interview, interviewer_ids: list[str], start: datetime, end: datetime, timezone: str) -> dict:
        raise NotImplementedError

    def read_back_event(self, external_event_id: str) -> dict | None:
        raise NotImplementedError


class MockCalendarAdapter(CalendarAdapter):
    def free_busy(self, interviewer_ids: list[str], start: datetime, end: datetime) -> dict[str, list[dict]]:
        return self.config.get("busy") or {}

    def create_event(self, *, interview: Interview, interviewer_ids: list[str], start: datetime, end: datetime, timezone: str) -> dict:
        event_id = "mock-event:" + hashlib.sha256(f"{interview.id}:{start.isoformat()}:{interviewer_ids}".encode("utf-8")).hexdigest()[-16:]
        event = {"id": event_id, "status": "confirmed", "start": _iso(start), "end": _iso(end), "attendees": [{"email": item} for item in interviewer_ids if "@" in item], "mock": True}
        self.config["_mock_event"] = event
        return event

    def read_back_event(self, external_event_id: str) -> dict | None:
        if self.config.get("mock_read_back_missing"):
            return None
        return self.config.get("_mock_event") or {"id": external_event_id, "status": "confirmed", "mock": True}


class HttpCalendarAdapter(CalendarAdapter):
    @property
    def token(self) -> str | None:
        return self.config.get("access_token") or self.config.get("oauth_token")

    def _headers(self) -> dict:
        if not self.token:
            raise RuntimeError("Calendar OAuth access token is not configured.")
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}


class GoogleCalendarAdapter(HttpCalendarAdapter):
    def free_busy(self, interviewer_ids: list[str], start: datetime, end: datetime) -> dict[str, list[dict]]:
        body = {"timeMin": _iso(start), "timeMax": _iso(end), "items": [{"id": item} for item in interviewer_ids]}
        response = httpx.post("https://www.googleapis.com/calendar/v3/freeBusy", json=body, headers=self._headers(), timeout=float(self.config.get("timeout_seconds") or 20))
        response.raise_for_status()
        data = response.json()
        if data.get("groups"):
            failed_groups = {key: value.get("errors") for key, value in data["groups"].items() if value.get("errors")}
            if failed_groups:
                raise RuntimeError(f"Google free/busy group errors: {failed_groups}")
        calendars = data.get("calendars") or {}
        failed = {key: value.get("errors") for key, value in calendars.items() if value.get("errors")}
        if failed:
            raise RuntimeError(f"Google free/busy calendar errors: {failed}")
        missing = set(interviewer_ids) - set(calendars.keys())
        if missing:
            raise RuntimeError(f"Google free/busy did not return availability for: {sorted(missing)}")
        return {key: value.get("busy") or [] for key, value in calendars.items()}

    def create_event(self, *, interview: Interview, interviewer_ids: list[str], start: datetime, end: datetime, timezone: str) -> dict:
        calendar_id = self.config.get("calendar_id") or "primary"
        body = {
            "summary": self.config.get("summary") or f"Interview #{interview.id}",
            "start": {"dateTime": start.astimezone(ZoneInfo(timezone)).isoformat(), "timeZone": timezone},
            "end": {"dateTime": end.astimezone(ZoneInfo(timezone)).isoformat(), "timeZone": timezone},
            "attendees": [{"email": item} for item in interviewer_ids if "@" in item],
        }
        response = httpx.post(f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events", json=body, headers=self._headers(), timeout=float(self.config.get("timeout_seconds") or 20))
        response.raise_for_status()
        return response.json()

    def read_back_event(self, external_event_id: str) -> dict | None:
        calendar_id = self.config.get("calendar_id") or "primary"
        response = httpx.get(f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{external_event_id}", headers=self._headers(), timeout=float(self.config.get("timeout_seconds") or 20))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()


class MicrosoftGraphCalendarAdapter(HttpCalendarAdapter):
    def free_busy(self, interviewer_ids: list[str], start: datetime, end: datetime) -> dict[str, list[dict]]:
        body = {
            "schedules": interviewer_ids,
            "startTime": {"dateTime": start.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="seconds"), "timeZone": "UTC"},
            "endTime": {"dateTime": end.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="seconds"), "timeZone": "UTC"},
            "availabilityViewInterval": int(self.config.get("availability_view_interval") or 60),
        }
        response = httpx.post("https://graph.microsoft.com/v1.0/me/calendar/getSchedule", json=body, headers=self._headers(), timeout=float(self.config.get("timeout_seconds") or 20))
        response.raise_for_status()
        items = response.json().get("value") or []
        by_schedule = {item.get("scheduleId"): item for item in items}
        missing = set(interviewer_ids) - set(by_schedule.keys())
        if missing:
            raise RuntimeError(f"Microsoft Graph free/busy did not return availability for: {sorted(missing)}")
        busy: dict[str, list[dict]] = {}
        for schedule_id, item in by_schedule.items():
            if item.get("error"):
                raise RuntimeError(f"Microsoft Graph free/busy error for {schedule_id}: {item['error']}")
            busy[schedule_id] = [
                {"start": entry.get("start", {}), "end": entry.get("end", {})}
                for entry in item.get("scheduleItems") or []
                if str(entry.get("status") or "").lower() in {"busy", "tentative", "oof", "workingelsewhere", "unknown"}
            ]
        return busy

    def create_event(self, *, interview: Interview, interviewer_ids: list[str], start: datetime, end: datetime, timezone: str) -> dict:
        body = {
            "subject": self.config.get("summary") or f"Interview #{interview.id}",
            "start": {"dateTime": start.astimezone(ZoneInfo(timezone)).replace(tzinfo=None).isoformat(timespec="seconds"), "timeZone": timezone},
            "end": {"dateTime": end.astimezone(ZoneInfo(timezone)).replace(tzinfo=None).isoformat(timespec="seconds"), "timeZone": timezone},
            "attendees": [{"emailAddress": {"address": item}, "type": "required"} for item in interviewer_ids if "@" in item],
        }
        response = httpx.post("https://graph.microsoft.com/v1.0/me/events", json=body, headers=self._headers(), timeout=float(self.config.get("timeout_seconds") or 20))
        response.raise_for_status()
        return response.json()

    def read_back_event(self, external_event_id: str) -> dict | None:
        response = httpx.get(f"https://graph.microsoft.com/v1.0/me/events/{external_event_id}", headers=self._headers(), timeout=float(self.config.get("timeout_seconds") or 20))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()


def get_calendar_adapter(policy: dict) -> CalendarAdapter:
    provider = str(policy.get("provider") or "mock").lower()
    if provider == "mock":
        return MockCalendarAdapter(policy)
    if provider in {"google", "google_calendar"}:
        return GoogleCalendarAdapter(policy)
    if provider in {"microsoft", "microsoft_graph", "outlook", "graph"}:
        return MicrosoftGraphCalendarAdapter(policy)
    raise RuntimeError(f"Unsupported calendar provider: {provider}")


def _read_back_matches(read_back: dict | None, external_event_id: str | None, *, start: datetime | None = None, end: datetime | None = None, interviewer_ids: list[str] | None = None) -> bool:
    if not read_back or not external_event_id:
        return False
    read_back_id = str(read_back.get("id") or read_back.get("iCalUId") or "")
    if read_back.get("isCancelled") is True:
        return False
    if read_back_id != str(external_event_id) or str(read_back.get("status") or "").lower() in {"cancelled", "canceled", "deleted"}:
        return False
    if start and end:
        raw_start = read_back.get("start")
        raw_end = read_back.get("end")
        rb_start = raw_start.get("dateTime") if isinstance(raw_start, dict) else raw_start
        rb_end = raw_end.get("dateTime") if isinstance(raw_end, dict) else raw_end
        if not rb_start or not rb_end:
            return False
        start_zone = raw_start.get("timeZone") if isinstance(raw_start, dict) else None
        end_zone = raw_end.get("timeZone") if isinstance(raw_end, dict) else None
        parsed_start = datetime.fromisoformat(str(rb_start).replace("Z", "+00:00"))
        parsed_end = datetime.fromisoformat(str(rb_end).replace("Z", "+00:00"))
        if parsed_start.tzinfo is None:
            if not start_zone:
                return False
            parsed_start = parsed_start.replace(tzinfo=ZoneInfo(start_zone))
        if parsed_end.tzinfo is None:
            if not end_zone:
                return False
            parsed_end = parsed_end.replace(tzinfo=ZoneInfo(end_zone))
        parsed_start = parsed_start.astimezone(UTC)
        parsed_end = parsed_end.astimezone(UTC)
        if parsed_start != start.astimezone(UTC) or parsed_end != end.astimezone(UTC):
            return False
    if interviewer_ids:
        attendees = read_back.get("attendees") or []
        returned = {
            str((item.get("emailAddress") or {}).get("address") or item.get("email") or "").lower()
            for item in attendees
            if isinstance(item, dict)
        }
        required = {item.lower() for item in interviewer_ids if "@" in item}
        if required and not required.issubset(returned):
            return False
    return True


def book_interview_calendar(db: Session, interview: Interview, config: dict | None = None) -> CalendarBooking:
    policy = calendar_policy(config)
    adapter = get_calendar_adapter(policy)
    key_basis = f"{interview.id}:{policy['provider']}:{policy.get('interviewer_ids')}"
    idempotency_key = "calendar-book:" + hashlib.sha256(key_basis.encode("utf-8")).hexdigest()
    existing = db.scalar(select(CalendarBooking).where(CalendarBooking.idempotency_key == idempotency_key))
    if existing:
        if existing.status == "Unverified" and existing.external_event_id and existing.starts_at and existing.ends_at:
            try:
                rb_start = datetime.fromisoformat(existing.starts_at.replace("Z", "+00:00"))
                rb_end = datetime.fromisoformat(existing.ends_at.replace("Z", "+00:00"))
                read_back = adapter.read_back_event(existing.external_event_id)
                if _read_back_matches(read_back, existing.external_event_id, start=rb_start, end=rb_end, interviewer_ids=[str(item) for item in (existing.interviewer_ids or [])]):
                    existing.status = "Booked"
                    existing.read_back = {"provider": policy["provider"], "verified": True, "event": read_back, "starts_at": existing.starts_at, "ends_at": existing.ends_at}
            except Exception as exc:
                existing.failure_reason = f"Existing unverified booking could not be recovered by read-back: {exc}"
        return existing
    search_start = datetime.now(UTC)
    search_end = search_start + timedelta(days=int(policy.get("search_days") or 21))
    try:
        policy["busy"] = {**(policy.get("busy") or {}), **(adapter.free_busy([str(item) for item in (policy.get("interviewer_ids") or [])], search_start, search_end) or {})}
    except Exception as exc:
        policy["busy_error"] = str(exc)
    slot = find_available_slot(policy)
    booking = CalendarBooking(
        interview_id=interview.id,
        candidate_id=interview.candidate_id,
        job_id=interview.job_id,
        provider=policy["provider"],
        interviewer_ids=policy.get("interviewer_ids") or [],
        timezone=policy["timezone"],
        idempotency_key=idempotency_key,
    )
    db.add(booking)
    db.flush()
    if policy.get("busy_error"):
        booking.status = "Manual Review Required"
        booking.failure_reason = f"Calendar free/busy failed: {policy['busy_error']}"
        db.add(SystemError(service="Calendar", feature="calendar.free_busy", severity="Warning", message=booking.failure_reason, impact="HR must verify availability manually."))
        return booking
    if not slot:
        booking.status = "Manual Review Required"
        booking.failure_reason = "No calendar slot matched the configured workdays, hourly slots, candidate availability and two-interviewer availability rule."
        db.add(SystemError(service="Calendar", feature="calendar.book_interview", severity="Warning", message=booking.failure_reason, impact="HR must schedule manually or update interviewer availability."))
        return booking
    slot_start, slot_end = slot
    starts_at = _iso(slot_start)
    ends_at = _iso(slot_end)
    conflicts = db.execute(select(CalendarBooking).where(CalendarBooking.status.in_(["Booked", "Unverified"]))).scalars().all()
    conflict = next(
        (
            item
            for item in conflicts
            if set(item.interviewer_ids or []).intersection(set(policy.get("interviewer_ids") or []))
            and item.starts_at
            and item.ends_at
            and _overlaps(slot_start, slot_end, {"start": item.starts_at, "end": item.ends_at}, policy["timezone"])
        ),
        None,
    )
    if conflict:
        booking.status = "Manual Review Required"
        booking.failure_reason = "Calendar slot overlaps an existing held or booked interval for at least one interviewer."
        return booking
    booking.starts_at = starts_at
    booking.ends_at = ends_at
    booking.hold_token = "hold:" + hashlib.sha256(f"{idempotency_key}:{starts_at}".encode("utf-8")).hexdigest()
    try:
        created = adapter.create_event(interview=interview, interviewer_ids=[str(item) for item in (policy.get("interviewer_ids") or [])], start=slot_start, end=slot_end, timezone=policy["timezone"])
        booking.external_event_id = created.get("id")
        read_back = adapter.read_back_event(booking.external_event_id) if booking.external_event_id else None
    except Exception as exc:
        booking.status = "Unverified"
        booking.failure_reason = f"Calendar provider did not complete verified booking: {exc}"
        booking.read_back = {"provider": policy["provider"], "verified": False, "error": str(exc), "starts_at": starts_at, "ends_at": ends_at, "local_starts_at": slot_start.isoformat(timespec="minutes")}
        db.add(SystemError(service="Calendar", feature="calendar.book_interview", severity="Warning", message=booking.failure_reason, impact="HR must verify or schedule manually before telling the candidate the interview is booked."))
        return booking
    if _read_back_matches(read_back, booking.external_event_id, start=slot_start, end=slot_end, interviewer_ids=[str(item) for item in (policy.get("interviewer_ids") or [])]):
        booking.status = "Booked"
        booking.read_back = {"provider": policy["provider"], "verified": True, "mock": policy["provider"] == "mock", "event": read_back, "starts_at": starts_at, "ends_at": ends_at, "local_starts_at": slot_start.isoformat(timespec="minutes")}
    else:
        booking.status = "Unverified"
        booking.failure_reason = "Calendar provider create response was not confirmed by read-back."
        booking.read_back = {"provider": policy["provider"], "verified": False, "event": read_back, "starts_at": starts_at, "ends_at": ends_at, "local_starts_at": slot_start.isoformat(timespec="minutes")}
    if booking.status == "Booked":
        interview.interview_date = slot_start.date().isoformat()
        interview.interview_time = slot_start.strftime("%H:%M")
        interview.timezone = policy["timezone"]
        interview.status = "Scheduled"
    return booking
