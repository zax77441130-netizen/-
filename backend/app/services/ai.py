from __future__ import annotations

import asyncio
import json
import math
import re
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.app.core.config import get_settings
from backend.app.models.domain import AIProvider, AIRequest, Job, SystemError, TaskModelRoute
from backend.app.services.security import decrypt_secret


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 3)


def mask_sensitive_text(text: str | None) -> str:
    if not text:
        return ""
    masked = re.sub(r"09\d{8}", "[phone]", text)
    masked = re.sub(r"[\w.\-+%]+@[\w.\-]+\.[A-Za-z]{2,}", "[email]", masked)
    masked = re.sub(r"(?i)(phone|mobile|tel|email|address|id number|national id)\s*[:：]\s*\S+", r"\1: [redacted]", masked)
    return masked


def mask_provider_error(text: str | None, api_key: str | None = None) -> str:
    """Return provider diagnostics without leaking bearer tokens or API keys."""
    masked = text or ""
    if api_key:
        masked = masked.replace(api_key, "[REDACTED_API_KEY]")
    masked = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+\-/=]+", r"\1[REDACTED_API_KEY]", masked)
    masked = re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "[REDACTED_API_KEY]", masked)
    return masked[:4000]


def _price_for(provider: str, model: str) -> dict[str, float]:
    prices = get_settings().model_prices
    return prices.get(f"{provider}:{model}") or prices.get(model) or prices.get(provider) or {}


def calculate_cost(provider: str, model: str, input_tokens: int, output_tokens: int) -> float:
    price = _price_for(provider, model)
    input_per_m = float(price.get("input_per_million", 0) or 0)
    output_per_m = float(price.get("output_per_million", 0) or 0)
    return round((input_tokens / 1_000_000 * input_per_m) + (output_tokens / 1_000_000 * output_per_m), 8)


def _provider_type(provider: AIProvider) -> str:
    return (provider.provider_type or "openai_compatible").strip().lower().replace("-", "_")


def _route_providers(db: Session, task_name: str) -> tuple[TaskModelRoute | None, list[AIProvider]]:
    route = db.scalar(select(TaskModelRoute).where(TaskModelRoute.task_name == task_name))
    providers: list[AIProvider] = []
    seen: set[int] = set()
    if route and route.primary_provider_id:
        primary = db.get(AIProvider, route.primary_provider_id)
        if primary:
            providers.append(primary)
            seen.add(primary.id)
    if route:
        for provider_id in route.fallback_provider_ids or []:
            provider = db.get(AIProvider, provider_id)
            if provider and provider.id not in seen:
                providers.append(provider)
                seen.add(provider.id)
    return route, providers


def _budget_allowed(db: Session, route: TaskModelRoute | None) -> tuple[bool, str | None]:
    if not route:
        return True, None
    policy = route.budget_policy or {}
    today = datetime.now(UTC).date()
    today_start = datetime(today.year, today.month, today.day)
    month_start = datetime(today.year, today.month, 1)
    today_tokens = db.scalar(select(func.sum(AIRequest.total_tokens)).where(AIRequest.feature == route.task_name, AIRequest.created_at >= today_start)) or 0
    month_tokens = db.scalar(select(func.sum(AIRequest.total_tokens)).where(AIRequest.feature == route.task_name, AIRequest.created_at >= month_start)) or 0
    if policy.get("daily_token_budget") is not None and today_tokens >= int(policy["daily_token_budget"]):
        return False, f"Daily token budget exhausted for {route.task_name}."
    if policy.get("monthly_token_budget") is not None and month_tokens >= int(policy["monthly_token_budget"]):
        return False, f"Monthly token budget exhausted for {route.task_name}."
    return True, None


def _request_budget_allowed(route: TaskModelRoute | None, prompt: str) -> tuple[bool, str | None]:
    if not route:
        return True, None
    policy = route.budget_policy or {}
    estimated_input = estimate_tokens(prompt)
    if policy.get("max_input_tokens") is not None and estimated_input > int(policy["max_input_tokens"]):
        return False, f"Estimated input tokens exceed max_input_tokens for {route.task_name}."
    if policy.get("max_output_tokens") is not None and int(policy["max_output_tokens"]) <= 0:
        return False, f"max_output_tokens must be positive for {route.task_name}."
    return True, None


def _validate_json_schema(output: str, schema: dict | None) -> None:
    if not schema:
        return
    try:
        decoded = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI output is not valid JSON: {exc}") from exc
    _validate_schema_value(decoded, schema, path="$")


def _validate_schema_value(value: Any, schema: dict, *, path: str) -> None:
    expected = schema.get("type")
    if expected == "object":
        if not isinstance(value, dict):
            raise ValueError(f"AI JSON {path} must be an object.")
        missing = [key for key in (schema.get("required") or []) if key not in value]
        if missing:
            raise ValueError(f"AI JSON {path} missing required key(s): {', '.join(missing)}")
        for key, rules in (schema.get("properties") or {}).items():
            if key in value and isinstance(rules, dict):
                _validate_schema_value(value[key], rules, path=f"{path}.{key}")
        return
    if expected == "array":
        if not isinstance(value, list):
            raise ValueError(f"AI JSON {path} must be an array.")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(value):
                _validate_schema_value(item, item_schema, path=f"{path}[{index}]")
        return
    if expected == "string" and not isinstance(value, str):
        raise ValueError(f"AI JSON {path} must be a string.")
    if expected == "boolean" and not isinstance(value, bool):
        raise ValueError(f"AI JSON {path} must be a boolean.")
    if expected in {"number", "integer"}:
        valid_number = isinstance(value, (int, float)) and not isinstance(value, bool)
        if expected == "integer":
            valid_number = isinstance(value, int) and not isinstance(value, bool)
        if not valid_number:
            raise ValueError(f"AI JSON {path} must be a {expected}.")
        if not math.isfinite(float(value)):
            raise ValueError(f"AI JSON {path} must be finite.")
        if "minimum" in schema and value < schema["minimum"]:
            raise ValueError(f"AI JSON {path} must be >= {schema['minimum']}.")
        if "maximum" in schema and value > schema["maximum"]:
            raise ValueError(f"AI JSON {path} must be <= {schema['maximum']}.")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"AI JSON {path} must be one of: {', '.join(map(str, schema['enum']))}.")


def _circuit_open(provider: AIProvider) -> bool:
    open_until = provider.circuit_open_until
    if not open_until:
        return False
    try:
        return datetime.fromisoformat(open_until.replace("Z", "+00:00")) > datetime.now(UTC)
    except ValueError:
        return False


def _mark_provider_failure(provider: AIProvider, exc: Exception) -> None:
    provider.last_status = "Error"
    provider.consecutive_failures = int(provider.consecutive_failures or 0) + 1
    threshold = int((provider.retry_policy or {}).get("circuit_breaker_failures") or 3)
    pause_seconds = int((provider.retry_policy or {}).get("circuit_breaker_seconds") or 300)
    if provider.consecutive_failures >= threshold:
        from datetime import timedelta

        provider.circuit_open_until = (datetime.now(UTC) + timedelta(seconds=pause_seconds)).isoformat(timespec="seconds").replace("+00:00", "Z")
    provider.last_tested_at = now_iso()


def _mark_provider_success(provider: AIProvider, started: float) -> None:
    provider.last_status = "Connected"
    provider.last_latency_ms = int((perf_counter() - started) * 1000)
    provider.last_tested_at = now_iso()
    provider.consecutive_failures = 0
    provider.circuit_open_until = None


def record_ai_request(
    db: Session,
    *,
    provider: str,
    model: str,
    feature: str,
    prompt: str,
    output: str | None,
    started: float,
    status: str,
    candidate_id: int | None = None,
    job_id: int | None = None,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cached_tokens: int = 0,
    error: str | None = None,
    usage_source: str = "Estimated",
) -> AIRequest:
    estimated_input = estimate_tokens(prompt)
    estimated_output = estimate_tokens(output or "")
    actual_input = input_tokens if input_tokens is not None else estimated_input
    actual_output = output_tokens if output_tokens is not None else estimated_output
    request = AIRequest(
        provider=provider,
        model=model,
        feature=feature,
        candidate_id=candidate_id,
        job_id=job_id,
        input_tokens=actual_input,
        output_tokens=actual_output,
        total_tokens=actual_input + actual_output,
        cached_tokens=cached_tokens,
        usage_source=usage_source,
        estimated_cost=calculate_cost(provider, model, actual_input, actual_output),
        latency_ms=int((perf_counter() - started) * 1000),
        status=status,
        error=error,
        prompt_masked=mask_sensitive_text(prompt)[:4000],
        response_masked=mask_sensitive_text(output)[:4000] if output else None,
    )
    db.add(request)
    return request


def _job_brief(job: Job) -> str:
    return (
        f"Title: {job.title}\n"
        f"Location: {job.location}\n"
        f"Area: {job.area}\n"
        f"Shift: {job.shift}\n"
        f"Hours: {job.hours}\n"
        f"Salary: {job.salary_text}\n"
        f"Headcount: {job.required_headcount}\n"
        f"Description: {job.description}\n"
        f"Requirements: {job.requirements}\n"
        f"Contact: {job.contact_name} / {job.contact_method}"
    )


def generate_recruitment_copy(db: Session, job: Job) -> dict:
    started = perf_counter()
    base = _job_brief(job)
    prompt = f"""Generate recruiting copy strictly from this approved job data.
Return JSON with keys: titles, facebook_long, facebook_short, recruit518, chickpt, line, dm.
Do not invent benefits, locations, salary, or requirements.

{base}
"""
    route, providers = _route_providers(db, "RecruitCopywriting")
    budget_ok, budget_error = _budget_allowed(db, route)
    request_ok, request_error = _request_budget_allowed(route, prompt)
    budget_ok = budget_ok and request_ok
    budget_error = budget_error or request_error
    if not budget_ok:
        db.add(SystemError(service="AI Router", feature="RecruitCopywriting", severity="Warning", message=budget_error or "Budget exhausted.", impact="Template fallback used; external provider call skipped."))
    for provider in providers if budget_ok else []:
        attempt_started = perf_counter()
        if not provider.enabled or _circuit_open(provider):
            continue
        try:
            output, usage = call_provider_sync(provider, prompt)
            _validate_json_schema(output, (route.budget_policy or {}).get("expected_json_schema") if route else None)
            parsed = parse_copy_json(output)
            result = {
                "job_id": job.id,
                "titles": parsed.get("titles") or [job.title],
                "facebook_long": parsed.get("facebook_long") or output,
                "facebook_short": parsed.get("facebook_short") or output[:240],
                "recruit518": parsed.get("recruit518") or output,
                "chickpt": parsed.get("chickpt") or output[:240],
                "line": parsed.get("line") or output[:400],
                "dm": parsed.get("dm") or output[:400],
                "ai_notice": f"Provider Mode: generated by {provider.name}/{provider.model}; HR review required before publishing.",
            }
            record_ai_request(
                db,
                provider=provider.name,
                model=provider.model,
                feature="RecruitCopywriting",
                job_id=job.id,
                prompt=prompt,
                output=output,
                started=started,
                status="success",
                input_tokens=usage.get("input_tokens"),
                output_tokens=usage.get("output_tokens"),
                cached_tokens=usage.get("cached_tokens", 0),
                usage_source="Actual" if usage.get("actual") else "Estimated",
            )
            _mark_provider_success(provider, attempt_started)
            db.commit()
            return result
        except Exception as exc:
            _mark_provider_failure(provider, exc)
            db.add(SystemError(service="AI Copywriting", feature="RecruitCopywriting", severity="Warning", message=str(exc), impact="Provider failed; template fallback used."))

    base_zh = "\n".join([
        f"職缺：{job.title}", f"工作地點：{job.location}",
        f"地區：{job.area}", f"班別：{job.shift}",
        f"工作時間：{job.hours}", f"薪資：{job.salary_text}",
        f"需求人數：{job.required_headcount}",
        f"工作內容：{job.description}", f"應徵條件：{job.requirements}",
        f"聯絡人：{job.contact_name}", f"聯絡方式：{job.contact_method}",
    ])
    result = {
        "job_id": job.id,
        "titles": [
            f"{job.area}｜{job.title}｜{job.shift}｜{job.salary_text}",
            f"{job.location}徵才｜{job.title}",
            f"{job.title}｜工作時間 {job.hours}",
            f"應徵{job.title}｜洽 {job.contact_name}",
        ],
        "facebook_long": f"【徵才資訊】\n{base_zh}\n\n有興趣歡迎留言或私訊詢問。面試安排與錄用結果由招募人員確認。",
        "facebook_short": f"【{job.area}徵才】{job.title}\n班別：{job.shift}\n時間：{job.hours}\n薪資：{job.salary_text}\n聯絡：{job.contact_name}／{job.contact_method}",
        "recruit518": f"{base_zh}\n\n應徵資料僅用於本次招募流程，錄用結果由招募人員確認。",
        "chickpt": f"{job.title}｜{job.location}｜{job.shift}｜{job.salary_text}｜{job.hours}",
        "line": f"【{job.area}徵才】{job.title}\n班別：{job.shift}\n薪資：{job.salary_text}\n時間：{job.hours}\n有意應徵請回覆偏好地區、班別、最快到職日期，以及是否同意後續聯繫。",
        "dm": f"您好，謝謝您對「{job.title}」有興趣！您可以詢問薪資、地點、班別與應徵流程，後續安排由招募人員確認。",
        "ai_notice": "內建模板產出，未使用外部 AI；發布或聯繫候選人前，請由 HR 審核。",
    }
    record_ai_request(
        db,
        provider="Local Template",
        model="rules-plus-template",
        feature="RecruitCopywriting",
        job_id=job.id,
        prompt=prompt,
        output=str(result),
        started=started,
        status="template",
        usage_source="Estimated",
    )
    db.commit()
    return result


def call_provider_sync(provider: AIProvider, prompt: str) -> tuple[str, dict[str, Any]]:
    base_url = (provider.base_url or "").rstrip("/")
    provider_type = _provider_type(provider)
    if provider_type == "local_template":
        return local_task_response("Generic", prompt, provider.name, provider.model), {"actual": False}
    if provider_type in {"ollama"} and not base_url:
        base_url = "http://localhost:11434"
    if not base_url:
        raise ValueError("Provider base_url is required.")
    api_key = decrypt_secret(provider.api_key_encrypted)
    with httpx.Client(timeout=provider.timeout_seconds) as client:
        return _dispatch_provider_sync(client, provider, base_url, api_key, prompt)


def _openai_headers(api_key: str | None) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _extract_openai(data: dict) -> tuple[str, dict[str, Any]]:
    choices = data.get("choices") or []
    if not choices:
        raise ValueError("Provider returned no choices.")
    output = (choices[0].get("message") or {}).get("content") or choices[0].get("text")
    if not output:
        raise ValueError("Provider returned an empty response.")
    usage = data.get("usage") or {}
    input_tokens = usage.get("prompt_tokens") or usage.get("input_tokens")
    output_tokens = usage.get("completion_tokens") or usage.get("output_tokens")
    cached_tokens = ((usage.get("prompt_tokens_details") or {}).get("cached_tokens") or usage.get("cached_tokens") or 0)
    return output, {"input_tokens": input_tokens, "output_tokens": output_tokens, "cached_tokens": cached_tokens, "actual": input_tokens is not None or output_tokens is not None}


def _dispatch_provider_sync(client: httpx.Client, provider: AIProvider, base_url: str, api_key: str | None, prompt: str) -> tuple[str, dict[str, Any]]:
    provider_type = _provider_type(provider)
    if provider_type in {"openai", "openai_compatible", "vllm", "lm_studio"}:
        endpoint = provider.endpoint or "/chat/completions"
        response = client.post(
            f"{base_url}{endpoint if endpoint.startswith('/') else '/' + endpoint}",
            json={"model": provider.model, "messages": [{"role": "user", "content": prompt}], "temperature": provider.temperature, "max_tokens": provider.max_output},
            headers=_openai_headers(api_key),
        )
        response.raise_for_status()
        return _extract_openai(response.json())
    if provider_type == "anthropic":
        headers = {"Content-Type": "application/json", "anthropic-version": "2023-06-01"}
        if api_key:
            headers["x-api-key"] = api_key
        endpoint = provider.endpoint or "/v1/messages"
        response = client.post(
            f"{base_url}{endpoint if endpoint.startswith('/') else '/' + endpoint}",
            json={"model": provider.model, "max_tokens": provider.max_output, "temperature": provider.temperature, "messages": [{"role": "user", "content": prompt}]},
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
        content = data.get("content") or []
        output = "".join(part.get("text", "") for part in content if isinstance(part, dict))
        if not output:
            raise ValueError("Anthropic returned an empty response.")
        usage = data.get("usage") or {}
        return output, {"input_tokens": usage.get("input_tokens"), "output_tokens": usage.get("output_tokens"), "cached_tokens": 0, "actual": bool(usage)}
    if provider_type == "gemini":
        endpoint = provider.endpoint or f"/v1beta/models/{provider.model}:generateContent"
        params = {"key": api_key} if api_key else None
        response = client.post(
            f"{base_url}{endpoint if endpoint.startswith('/') else '/' + endpoint}",
            params=params,
            json={"contents": [{"role": "user", "parts": [{"text": prompt}]}], "generationConfig": {"temperature": provider.temperature, "maxOutputTokens": provider.max_output}},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        data = response.json()
        candidates = data.get("candidates") or []
        parts = ((candidates[0].get("content") or {}).get("parts") or []) if candidates else []
        output = "".join(part.get("text", "") for part in parts if isinstance(part, dict))
        if not output:
            raise ValueError("Gemini returned an empty response.")
        usage = data.get("usageMetadata") or {}
        return output, {"input_tokens": usage.get("promptTokenCount"), "output_tokens": usage.get("candidatesTokenCount"), "cached_tokens": 0, "actual": bool(usage)}
    if provider_type == "ollama":
        endpoint = provider.endpoint or "/api/chat"
        response = client.post(
            f"{base_url}{endpoint if endpoint.startswith('/') else '/' + endpoint}",
            json={"model": provider.model, "messages": [{"role": "user", "content": prompt}], "stream": False, "options": {"temperature": provider.temperature, "num_predict": provider.max_output}},
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        data = response.json()
        output = ((data.get("message") or {}).get("content") or data.get("response") or "").strip()
        if not output:
            raise ValueError("Ollama returned an empty response.")
        return output, {"input_tokens": data.get("prompt_eval_count"), "output_tokens": data.get("eval_count"), "cached_tokens": 0, "actual": bool(data.get("prompt_eval_count") or data.get("eval_count"))}
    raise ValueError(f"Unsupported provider_type: {provider.provider_type}")


def parse_copy_json(output: str) -> dict:
    import json
    import re

    text = output.strip()
    match = re.search(r"\{.*\}", text, re.S)
    if match:
        text = match.group(0)
    try:
        decoded = json.loads(text)
        return decoded if isinstance(decoded, dict) else {}
    except json.JSONDecodeError:
        return {}


def usage_summary(db: Session) -> dict:
    today = datetime.now(UTC).date()
    month_start = today.replace(day=1)
    today_start = datetime(today.year, today.month, today.day)
    month_dt = datetime(month_start.year, month_start.month, month_start.day)
    all_rows = db.execute(select(AIRequest.provider, func.sum(AIRequest.total_tokens)).group_by(AIRequest.provider)).all()
    statuses = db.execute(select(AIRequest.status, func.count()).group_by(AIRequest.status)).all()
    today_tokens = db.scalar(select(func.sum(AIRequest.total_tokens)).where(AIRequest.created_at >= today_start)) or 0
    month_tokens = db.scalar(select(func.sum(AIRequest.total_tokens)).where(AIRequest.created_at >= month_dt)) or 0
    cost = db.scalar(select(func.sum(AIRequest.estimated_cost)).where(AIRequest.created_at >= month_dt)) or 0
    usage_rows = db.execute(select(AIRequest.usage_source, func.sum(AIRequest.total_tokens)).group_by(AIRequest.usage_source)).all()
    return {
        "today_tokens": int(today_tokens),
        "month_tokens": int(month_tokens),
        "estimated_cost": float(cost),
        "by_provider": {row[0]: int(row[1] or 0) for row in all_rows},
        "status_counts": {row[0]: int(row[1] or 0) for row in statuses},
        "usage_source": {row[0] or "Estimated": int(row[1] or 0) for row in usage_rows},
    }


async def run_task_with_router(
    db: Session,
    task_name: str,
    prompt: str,
    candidate_id: int | None = None,
    job_id: int | None = None,
) -> dict:
    route, providers = _route_providers(db, task_name)
    budget_ok, budget_error = _budget_allowed(db, route)
    request_ok, request_error = _request_budget_allowed(route, prompt)
    budget_ok = budget_ok and request_ok
    budget_error = budget_error or request_error
    schema = (route.budget_policy or {}).get("expected_json_schema") if route else None

    attempts: list[dict] = []
    if not budget_ok:
        attempts.append({"provider": "budget", "model": None, "status": "skipped", "error": budget_error})
    for provider in providers if budget_ok else []:
        attempt = {"provider": provider.name, "model": provider.model, "status": "skipped", "error": None}
        if not provider.enabled:
            attempt["status"] = "disabled"
            attempts.append(attempt)
            continue
        if not provider.supports_text:
            attempt["status"] = "unsupported_capability"
            attempt["error"] = "Provider does not advertise text capability."
            attempts.append(attempt)
            continue
        if _circuit_open(provider):
            attempt["status"] = "circuit_open"
            attempt["error"] = f"Circuit open until {provider.circuit_open_until}."
            attempts.append(attempt)
            continue
        started = perf_counter()
        try:
            output, usage = await call_provider(provider, prompt)
            _validate_json_schema(output, schema)
            record_ai_request(
                db,
                provider=provider.name,
                model=provider.model,
                feature=task_name,
                candidate_id=candidate_id,
                job_id=job_id,
                prompt=prompt,
                output=output,
                started=started,
                status="success",
                input_tokens=usage.get("input_tokens"),
                output_tokens=usage.get("output_tokens"),
                cached_tokens=usage.get("cached_tokens", 0),
                usage_source="Actual" if usage.get("actual") else "Estimated",
            )
            _mark_provider_success(provider, started)
            attempt["status"] = "success"
            attempts.append(attempt)
            db.commit()
            return {"task_name": task_name, "provider": provider.name, "model": provider.model, "status": "success", "output": output, "attempts": attempts}
        except Exception as exc:
            _mark_provider_failure(provider, exc)
            attempt["status"] = "error"
            attempt["error"] = str(exc)
            attempts.append(attempt)
            db.add(SystemError(service="AI Router", feature=task_name, severity="Warning", message=str(exc), impact=f"Provider {provider.name} failed; fallback route continued."))

    started = perf_counter()
    output = local_task_response(task_name, prompt, "Local Template", "rules-plus-template")
    record_ai_request(
        db,
        provider="Local Template",
        model="rules-plus-template",
        feature=task_name,
        candidate_id=candidate_id,
        job_id=job_id,
        prompt=prompt,
        output=output,
        started=started,
        status="template_fallback",
        usage_source="Estimated",
    )
    db.add(SystemError(service="AI Router", feature=task_name, severity="Info", message="No configured provider route succeeded; template fallback was used.", impact="Output is deterministic template guidance and must not be shown as AI success."))
    attempts.append({"provider": "Local Template", "model": "rules-plus-template", "status": "template_fallback", "error": "No configured provider succeeded."})
    db.commit()
    return {"task_name": task_name, "provider": "Local Template", "model": "rules-plus-template", "status": "template_fallback", "output": output, "attempts": attempts}


def run_task_with_router_sync(
    db: Session,
    task_name: str,
    prompt: str,
    candidate_id: int | None = None,
    job_id: int | None = None,
    *,
    commit: bool = True,
) -> dict:
    route, providers = _route_providers(db, task_name)
    budget_ok, budget_error = _budget_allowed(db, route)
    request_ok, request_error = _request_budget_allowed(route, prompt)
    budget_ok = budget_ok and request_ok
    budget_error = budget_error or request_error
    schema = (route.budget_policy or {}).get("expected_json_schema") if route else None
    attempts: list[dict] = []
    if not budget_ok:
        attempts.append({"provider": "budget", "model": None, "status": "skipped", "error": budget_error})
    for provider in providers if budget_ok else []:
        attempt = {"provider": provider.name, "model": provider.model, "status": "skipped", "error": None}
        if not provider.enabled:
            attempt["status"] = "disabled"
            attempts.append(attempt)
            continue
        if not provider.supports_text:
            attempt["status"] = "unsupported_capability"
            attempt["error"] = "Provider does not advertise text capability."
            attempts.append(attempt)
            continue
        if _circuit_open(provider):
            attempt["status"] = "circuit_open"
            attempt["error"] = f"Circuit open until {provider.circuit_open_until}."
            attempts.append(attempt)
            continue
        started = perf_counter()
        try:
            output, usage = call_provider_sync(provider, prompt)
            _validate_json_schema(output, schema)
            record_ai_request(
                db,
                provider=provider.name,
                model=provider.model,
                feature=task_name,
                candidate_id=candidate_id,
                job_id=job_id,
                prompt=prompt,
                output=output,
                started=started,
                status="success",
                input_tokens=usage.get("input_tokens"),
                output_tokens=usage.get("output_tokens"),
                cached_tokens=usage.get("cached_tokens", 0),
                usage_source="Actual" if usage.get("actual") else "Estimated",
            )
            _mark_provider_success(provider, started)
            attempt["status"] = "success"
            attempts.append(attempt)
            if commit:
                db.commit()
            return {"task_name": task_name, "provider": provider.name, "model": provider.model, "status": "success", "output": output, "attempts": attempts}
        except Exception as exc:
            _mark_provider_failure(provider, exc)
            attempt["status"] = "error"
            attempt["error"] = str(exc)
            attempts.append(attempt)
            db.add(SystemError(service="AI Router", feature=task_name, severity="Warning", message=str(exc), impact=f"Provider {provider.name} failed; fallback route continued."))
    started = perf_counter()
    output = local_task_response(task_name, prompt, "Local Template", "rules-plus-template")
    record_ai_request(
        db,
        provider="Local Template",
        model="rules-plus-template",
        feature=task_name,
        candidate_id=candidate_id,
        job_id=job_id,
        prompt=prompt,
        output=output,
        started=started,
        status="template_fallback",
        usage_source="Estimated",
    )
    db.add(SystemError(service="AI Router", feature=task_name, severity="Info", message="No configured provider route succeeded; template fallback was used.", impact="Flow automation treats template fallback as manual-review only."))
    attempts.append({"provider": "Local Template", "model": "rules-plus-template", "status": "template_fallback", "error": "No configured provider succeeded."})
    if commit:
        db.commit()
    return {"task_name": task_name, "provider": "Local Template", "model": "rules-plus-template", "status": "template_fallback", "output": output, "attempts": attempts}


async def call_provider(provider: AIProvider, prompt: str) -> tuple[str, dict[str, Any]]:
    return await asyncio.to_thread(call_provider_sync, provider, prompt)


def local_task_response(task_name: str, prompt: str, provider: str, model: str) -> str:
    lowered = prompt.lower()
    if task_name.lower() in {"messageclassification", "message_classification"}:
        if any(term in lowered for term in ["salary", "pay", "wage", "薪", "錢"]):
            label = "salary_question"
        elif any(term in lowered for term in ["where", "location", "area", "地點", "哪裡"]):
            label = "location_question"
        elif any(term in lowered for term in ["apply", "interview", "應徵", "面試"]):
            label = "application_intent"
        else:
            label = "general_question"
        return f"Template fallback: label={label}; confidence=0.72; provider={provider}; model={model}; HR review required."
    if task_name.lower() in {"faq", "recruitreply", "recruit_reply"}:
        return "Template fallback: answer only from the job database. If information is missing, transfer to HR."
    if task_name.lower() in {"resumesummary", "resume_summary"}:
        return "Template fallback: extract contact, location, skills, licenses, availability, missing information, and HR notes."
    return f"Template fallback for {task_name}: review required. Prompt preview: {prompt[:240]}"


async def test_provider(provider: AIProvider) -> tuple[str, int | None, str, int | None]:
    started = perf_counter()
    if not provider.enabled:
        return "Disabled", None, "Provider is disabled.", None
    try:
        output, _usage = await call_provider(provider, "Respond with the word ok for a connection test.")
        latency = int((perf_counter() - started) * 1000)
        return "Connected", latency, f"Provider returned {len(output)} characters.", 200
    except httpx.HTTPStatusError as exc:
        latency = int((perf_counter() - started) * 1000)
        api_key = decrypt_secret(provider.api_key_encrypted)
        body = mask_provider_error(exc.response.text, api_key)
        return "Error", latency, body or str(exc), exc.response.status_code
    except Exception as exc:
        latency = int((perf_counter() - started) * 1000)
        if not provider.base_url:
            return "Not Configured", latency, "Base URL is required.", None
        return "Error", latency, mask_provider_error(str(exc)), None
