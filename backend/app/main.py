from datetime import datetime
import json

from fastapi import FastAPI, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy import inspect

from backend.app.api.routes import router
from backend.app.core.config import get_settings
from backend.app.core.db import Base, SessionLocal, engine
from backend.app.models.domain import UserSession
from backend.app.models import domain  # noqa: F401
from backend.app.services.security import hash_token
from backend.app.services.seed import seed_demo


settings = get_settings()
settings.validate_runtime_safety()
if settings.auto_create_tables:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if existing_tables and "alembic_version" not in existing_tables:
        raise RuntimeError("Existing database has no alembic_version. Run `alembic upgrade head` before startup; create_all is only allowed for empty local/test databases.")
    Base.metadata.create_all(bind=engine)
if settings.demo_mode:
    with SessionLocal() as db:
        seed_demo(db)

app = FastAPI(title=settings.app_name, version="0.1.0", redoc_url=None)


OPENAPI_SUMMARIES = {
    ("GET", "/api/health"): "系統健康狀態",
    ("POST", "/api/auth/login"): "登入",
    ("GET", "/api/auth/me"): "目前登入使用者",
    ("POST", "/api/auth/logout"): "登出",
    ("GET", "/api/jobs"): "職缺清單",
    ("POST", "/api/jobs"): "新增職缺",
    ("PATCH", "/api/jobs/{job_id}"): "更新職缺",
    ("POST", "/api/jobs/{job_id}/duplicate"): "複製職缺",
    ("GET", "/api/candidates"): "候選人清單",
    ("POST", "/api/candidates"): "新增候選人",
    ("PATCH", "/api/candidates/{candidate_id}"): "更新候選人",
    ("DELETE", "/api/candidates/{candidate_id}"): "刪除候選人資料",
    ("GET", "/api/candidates/{candidate_id}/export"): "匯出候選人資料",
    ("GET", "/api/applications"): "應徵清單",
    ("POST", "/api/applications"): "建立應徵",
    ("PATCH", "/api/applications/{application_id}/status"): "更新應徵狀態",
    ("GET", "/api/applications/{application_id}/history"): "應徵歷程",
    ("GET", "/api/interviews"): "面試清單",
    ("POST", "/api/interviews"): "安排面試",
    ("GET", "/api/conversations"): "對話清單",
    ("POST", "/api/conversations"): "新增對話",
    ("GET", "/api/conversations/{conversation_id}/messages"): "對話訊息",
    ("POST", "/api/conversations/{conversation_id}/messages"): "新增訊息",
    ("GET", "/api/matches"): "媒合結果",
    ("POST", "/api/matches/suggest"): "執行媒合建議",
    ("GET", "/api/analytics/summary"): "分析摘要",
    ("GET", "/api/posts"): "貼文清單",
    ("POST", "/api/posts"): "建立貼文",
    ("POST", "/api/ai/copywriting"): "產生招募文案",
    ("GET", "/api/ai/providers"): "AI 供應商清單",
    ("POST", "/api/ai/providers"): "新增 AI 供應商",
    ("POST", "/api/ai/providers/{provider_id}/test"): "測試 AI 供應商",
    ("GET", "/api/ai/routes"): "AI 任務路由",
    ("POST", "/api/ai/routes"): "儲存 AI 任務路由",
    ("POST", "/api/ai/run"): "執行 AI 任務",
    ("GET", "/api/platforms"): "平台能力清單",
    ("GET", "/api/platform-accounts"): "平台帳號清單",
    ("POST", "/api/platform-accounts"): "新增平台帳號",
    ("GET", "/api/queue/tasks"): "佇列任務清單",
    ("POST", "/api/queue/tasks"): "新增佇列任務",
    ("POST", "/api/queue/tasks/{task_id}/run"): "執行佇列任務",
    ("POST", "/api/queue/tasks/{task_id}/replay"): "重送佇列任務",
    ("GET", "/api/queue/outbox"): "佇列 Outbox",
    ("GET", "/api/queue/dlq"): "Redis 死信清單",
    ("POST", "/api/queue/dlq/{record_id}/replay"): "重送 Redis 死信",
    ("GET", "/api/queue/redis/metrics"): "Redis 佇列指標",
    ("GET", "/api/do-not-contact"): "不聯繫名單",
    ("POST", "/api/do-not-contact"): "新增不聯繫紀錄",
    ("POST", "/api/do-not-contact/{entry_id}/lift"): "解除不聯繫",
    ("GET", "/api/recruitment-blacklist"): "招募黑名單",
    ("POST", "/api/recruitment-blacklist"): "新增招募黑名單",
    ("POST", "/api/recruitment-blacklist/{entry_id}/confirm-scope"): "確認黑名單範圍",
    ("POST", "/api/recruitment-blacklist/{entry_id}/activate"): "啟用黑名單",
    ("POST", "/api/recruitment-blacklist/{entry_id}/lift"): "解除黑名單",
    ("GET", "/api/audit-logs"): "稽核紀錄",
}


def localized_openapi() -> dict:
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title="招募營運 API", version="0.1.0", routes=app.routes)
    schema["info"]["description"] = "招募營運系統的本地 API 規格，摘要與常見回應已中文化。"
    for path, methods in schema.get("paths", {}).items():
        for method, operation in methods.items():
            if not isinstance(operation, dict):
                continue
            summary = OPENAPI_SUMMARIES.get((method.upper(), path))
            if summary:
                operation["summary"] = summary
            responses = operation.get("responses", {})
            for status_code, response in responses.items():
                if not isinstance(response, dict):
                    continue
                if status_code == "200":
                    response["description"] = "成功回應"
                elif status_code == "201":
                    response["description"] = "建立成功"
                elif status_code == "204":
                    response["description"] = "已完成，無回應內容"
                elif status_code == "422":
                    response["description"] = "資料驗證錯誤"
    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = localized_openapi


def wants_browser_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept


def json_html_page(title: str, payload: dict) -> HTMLResponse:
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    escaped = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return HTMLResponse(
        f"""<!doctype html>
<html lang="zh-Hant">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title}</title>
    <style>
      :root {{ --bg: #f4f7fb; --ink: #18212f; --muted: #647084; --line: #dce4ef; --accent: #0f766e; }}
      * {{ box-sizing: border-box; }}
      body {{ margin: 0; min-height: 100vh; background: linear-gradient(180deg, rgba(37, 99, 235, .07), transparent 280px), var(--bg); color: var(--ink); font-family: "Noto Sans TC", "Microsoft JhengHei", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
      main {{ max-width: 1120px; margin: 0 auto; padding: 34px 20px; }}
      .eyebrow {{ margin: 0 0 8px; color: var(--accent); font-size: 13px; font-weight: 800; }}
      h1 {{ margin: 0 0 18px; font-size: 30px; letter-spacing: 0; }}
      pre {{ margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; background: rgba(255,255,255,.94); border: 1px solid var(--line); border-radius: 8px; padding: 20px; line-height: 1.6; box-shadow: 0 18px 45px rgba(30, 43, 66, .09); }}
    </style>
  </head>
  <body>
    <main>
      <p class="eyebrow">RecruitOps API</p>
      <h1>{title}</h1>
      <pre>{escaped}</pre>
    </main>
  </body>
</html>""",
        headers={"Cache-Control": "no-store", "Content-Language": "zh-Hant"},
    )


@app.middleware("http")
async def csrf_middleware(request: Request, call_next):
    if request.url.path == "/openapi.json" and wants_browser_html(request):
        return json_html_page("OpenAPI 規格", app.openapi())
    csrf_exempt = {"/api/auth/login"}
    if request.url.path.startswith("/api/webhooks/"):
        csrf_exempt.add(request.url.path)
    if request.url.path.startswith("/api/") and request.method not in {"GET", "HEAD", "OPTIONS"} and request.url.path not in csrf_exempt:
        session_cookie = request.cookies.get("recruit_session")
        csrf_cookie = request.cookies.get("recruit_csrf")
        csrf_header = request.headers.get("x-csrf-token")
        if not session_cookie or not csrf_cookie or not csrf_header or csrf_cookie != csrf_header:
            return JSONResponse({"detail": "CSRF validation failed"}, status_code=403)
        with SessionLocal() as db:
            session = db.scalar(
                select(UserSession).where(
                    UserSession.token_hash == hash_token(session_cookie),
                    UserSession.csrf_token == csrf_cookie,
                    UserSession.revoked_at.is_(None),
                    UserSession.expires_at > datetime.utcnow(),
                )
            )
        if not session:
            return JSONResponse({"detail": "CSRF session not found"}, status_code=403)
    response = await call_next(request)
    if request.url.path == "/openapi.json" or response.headers.get("content-type", "").startswith("application/json"):
        response.headers["content-type"] = "application/json; charset=utf-8"
        response.headers["cache-control"] = "no-store"
        response.headers["content-language"] = "zh-Hant"
        response.headers["x-content-type-options"] = "nosniff"
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router, prefix="/api")


@app.get("/redoc", include_in_schema=False)
def local_redoc() -> HTMLResponse:
    return HTMLResponse(
        """
        <!doctype html>
        <html lang="zh-Hant">
          <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>招募系統 API 文件</title>
            <style>
              body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; color: #17202a; background: #f6f8fb; }
              main { max-width: 960px; margin: 0 auto; padding: 40px 24px; }
              h1 { margin: 0 0 12px; font-size: 32px; }
              p { line-height: 1.7; }
              a { color: #1b66d2; }
              .panel { background: #fff; border: 1px solid #d9e2ef; border-radius: 8px; padding: 20px; margin: 16px 0; }
              code { background: #eef3fb; padding: 2px 6px; border-radius: 4px; }
            </style>
          </head>
          <body>
            <main>
              <h1>招募系統 API 文件</h1>
              <p>這是本地版文件入口，避免 ReDoc 外部資源載入失敗時出現空白頁。</p>
              <section class="panel">
                <h2>可用入口</h2>
                <p><a href="/docs">互動式 API 文件</a></p>
                <p><a href="/openapi.json">OpenAPI JSON（中文摘要）</a></p>
                <p><a href="/api/health">系統健康狀態</a></p>
              </section>
              <section class="panel">
                <h2>常用端點</h2>
                <p><code>/api/jobs</code> 職缺</p>
                <p><code>/api/candidates</code> 候選人</p>
                <p><code>/api/applications</code> 應徵流程</p>
                <p><code>/api/queue/tasks</code> 佇列任務</p>
              </section>
            </main>
          </body>
        </html>
        """
    )


app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
