import base64
import hashlib
import hmac
import json
import secrets
import time
from datetime import datetime, timedelta

from fastapi import Depends, HTTPException, Request, Response, status
from cryptography.fernet import Fernet, InvalidToken
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.core.config import get_settings
from backend.app.core.db import get_db
from backend.app.models.domain import User, UserSession


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def mask_secret(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 8:
        return "****"
    return f"{value[:4]}...{value[-4:]}"


def encrypt_secret(value: str | None) -> str | None:
    if not value:
        return None
    return _fernet().encrypt(value.encode()).decode()


def decrypt_secret(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return _fernet().decrypt(value.encode()).decode()
    except InvalidToken as exc:
        raise ValueError("Secret failed integrity validation.") from exc


def _fernet() -> Fernet:
    digest = hashlib.sha256(get_settings().secret_key.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _sign(value: str) -> str:
    key = get_settings().secret_key.encode()
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()


def create_session_token(user: User, ttl_seconds: int | None = None) -> str:
    ttl = ttl_seconds or get_settings().session_ttl_seconds
    payload = {"sub": user.id, "role": user.role, "sid": secrets.token_urlsafe(24), "exp": int(time.time()) + ttl}
    body = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()
    return f"{body}.{_sign(body)}"


def hash_token(token: str) -> str:
    return hmac.new(get_settings().secret_key.encode(), token.encode(), hashlib.sha256).hexdigest()


def parse_session_token(token: str) -> dict:
    try:
        body, signature = token.split(".", 1)
        if not hmac.compare_digest(_sign(body), signature):
            raise ValueError("bad signature")
        payload = json.loads(base64.urlsafe_b64decode(body.encode()).decode())
        if int(payload["exp"]) < int(time.time()):
            raise ValueError("expired")
        return payload
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session") from exc


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        "recruit_session",
        token,
        httponly=True,
        secure=get_settings().cookie_secure or get_settings().is_production,
        samesite="lax",
        max_age=get_settings().session_ttl_seconds,
    )


def set_csrf_cookie(response: Response, csrf_token: str) -> None:
    response.set_cookie(
        "recruit_csrf",
        csrf_token,
        httponly=False,
        secure=get_settings().cookie_secure or get_settings().is_production,
        samesite="lax",
        max_age=get_settings().session_ttl_seconds,
    )


def create_persistent_session(db: Session, user: User, request: Request, response: Response) -> None:
    token = create_session_token(user)
    csrf_token = secrets.token_urlsafe(32)
    db.add(
        UserSession(
            user_id=user.id,
            token_hash=hash_token(token),
            csrf_token=csrf_token,
            expires_at=datetime.utcnow() + timedelta(seconds=get_settings().session_ttl_seconds),
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent", "")[:255],
        )
    )
    set_session_cookie(response, token)
    set_csrf_cookie(response, csrf_token)


def get_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    token = request.cookies.get("recruit_session")
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Login required")
    payload = parse_session_token(token)
    session = db.scalar(
        select(UserSession).where(
            UserSession.token_hash == hash_token(token),
            UserSession.revoked_at.is_(None),
            UserSession.expires_at > datetime.utcnow(),
        )
    )
    if not session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session revoked or expired")
    user = db.scalar(select(User).where(User.id == payload["sub"], User.active.is_(True)))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


def revoke_current_session(request: Request, response: Response, db: Session) -> None:
    token = request.cookies.get("recruit_session")
    if token:
        session = db.scalar(select(UserSession).where(UserSession.token_hash == hash_token(token), UserSession.revoked_at.is_(None)))
        if session:
            session.revoked_at = datetime.utcnow()
    response.delete_cookie("recruit_session")
    response.delete_cookie("recruit_csrf")


def require_csrf(request: Request, db: Session = Depends(get_db)) -> None:
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return
    token = request.cookies.get("recruit_session")
    csrf_cookie = request.cookies.get("recruit_csrf")
    csrf_header = request.headers.get("x-csrf-token")
    if not token or not csrf_cookie or not csrf_header or not hmac.compare_digest(csrf_cookie, csrf_header):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CSRF validation failed")
    session = db.scalar(select(UserSession).where(UserSession.token_hash == hash_token(token), UserSession.csrf_token == csrf_cookie))
    if not session:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CSRF session not found")


ROLE_LEVEL = {
    "Viewer": 10,
    "Dispatcher": 30,
    "Recruiter / HR": 50,
    "HR": 50,
    "Manager": 80,
    "Super Admin": 100,
}


def require_role(min_role: str):
    def dependency(user: User = Depends(get_current_user)) -> User:
        if ROLE_LEVEL.get(user.role, 0) < ROLE_LEVEL[min_role]:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permission")
        return user

    return dependency
