"""WBES session tokens: storage outside the project, atomic updates, no leakage.

The WBES portal issues a short-lived access token (JWT) and a refresh token
that **rotates** on every refresh: once a refresh succeeds, the old refresh
token is dead. The new pair must therefore be persisted immediately and
atomically (write a temporary file in the same directory, flush, fsync, then
``os.replace``), or a crash could leave only an invalidated token behind.

Location rules (decision W9): the session file must not be inside the
project, the raw-data directory, the log directory, or any OneDrive folder
(the project itself is synced to OneDrive). The default is
``<local app data>/pi/wbes/session.json``.

Tokens are never logged, never included in ``repr`` or error messages, and
never written to raw storage or the database.
"""

from __future__ import annotations

import base64
import json
import os
import tempfile
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PROJECT_ROOT, Settings
from src.pipelines.wbes.errors import WbesSessionError

SESSION_FORMAT_VERSION = 1
_ONEDRIVE_ENV = ("OneDrive", "OneDriveCommercial", "OneDriveConsumer")


def default_session_path() -> Path:
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(base) / "pi" / "wbes" / "session.json"


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def validate_session_path(path: Path, settings: Settings) -> Path:
    """Resolve ``path`` and refuse locations that could be committed, synced or archived.

    Raises:
        WbesSessionError: The path is inside the project, raw data, logs or OneDrive.
    """
    resolved = Path(path).expanduser().resolve()
    forbidden = {"the project directory": PROJECT_ROOT.resolve(), "RAW_DATA_DIR": Path(settings.raw_data_dir).resolve(),
                 "LOG_DIR": Path(settings.log_dir).resolve()}
    for env in _ONEDRIVE_ENV:
        if os.environ.get(env):
            forbidden[f"OneDrive ({env})"] = Path(os.environ[env]).resolve()
    for label, parent in forbidden.items():
        if _is_within(resolved, parent):
            raise WbesSessionError(f"WBES session file must not be inside {label}: {resolved}")
    if any("onedrive" in part.lower() for part in resolved.parts):
        raise WbesSessionError(f"WBES session file must not be inside a OneDrive folder: {resolved}")
    return resolved


def session_path(settings: Settings) -> Path:
    return validate_session_path(settings.wbes_session_file or default_session_path(), settings)


def jwt_expiry(token: str) -> datetime | None:
    """Expiry (``exp``) of a JWT, read without verifying the signature (used only to schedule a refresh)."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))["exp"]
        return datetime.fromtimestamp(float(exp), tz=timezone.utc)
    except Exception:  # malformed token: treat as expired-unknown
        return None


@dataclass(frozen=True)
class SessionTokens:
    """A WBES login. ``repr`` never shows the tokens."""

    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    utility_id: str
    login_at: datetime
    refreshed_at: datetime | None = None

    def __repr__(self) -> str:
        return (f"SessionTokens(utility_id={self.utility_id!r}, login_at={self.login_at.isoformat()}, "
                f"refreshed_at={self.refreshed_at.isoformat() if self.refreshed_at else None}, tokens=<hidden>)")

    __str__ = __repr__

    @property
    def access_expires_at(self) -> datetime | None:
        return jwt_expiry(self.access_token)

    def rotated(self, access_token: str, refresh_token: str, at: datetime) -> SessionTokens:
        return replace(self, access_token=access_token, refresh_token=refresh_token, refreshed_at=at)

    def to_json(self) -> dict[str, Any]:
        return {"version": SESSION_FORMAT_VERSION, "access_token": self.access_token,
                "refresh_token": self.refresh_token, "utility_id": self.utility_id,
                "login_at": self.login_at.isoformat(),
                "refreshed_at": self.refreshed_at.isoformat() if self.refreshed_at else None}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> SessionTokens:
        refreshed = data.get("refreshed_at")
        return cls(access_token=str(data["access_token"]), refresh_token=str(data["refresh_token"]),
                   utility_id=str(data["utility_id"]), login_at=datetime.fromisoformat(data["login_at"]),
                   refreshed_at=datetime.fromisoformat(refreshed) if refreshed else None)


class TokenStore:
    """Reads and atomically replaces the session file."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def __repr__(self) -> str:
        return f"TokenStore({str(self.path)!r})"

    def exists(self) -> bool:
        return self.path.is_file()

    def load(self) -> SessionTokens | None:
        """The saved session, or None if there is none.

        Raises:
            WbesSessionError: The file exists but cannot be read (content is never echoed).
        """
        if not self.path.is_file():
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data.get("version") != SESSION_FORMAT_VERSION:
                raise ValueError("unsupported session format version")
            return SessionTokens.from_json(data)
        except Exception as exc:
            raise WbesSessionError(f"WBES session file {self.path} is unreadable ({type(exc).__name__}); "
                                   "delete it and log in again") from None

    def save(self, tokens: SessionTokens) -> None:
        """Atomically replace the session file (same-directory temp file + fsync + os.replace)."""
        directory = self.path.parent
        directory.mkdir(parents=True, exist_ok=True)
        _restrict(directory, 0o700)
        fd, tmp_name = tempfile.mkstemp(prefix=".session-", suffix=".tmp", dir=directory)
        try:
            _restrict(Path(tmp_name), 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(tokens.to_json(), handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
        _restrict(self.path, 0o600)

    def delete(self) -> bool:
        try:
            self.path.unlink()
            return True
        except FileNotFoundError:
            return False


def _restrict(path: Path, mode: int) -> None:
    """Owner-only permissions where the OS supports them (on Windows the per-user profile ACL applies)."""
    try:
        os.chmod(path, mode)
    except OSError:
        pass
