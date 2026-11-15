"""Interactive WBES login (decision W9).

A visible browser window opens on the WBES portal with a fresh, empty profile.
**A person** enters the username, the OTP and the CAPTCHA; this code never
fills in, solves or bypasses any of them. It only waits until the portal's own
page holds a session (the portal keeps the logged-in user, with its access and
refresh tokens, in the page's ``localStorage`` under ``currentUser``), copies
that session, closes the window and saves it with :class:`TokenStore`.

It is only ever run on request (``python -m scripts.ingest_wbes login``);
ingestion runs never open a browser. Playwright is imported lazily so the rest
of PI does not depend on it.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Protocol

from config.settings import Settings
from src.pipelines.wbes.errors import WbesError
from src.pipelines.wbes.session import SessionTokens, TokenStore

_UTILITY_ID = re.compile(r"^[0-9A-Fa-f]{32}$")

# Runs inside the portal page. Returns what the portal itself stored, or null.
_READ_SESSION_JS = r"""
() => {
  const stored = window.localStorage.getItem('currentUser');
  if (!stored) { return null; }
  let parsed;
  try { parsed = JSON.parse(stored); } catch (err) { return null; }
  const user = (parsed && parsed.user) ? parsed.user : parsed;
  if (!user) { return null; }
  const picked = Array.from(document.querySelectorAll('select'))
    .map(el => el.value).find(v => /^[0-9A-Fa-f]{32}$/.test(v || ''));
  return {access: user.Token || null, refresh: user.RefreshToken || null, utility: picked || null};
}
"""


class PageLike(Protocol):
    def goto(self, url: str, **kwargs: Any) -> Any: ...
    def evaluate(self, script: str) -> Any: ...
    def wait_for_timeout(self, milliseconds: float) -> None: ...


Launcher = Callable[[], tuple[PageLike, Callable[[], None]]]


def read_portal_session(page: PageLike) -> dict[str, str | None] | None:
    try:
        found = page.evaluate(_READ_SESSION_JS)
    except Exception:      # the page is navigating between login steps
        return None
    return found if isinstance(found, dict) else None


def _playwright_launcher(settings: Settings) -> Launcher:
    def launch() -> tuple[PageLike, Callable[[], None]]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - depends on the local install
            raise WbesError("Playwright is required for the interactive login: pip install playwright "
                            "(and 'playwright install chromium' if Chrome/Edge are not installed)") from exc
        manager = sync_playwright().start()
        channel = settings.wbes_login_browser
        try:
            browser = manager.chromium.launch(headless=False, **({} if channel == "chromium" else {"channel": channel}))
        except Exception:
            browser = manager.chromium.launch(headless=False)
        context = browser.new_context()          # empty profile: no stored sessions or cookies
        page = context.new_page()

        def close() -> None:
            for closer in (context.close, browser.close, manager.stop):
                try:
                    closer()
                except Exception:
                    pass
        return page, close
    return launch


def interactive_login(settings: Settings, store: TokenStore, *, launcher: Launcher | None = None,
                      wait_minutes: float | None = None, poll_seconds: float = 3.0,
                      out: Callable[[str], None] = print, save: bool = True,
                      clock: Callable[[], float] = time.monotonic) -> SessionTokens:
    """Wait for a person to log in to WBES in a browser window, then save the session.

    With ``save=False`` the captured session is returned unsaved (the caller saves it,
    e.g. once it holds the session lock for the utility id read from the portal).

    Raises:
        WbesError: The login was not completed in time, or no utility id is available.
    """
    launch = launcher or _playwright_launcher(settings)
    deadline = clock() + 60 * (wait_minutes if wait_minutes is not None else settings.wbes_login_wait_minutes)
    page, close = launch()
    captured: dict[str, str | None] | None = None
    try:
        out(f"A browser window has opened on {settings.wbes_portal_url}. Log in there yourself "
            "(username, OTP, CAPTCHA). This window closes automatically afterwards.")
        try:
            page.goto(settings.wbes_portal_url, wait_until="domcontentloaded", timeout=30000)
        except Exception:
            out("The portal did not finish loading yet; keep the window open and log in when it appears.")
        tokens_seen_at: float | None = None
        while clock() < deadline:
            found = read_portal_session(page)
            if found and found.get("access") and found.get("refresh"):
                captured = found
                tokens_seen_at = tokens_seen_at or clock()
                # Wait briefly for the utility selector unless the id is configured.
                if settings.wbes_utility_id or found.get("utility") or clock() - tokens_seen_at > 30:
                    break
            page.wait_for_timeout(poll_seconds * 1000)
    finally:
        close()
    if not captured:
        raise WbesError("WBES login was not completed before the login window timed out")
    utility = settings.wbes_utility_id or captured.get("utility")
    if not utility or not _UTILITY_ID.fullmatch(utility):
        raise WbesError("logged in, but the WBES utility id could not be determined; set WBES_UTILITY_ID in .env")
    tokens = SessionTokens(access_token=str(captured["access"]), refresh_token=str(captured["refresh"]),
                           utility_id=utility.upper(), login_at=datetime.now(timezone.utc))
    if save:
        store.save(tokens)
        out(f"WBES session saved to {store.path} (utility {tokens.utility_id}). Tokens are not displayed.")
    return tokens
