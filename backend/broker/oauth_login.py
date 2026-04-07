"""
Smart Trader — Shoonya OAuth Login (standalone, no external dependencies)
=========================================================================

SEBI-mandated daily re-authentication via Shoonya's OAuth flow.
Uses headless Firefox + geckodriver for automated browser login.

This is a self-contained module — does NOT import from shoonya_platform.
"""

import json
import hashlib
import time
import logging
import os
import shutil
import tempfile
import requests
import pyotp
from urllib.parse import urlparse, parse_qs
from typing import Optional

logger = logging.getLogger(__name__)


# ── Token result helper ──────────────────────────────────────────────────────

class _TokenResult(str):
    """str subclass that also carries access_token for Bearer auth."""
    access_token: str = ""
    userid: str = ""
    actid: str = ""


def _make_token_result(susertoken: str, access_token: str = "",
                       userid: str = "", actid: str = "") -> "_TokenResult":
    """Build a _TokenResult from components (used when loading from cache)."""
    tr = _TokenResult(susertoken)
    tr.access_token = access_token or ""
    tr.userid = userid or ""
    tr.actid = actid or userid or ""
    return tr


# ── Selenium (lazy import) ───────────────────────────────────────────────────
try:
    from selenium import webdriver as _webdriver
    from selenium.webdriver.common.by import By as _By
    from selenium.webdriver.support.ui import WebDriverWait as _WebDriverWait
    from selenium.webdriver.support import expected_conditions as _EC
    from selenium.webdriver.firefox.service import Service as _FirefoxService
    from selenium.webdriver.firefox.options import Options as _FirefoxOptions
    from selenium.common.exceptions import (
        InvalidSessionIdException as _InvalidSessionIdException,
        WebDriverException as _WebDriverException,
    )
    _SELENIUM_AVAILABLE = True
except ImportError:
    _SELENIUM_AVAILABLE = False

    class _Stub:
        """Placeholder so attribute access doesn't crash at import time."""
        def __getattr__(self, _):
            raise RuntimeError("selenium is not installed")

    _webdriver = _Stub()  # type: ignore[assignment]
    _By = _Stub()  # type: ignore[assignment]
    _WebDriverWait = _Stub  # type: ignore[assignment,misc]
    _EC = _Stub()  # type: ignore[assignment]
    _FirefoxService = _Stub  # type: ignore[assignment,misc]
    _FirefoxOptions = _Stub  # type: ignore[assignment,misc]
    _InvalidSessionIdException = type("_ISE", (Exception,), {})  # type: ignore[assignment,misc]
    _WebDriverException = type("_WDE", (Exception,), {})  # type: ignore[assignment,misc]

# ── Constants ────────────────────────────────────────────────────────────────
_BASE_URL = "https://trade.shoonya.com"
_TOKEN_URL = f"{_BASE_URL}/NorenWClientAPI/GenAcsTok"
_OAUTH_LOGIN_URL_TPL = (
    f"{_BASE_URL}/OAuthlogin/investor-entry-level/login"
    "?api_key={vendor_code}&route_to={user_id}+s+apikey"
)
_GECKODRIVER_PATH = "/usr/local/bin/geckodriver"
_FIREFOX_BINARY = "/usr/lib/firefox/firefox"


# ── Credential helpers ───────────────────────────────────────────────────────

def _read_creds_from_env() -> dict:
    """Read credentials from already-loaded environment variables."""
    def _strip(val: str) -> str:
        if val and "#" in val:
            val = val[: val.index("#")]
        return val.strip()

    return {
        "user_id":      _strip(os.getenv("USER_ID", "")),
        "password":     _strip(os.getenv("PASSWORD", "")),
        "totp_key":     _strip(os.getenv("TOKEN", "")),
        "vendor_code":  _strip(os.getenv("VC", "")),
        "oauth_secret": _strip(os.getenv("OAUTH_SECRET", "")),
    }


# ── Firefox driver ────────────────────────────────────────────────────────────

def _build_driver():
    """Create headless Firefox WebDriver using geckodriver (ARM64 compatible)."""
    if not _SELENIUM_AVAILABLE:
        raise RuntimeError("selenium is not installed. Run: pip install selenium")

    profile_dir = tempfile.mkdtemp(prefix="smart-trader-oauth-firefox-")
    options = _FirefoxOptions()
    options.add_argument("--headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    options.binary_location = _FIREFOX_BINARY  # type: ignore[attr-defined]

    options.set_preference("browser.download.folderList", 2)
    options.set_preference("profile", profile_dir)

    service = _FirefoxService(executable_path=_GECKODRIVER_PATH)  # type: ignore[call-arg]
    try:
        driver = _webdriver.Firefox(service=service, options=options)  # type: ignore[attr-defined]
        setattr(driver, "_tmp_profile_dir", profile_dir)
        logger.info("Firefox launched in headless mode")
        return driver
    except Exception as exc:
        shutil.rmtree(profile_dir, ignore_errors=True)
        raise RuntimeError(f"Unable to launch Firefox WebDriver: {exc}")


# ── Auth code capture ─────────────────────────────────────────────────────────

def _fast_fill(element, value: str) -> None:
    element.click()
    time.sleep(0.1)
    element.clear()
    element.send_keys(value)
    time.sleep(0.1)


def _capture_auth_code(driver, creds: dict) -> Optional[str]:
    """Drive headless browser through OAuth login and capture auth code."""
    wait = _WebDriverWait(driver, 30)  # type: ignore[call-arg]
    login_url = _OAUTH_LOGIN_URL_TPL.format(
        vendor_code=creds["vendor_code"],
        user_id=creds["user_id"],
    )

    logger.info("Opening Shoonya OAuth login page...")
    driver.get(login_url)
    wait.until(_EC.presence_of_element_located((_By.CSS_SELECTOR, "input")))
    time.sleep(2)

    all_inputs = driver.find_elements(
        _By.CSS_SELECTOR,
        "input:not([type='hidden']):not([type='checkbox']):not([type='radio'])",
    )
    visible_inputs = [inp for inp in all_inputs if inp.is_displayed()]
    logger.info("Visible input fields found: %d", len(visible_inputs))

    if len(visible_inputs) < 2:
        raise RuntimeError(
            f"Expected at least 2 input fields, got {len(visible_inputs)}"
        )

    _fast_fill(visible_inputs[0], creds["user_id"])
    _fast_fill(visible_inputs[1], creds["password"])

    otp_value: Optional[str] = None
    if len(visible_inputs) >= 3:
        otp_value = pyotp.TOTP(creds["totp_key"]).now()
        _fast_fill(visible_inputs[2], otp_value)
        logger.info("TOTP entered")

    # Click LOGIN button
    try:
        wait.until(
            _EC.element_to_be_clickable((_By.XPATH, "//button[normalize-space()='LOGIN']"))
        ).click()
    except Exception:
        try:
            wait.until(
                _EC.element_to_be_clickable(
                    (_By.XPATH, "//button[contains(translate(text(),'login','LOGIN'),'LOGIN')]")
                )
            ).click()
        except Exception:
            visible_inputs[1].submit()

    logger.info("Credentials submitted — waiting for auth code...")

    start = time.time()
    while True:
        current_url = driver.current_url
        if "code=" in current_url:
            parsed = urlparse(current_url)
            code = parse_qs(parsed.query).get("code", [None])[0]
            if code:
                logger.info("Auth code captured from redirect URL")
                return code

        # Shoonya redirects headless sessions to /GetAuthCode — code is in the JSON body
        if "GetAuthCode" in current_url or "getauthcode" in current_url.lower():
            try:
                import json as _json, re as _re
                page_src = driver.page_source
                body_text = _re.sub(r"<[^>]+>", "", page_src).strip()
                result = _json.loads(body_text)
                code = result.get("code")
                if code:
                    logger.info("Auth code captured from GetAuthCode page source")
                    return code
            except Exception as _e:
                logger.debug("GetAuthCode page source parse failed: %s", _e)

        if time.time() - start > 60:
            if otp_value and creds.get("totp_key"):
                new_otp = pyotp.TOTP(creds["totp_key"]).now()
                if new_otp != otp_value:
                    try:
                        _fast_fill(visible_inputs[2], new_otp)
                        wait.until(
                            _EC.element_to_be_clickable(
                                (_By.XPATH, "//button[normalize-space()='LOGIN']")
                            )
                        ).click()
                        start = time.time()
                        otp_value = new_otp
                        logger.info("TOTP refreshed, retrying...")
                        continue
                    except Exception:
                        pass
            logger.error("Timeout capturing auth code. Current URL: %s", driver.current_url)
            return None

        time.sleep(0.5)


# ── Main OAuth flow ───────────────────────────────────────────────────────────

def run_oauth_login(config=None):
    """
    Execute the daily Shoonya OAuth login flow (SEBI compliance).

    Args:
        config: Optional object with user_id, password, totp_key, vendor_code,
                oauth_secret attributes. If None, reads from environment.

    Returns:
        Tuple (susertoken, access_token) on success, (None, None) on failure.
        For backwards compatibility callers that expect a plain string:
        the returned object is a str subclass equal to susertoken but with an
        extra `access_token` attribute when access_token is also available.
        Access token string on success, None on failure.
    """
    if config is not None:
        creds = {
            "user_id":      config.user_id or "",
            "password":     config.password or "",
            "totp_key":     config.totp_key or "",
            "vendor_code":  config.vendor_code or "",
            "oauth_secret": getattr(config, "oauth_secret", None) or os.getenv("OAUTH_SECRET", ""),
        }
    else:
        creds = _read_creds_from_env()

    if not creds.get("oauth_secret"):
        logger.error("OAUTH_SECRET not set — cannot run OAuth login.")
        return None

    if not _SELENIUM_AVAILABLE:
        logger.error("selenium not installed — run: pip install selenium")
        return None

    driver = None
    auth_code: Optional[str] = None

    try:
        driver = _build_driver()
        auth_code = _capture_auth_code(driver, creds)
    except (_InvalidSessionIdException, _WebDriverException) as exc:
        logger.error("Browser error during OAuth login: %s", exc)
    except Exception as exc:
        logger.exception("Unexpected error during OAuth login: %s", exc)
    finally:
        if driver:
            tmp_profile_dir = getattr(driver, "_tmp_profile_dir", None)
            try:
                driver.quit()
            except Exception:
                pass
            if tmp_profile_dir:
                shutil.rmtree(tmp_profile_dir, ignore_errors=True)

    if not auth_code:
        logger.error("OAuth login failed — auth code not captured")
        return None

    # Compute checksum: SHA256(vendor_code + oauth_secret + auth_code)
    checksum = hashlib.sha256(
        (creds["vendor_code"] + creds["oauth_secret"] + auth_code).encode()
    ).hexdigest()

    # Call GenAcsTok to complete activation
    # uid field is required — server returns empty body (→ JSONDecodeError) without it
    payload = f'jData={{"code":"{auth_code}","uid":"{creds["user_id"]}","checksum":"{checksum}"}}'

    try:
        resp = requests.post(_TOKEN_URL, data=payload, timeout=30)
        result = resp.json()
        logger.info("GenAcsTok: stat=%s", result.get("stat"))
        logger.info("GenAcsTok fields: stat=%s susertoken=%s access_token=%s USERID=%s actid=%s",
                    result.get("stat"), bool(result.get("susertoken")),
                    bool(result.get("access_token")), result.get("USERID"), result.get("actid"))

        # New Shoonya OAuth returns access_token + susertoken (Bearer auth required)
        # Older flow returned stat=Ok + susertoken (jKey auth)
        susertoken = result.get("susertoken")
        access_token = result.get("access_token")
        userid = result.get("USERID") or result.get("uid") or creds["user_id"]
        actid = result.get("actid") or userid

        success = (result.get("stat") == "Ok" or "access_token" in result) and susertoken

        if success:
            logger.info("✅ Daily OAuth login successful (SEBI compliant)")
            return _make_token_result(susertoken, access_token or "", userid, actid)
        logger.warning("OAuth response missing tokens: %s", result)

    except Exception as exc:
        logger.exception("GenAcsTok request failed: %s", exc)

    return None
