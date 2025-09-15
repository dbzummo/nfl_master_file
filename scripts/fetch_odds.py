# odds_client.py  (you can name this fetch_odds.py if you prefer)

import os, time, json, math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---------- Config ----------
STRICT_MODE: bool = os.getenv("STRICT_MODE", "1") == "1"
ODDS_TIMEOUT: float = float(os.getenv("ODDS_TIMEOUT", "6.0"))
ODDS_RETRIES: int = int(os.getenv("ODDS_RETRIES", "3"))
ODDS_BACKOFF_BASE: float = float(os.getenv("ODDS_BACKOFF_BASE", "0.5"))  # seconds
PROVIDER_ERROR_LOG = os.getenv("PROVIDER_ERROR_LOG", "out/out_provider_errors.jsonl")

TRANSIENT_HTTP = {429, 500, 502, 503, 504}

# ---------- Errors ----------
class ProviderError(Exception):
    pass

class ProviderHTTPError(ProviderError):
    def __init__(self, status: int, text_snippet: str, *args, **kwargs):
        super().__init__(f"HTTP {status}: {text_snippet}", *args, **kwargs)
        self.status = status
        self.text_snippet = text_snippet

class ProviderParseError(ProviderError):
    pass

# ---------- Utilities ----------
def _ensure_out_dir(path: str) -> None:
    import pathlib
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

def _log_provider_error(provider: str,
                        endpoint: str,
                        params: Dict[str, Any],
                        status: Optional[int] = None,
                        body_snippet: Optional[str] = None,
                        note: Optional[str] = None) -> None:
    _ensure_out_dir(PROVIDER_ERROR_LOG)
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "provider": provider,
        "endpoint": endpoint,
        "params": params,
        "status": status,
        "body_snippet": (body_snippet[:500] if body_snippet else None),
        "note": note,
    }
    with open(PROVIDER_ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def _implied_from_american(odds: Optional[int]) -> Optional[float]:
    if odds is None:
        return None
    try:
        o = int(odds)
    except Exception:
        return None
    if o > 0:
        return 100.0 / (o + 100.0)
    elif o < 0:
        return (-o) / ((-o) + 100.0)
    return None

def _clip01(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        return max(0.0, min(1.0, float(x)))
    except Exception:
        return None

# ---------- HTTP Core ----------
def _call_odds_api(session: requests.Session,
                   url: str,
                   params: Dict[str, Any],
                   headers: Dict[str, str]) -> Dict[str, Any]:
    resp = session.get(url, params=params, headers=headers, timeout=ODDS_TIMEOUT)
    if resp is None:
        raise ProviderError("No response object (None) from provider.")

    if resp.status_code != 200:
        # include a short snippet so logs are readable but not huge
        snippet = (resp.text or "")[:500]
        raise ProviderHTTPError(resp.status_code, snippet)

    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "json" not in ctype:
        raise ProviderParseError(f"Expected JSON content-type, got: {ctype or 'N/A'}")

    try:
        data = resp.json()
    except Exception as e:
        raise ProviderParseError(f"Invalid JSON: {e}") from e

    if data is None or (isinstance(data, (list, dict)) and len(data) == 0):
        raise ProviderParseError("Empty JSON payload.")

    return data

# ---------- Schema normalization ----------
def _normalize_odds_schema(provider: str, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert provider JSON to a stable list of odds rows with fields:
      - game_id, home_team, away_team
      - moneyline_home, moneyline_away (american ints or None)
      - spread, spread_home_odds, spread_away_odds (if present)
      - total, over_odds, under_odds (if present)
      - implied_home, implied_away (floats in [0,1] if derivable)
      - source_ts (ISO8601)
      - provider
    This function is written to be permissive; unknown structures pass through best-effort.
    """
    out: List[Dict[str, Any]] = []
    ts = datetime.now(timezone.utc).isoformat()

    # Generic cases (adjust these adapters as needed per provider)
    # ---- Example: Provider returns {"events":[{...}, ...]}
    if isinstance(raw, dict) and "events" in raw and isinstance(raw["events"], list):
        events = raw["events"]
        for e in events:
            game_id = e.get("id") or e.get("game_id") or e.get("msf_game_id")
            home = (e.get("home_team") or e.get("home") or {}).get("name") if isinstance(e.get("home_team") or e.get("home"), dict) else (e.get("home_team") or e.get("home"))
            away = (e.get("away_team") or e.get("away") or {}).get("name") if isinstance(e.get("away_team") or e.get("away"), dict) else (e.get("away_team") or e.get("away"))

            ml_home = e.get("moneyline_home") or (e.get("odds") or {}).get("home_ml")
            ml_away = e.get("moneyline_away") or (e.get("odds") or {}).get("away_ml")

            spread = e.get("spread") or (e.get("odds") or {}).get("spread")
            s_home = (e.get("odds") or {}).get("spread_home_odds")
            s_away = (e.get("odds") or {}).get("spread_away_odds")

            total = e.get("total") or (e.get("odds") or {}).get("total")
            o_odds = (e.get("odds") or {}).get("over_odds")
            u_odds = (e.get("odds") or {}).get("under_odds")

            ih = _clip01(_implied_from_american(ml_home))
            ia = _clip01(_implied_from_american(ml_away))

            out.append({
                "provider": provider,
                "source_ts": ts,
                "game_id": game_id,
                "home_team": home,
                "away_team": away,
                "moneyline_home": ml_home,
                "moneyline_away": ml_away,
                "spread": spread,
                "spread_home_odds": s_home,
                "spread_away_odds": s_away,
                "total": total,
                "over_odds": o_odds,
                "under_odds": u_odds,
                "implied_home": ih,
                "implied_away": ia,
            })
        return out

    # ---- Example: list of game objects directly
    if isinstance(raw, list):
        for e in raw:
            game_id = e.get("id") or e.get("game_id") or e.get("msf_game_id")
            home = e.get("home_team") or e.get("home")
            away = e.get("away_team") or e.get("away")
            ml_home = e.get("moneyline_home") or e.get("home_ml")
            ml_away = e.get("moneyline_away") or e.get("away_ml")
            ih = _clip01(_implied_from_american(ml_home))
            ia = _clip01(_implied_from_american(ml_away))
            out.append({
                "provider": provider,
                "source_ts": ts,
                "game_id": game_id,
                "home_team": home,
                "away_team": away,
                "moneyline_home": ml_home,
                "moneyline_away": ml_away,
                "spread": e.get("spread"),
                "spread_home_odds": e.get("spread_home_odds"),
                "spread_away_odds": e.get("spread_away_odds"),
                "total": e.get("total"),
                "over_odds": e.get("over_odds"),
                "under_odds": e.get("under_odds"),
                "implied_home": ih,
                "implied_away": ia,
            })
        return out

    # Fallback: best-effort single mapping
    game_id = raw.get("id") or raw.get("game_id") or raw.get("msf_game_id")
    home = raw.get("home_team") or raw.get("home")
    away = raw.get("away_team") or raw.get("away")
    ml_home = raw.get("moneyline_home") or raw.get("home_ml")
    ml_away = raw.get("moneyline_away") or raw.get("away_ml")
    ih = _clip01(_implied_from_american(ml_home))
    ia = _clip01(_implied_from_american(ml_away))
    out.append({
        "provider": provider,
        "source_ts": ts,
        "game_id": game_id,
        "home_team": home,
        "away_team": away,
        "moneyline_home": ml_home,
        "moneyline_away": ml_away,
        "spread": raw.get("spread"),
        "spread_home_odds": raw.get("spread_home_odds"),
        "spread_away_odds": raw.get("spread_away_odds"),
        "total": raw.get("total"),
        "over_odds": raw.get("over_odds"),
        "under_odds": raw.get("under_odds"),
        "implied_home": ih,
        "implied_away": ia,
    })
    return out

# ---------- Public entry ----------
def fetch_odds(provider: str,
               url: str,
               params: Dict[str, Any],
               headers: Dict[str, str]) -> Optional[List[Dict[str, Any]]]:
    """
    Returns normalized odds rows or None (if STRICT_MODE=0 and provider failed).
    On failure in STRICT_MODE=1: raises.
    """
    session = requests.Session()
    last_err: Optional[Exception] = None

    for attempt in range(ODDS_RETRIES):
        try:
            raw = _call_odds_api(session, url, params, headers)
            normalized = _normalize_odds_schema(provider, raw)
            # Validate minimally
            if not isinstance(normalized, list) or len(normalized) == 0:
                raise ProviderParseError("Normalized odds are empty.")
            return normalized

        except ProviderHTTPError as e:
            last_err = e
            transient = (e.status in TRANSIENT_HTTP)
            if transient and attempt < ODDS_RETRIES - 1:
                time.sleep(ODDS_BACKOFF_BASE * (2 ** attempt))
                continue
            # log once and break
            _log_provider_error(provider, url, params, e.status, e.text_snippet, note="HTTP error")
            break

        except (ProviderParseError, ProviderError, requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            # Retry on networky errors; parse errors typically won't improve
            retryable = isinstance(e, (requests.Timeout, requests.ConnectionError))
            if retryable and attempt < ODDS_RETRIES - 1:
                time.sleep(ODDS_BACKOFF_BASE * (2 ** attempt))
                continue
            _log_provider_error(provider, url, params, None, None, note=str(e))
            break

    # If we reach here, it failed
    if STRICT_MODE:
        raise last_err or ProviderError("Unknown provider failure.")
    return None