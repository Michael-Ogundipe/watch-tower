"""Validated settings for one explicitly selected WatchTower instance."""

import math
import os

from instance_config import load_instance_environment


INSTANCE_ID = load_instance_environment()
DATA_DIR = os.environ["DATA_DIR"]


def _required(name: str) -> str:
    """A credential that has no safe default: missing means stop, not guess."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"{name} is not set. Add it to the selected .env.local or .env.vps "
            f"(see .env.example), or export it before starting Watch Tower."
        )
    return value


APP_ID = _required("DERIV_APP_ID")
API_TOKEN = _required("DERIV_API_TOKEN")
DEMO_ACCOUNT_ID = _required("DERIV_DEMO_ACCOUNT_ID")

# Symbols Watch Tower watches concurrently. Each one gets its own
# independent LiveTrader (own candles/confluence/pending-signal/open
# trade), so e.g. JD75 and R_75 can each be in a trade at the same time.
#
# NOTE ON SYMBOL CODES:
# Deriv's actual API symbol for "Volatility 75 Index" (what people call
# "V_75") is "R_75" -- that's what get_candles()/subscribe_ticks() expect,
# and it's the same symbol already used as the default in live_trader.py.
# "JD75" / "JD10" are the API codes Deriv uses for Jump 75 / Jump 10.
#
# Before going live, confirm these are exactly right for your account by
# running list_symbols.py, which queries Deriv's own active_symbols list.
SYMBOLS = [
    "JD75",
    "JD25",
    "R_75",  # "V_75" / Volatility 75 Index
]


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized not in {"1", "true", "yes", "0", "false", "no"}:
        raise RuntimeError(f"{name} must be a boolean value.")
    return normalized in {"1", "true", "yes"}


try:
    MAX_ENTRY_SPREAD = float(os.environ.get("WATCHTOWER_MAX_ENTRY_SPREAD", "1.0"))
    BREAKEVEN_TRIGGER_R = float(os.environ.get("WATCHTOWER_BREAKEVEN_TRIGGER_R", "1.5"))
    MIN_ENTRY_RR = float(os.environ.get("WATCHTOWER_MIN_ENTRY_RR", "1.0"))
except ValueError as exc:
    raise RuntimeError("WatchTower execution thresholds must be numeric.") from exc

BREAKEVEN_ENABLED = _env_bool("WATCHTOWER_BREAKEVEN_ENABLED", True)
STRUCTURAL_TRAILING_ENABLED = _env_bool("WATCHTOWER_STRUCTURAL_TRAILING_ENABLED", False)
if (
    not math.isfinite(MAX_ENTRY_SPREAD)
    or not math.isfinite(BREAKEVEN_TRIGGER_R)
    or not math.isfinite(MIN_ENTRY_RR)
    or MAX_ENTRY_SPREAD <= 0
    or BREAKEVEN_TRIGGER_R <= 0
    or MIN_ENTRY_RR < 0
):
    raise RuntimeError("Spread/breakeven thresholds must be positive and minimum entry R:R cannot be negative.")

# One place to override the watch list without editing code, for the VPS.
_symbols_override = os.environ.get("WATCHTOWER_SYMBOLS", "").strip()
if _symbols_override:
    SYMBOLS = [s.strip() for s in _symbols_override.split(",") if s.strip()]
