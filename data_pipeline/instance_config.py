"""Explicit per-instance environment and storage configuration."""

from __future__ import annotations

import os
import sys
import fcntl
from pathlib import Path
from typing import MutableMapping, TextIO


VALID_INSTANCE_IDS = {"local", "vps"}
PROJECT_ROOT = Path(__file__).resolve().parent
_INSTANCE_KEYS = {
    "INSTANCE_ID",
    "DATA_DIR",
    "DATABASE_URL",
    "WATCHTOWER_DB_ENABLED",
    "WATCHTOWER_SYMBOLS",
    "WATCHTOWER_MAX_ENTRY_SPREAD",
    "WATCHTOWER_BREAKEVEN_ENABLED",
    "WATCHTOWER_BREAKEVEN_TRIGGER_R",
    "WATCHTOWER_STRUCTURAL_TRAILING_ENABLED",
    "WATCHTOWER_MIN_ENTRY_RR",
}
_INSTANCE_PREFIXES = ("DERIV_", "POSTGRES_")


def read_env_file(path: Path) -> dict[str, str]:
    """Read a small dotenv file without expanding or printing its contents."""
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def load_instance_environment(
    *,
    project_root: Path = PROJECT_ROOT,
    environ: MutableMapping[str, str] | None = None,
) -> str:
    """Load only the env file selected by INSTANCE_ID.

    On macOS developer machines, a direct local launch may omit INSTANCE_ID;
    in that case, default to the local instance only when its env file exists.
    Server/container launches remain explicit. Managed credentials and
    database settings from the ambient environment are cleared before values
    from the selected file are applied.
    """
    target_env = os.environ if environ is None else environ
    instance_id = target_env.get("INSTANCE_ID", "").strip().lower()
    if (
        not instance_id
        and environ is None
        and sys.platform == "darwin"
        and (Path(project_root) / ".env.local").is_file()
    ):
        instance_id = "local"
    if instance_id not in VALID_INSTANCE_IDS:
        raise RuntimeError("INSTANCE_ID must be set to 'local' or 'vps'.")

    # In containers Compose has already loaded the selected env_file. Reading
    # the owner-only host file again from the UID-mapped app user would both
    # fail permissions and unnecessarily expose the secret file in /app.
    if target_env.get("INSTANCE_ENV_PRELOADED") == "1":
        data_dir_value = target_env.get("DATA_DIR", "").strip()
        if not data_dir_value:
            raise RuntimeError("DATA_DIR must be supplied in the preloaded instance environment.")
        data_dir = Path(data_dir_value)
        if not data_dir.is_absolute():
            data_dir = Path(project_root) / data_dir
        data_dir = data_dir.resolve()
        if data_dir.name != instance_id or data_dir.parent.name != "data":
            raise RuntimeError(
                "DATA_DIR in the preloaded environment must resolve to an instance-specific data/<id> directory."
            )
        for key in ("DERIV_APP_ID", "DERIV_API_TOKEN", "DERIV_DEMO_ACCOUNT_ID"):
            if not target_env.get(key, "").strip():
                raise RuntimeError(f"{key} is missing from the preloaded instance environment.")
        target_env["DATA_DIR"] = str(data_dir)
        target_env["INSTANCE_ID"] = instance_id
        return instance_id

    env_path = Path(project_root) / f".env.{instance_id}"
    if not env_path.is_file():
        raise RuntimeError(f"Required instance environment file is missing: {env_path.name}")

    values = read_env_file(env_path)
    file_instance = values.get("INSTANCE_ID", "").strip().lower()
    if file_instance != instance_id:
        raise RuntimeError(
            f"INSTANCE_ID in {env_path.name} must be '{instance_id}'."
        )

    managed = set(_INSTANCE_KEYS)
    for key in tuple(target_env):
        if key in managed or key.startswith(_INSTANCE_PREFIXES):
            target_env.pop(key, None)

    for key, value in values.items():
        if key in managed or key.startswith(_INSTANCE_PREFIXES):
            target_env[key] = value

    data_dir_value = values.get("DATA_DIR", "").strip()
    if not data_dir_value:
        raise RuntimeError(f"DATA_DIR must be set in {env_path.name}.")
    data_dir = Path(data_dir_value)
    if not data_dir.is_absolute():
        data_dir = Path(project_root) / data_dir
    data_dir = data_dir.resolve()
    if data_dir.name != instance_id or data_dir.parent.name != "data":
        raise RuntimeError(
            f"DATA_DIR in {env_path.name} must resolve to an instance-specific data/<id> directory."
        )
    target_env["DATA_DIR"] = str(data_dir)
    target_env["INSTANCE_ID"] = instance_id
    return instance_id


def selected_data_dir() -> Path:
    """Return the configured path; fail closed for unconfigured runtimes."""
    instance_id = os.environ.get("INSTANCE_ID", "").strip().lower()
    value = os.environ.get("DATA_DIR", "").strip()
    if instance_id not in VALID_INSTANCE_IDS or not value:
        raise RuntimeError("Instance storage is not configured; set INSTANCE_ID and load its env file.")
    path = Path(value).resolve()
    if path.name != instance_id or path.parent.name != "data":
        raise RuntimeError("Configured data directory is not isolated for this WatchTower instance.")
    return path


def history_path(filename: str, legacy_path: Path) -> Path:
    """Resolve a journal path after runtime config, retaining test injection."""
    if legacy_path != Path(__file__).resolve().parent / filename:
        return legacy_path
    return selected_data_dir() / filename


def acquire_trader_lock(data_dir: Path | None = None) -> TextIO:
    """Hold an exclusive per-instance lock for the lifetime of one trader."""
    directory = data_dir or selected_data_dir()
    directory.mkdir(parents=True, exist_ok=True)
    lock_path = directory / ".watchtower-trader.lock"
    handle = lock_path.open("a+", encoding="utf-8")
    os.chmod(lock_path, 0o600)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError(
            "A WatchTower trader is already running for this instance."
        ) from exc
    return handle
