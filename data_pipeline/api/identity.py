"""Strict instance identity for the dashboard API."""

from __future__ import annotations

import os
from collections.abc import Mapping


def configured_instance_id(environ: Mapping[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    instance_id = values.get("INSTANCE_ID", "").strip().lower()
    if instance_id not in {"local", "vps"}:
        raise RuntimeError("API requires INSTANCE_ID=local or INSTANCE_ID=vps.")
    return instance_id
