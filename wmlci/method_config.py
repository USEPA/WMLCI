"""
Load WMLCI LCA method YAML configurations.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

METHODS_DIR = Path(__file__).resolve().parent / "methods"


def load_method_config(method_name: str) -> dict[str, Any]:
    """Load a method YAML from ``wmlci/methods/{method_name}.yaml``."""
    path = METHODS_DIR / f"{method_name}.yaml"
    if not path.exists():
        available = sorted(p.stem for p in METHODS_DIR.glob("*.yaml"))
        raise FileNotFoundError(
            f"Method config '{method_name}' not found at {path}. "
            f"Available methods: {available}"
        )

    with path.open(encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    if "method_name" not in config:
        config["method_name"] = method_name

    defaults = deepcopy(config.get("model_defaults") or {})
    processes = config.get("processes")
    if not processes:
        raise ValueError(
            f"Method '{method_name}' must define processes, "
            "include all processes to model."
        )

    config["processes"] = {
        name: _apply_system_specific_settings(defaults, overrides)
        for name, overrides in processes.items()
    }
    return config


def _apply_system_specific_settings(
    defaults: dict[str, Any], overrides: dict[str, Any] | None
) -> dict[str, Any]:
    """Merge per-system overrides into model_defaults (flowsa-style)."""
    merged = deepcopy(defaults)
    if not overrides:
        return merged

    for key, value in overrides.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = {**merged[key], **deepcopy(value)}
        else:
            merged[key] = deepcopy(value)
    return merged
