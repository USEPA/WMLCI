"""
Shared helpers for wmlci/extract scripts.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import yaml

from wmlci.settings import extractpath, source_data_path

API_KEYS_ENV_PATH = extractpath / "API_Keys.env"


def _script_function(loader, module_name, node):
    """Resolve ``!script_function:module function_name`` to a callable."""
    module = importlib.import_module(f"wmlci.{module_name}")
    return getattr(module, loader.construct_scalar(node))


yaml.SafeLoader.add_multi_constructor("!script_function:", _script_function)


def load_extract_yaml(method_name: str) -> dict[str, Any]:
    """Load extract yaml for ``method_name``, resolving ``!script_function``."""
    path = extractpath / f"{method_name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Extract config not found: {path}")
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def source_data_dir(method_name: str, version: str | None = None) -> Path:
    """Return the local directory for a method's source data."""
    if version:
        return source_data_path / f"{method_name}_v{version.replace('.', '_')}"
    return source_data_path / method_name


def jsonld_source_dir(fname: str, version: str | None = None) -> Path:
    """Local directory containing JSON-LD for ``fname``."""
    if not (extractpath / f"{fname}.yaml").exists():
        return source_data_path / fname

    config = load_extract_yaml(fname)
    version = version or config.get("version")
    root = source_data_dir(fname, version)
    # Script products write into root; API downloads unzip into root/fname.
    return root if "script_function" in config else root / fname


def extract_source_data(method_name: str, version: str | None = None) -> Path:
    """
    Obtain source data using an extract yaml.

    Uses ``script_function`` when present; otherwise downloads from the
    yaml's configured URL.
    """
    config = load_extract_yaml(method_name)
    if "script_function" in config:
        from wmlci.extract.extract_source_data_from_script import (
            extract_source_data_from_script,
        )

        return extract_source_data_from_script(method_name, version=version)

    from wmlci.extract.download_source_data_from_api import download_source_data

    download_source_data(method_name, version=version)
    return jsonld_source_dir(method_name, version=version)
