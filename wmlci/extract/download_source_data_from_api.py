"""
Download source data from APIs.

YAML configs: ``wmlci/extract/{method_name}.yaml``.
Files are saved under ``wmlci/data/source_data/{method_name}/``.
"""

from __future__ import annotations

import os
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Any
from urllib import parse

import yaml
from dotenv import load_dotenv
from esupy.processed_data_mgmt import mkdir_if_missing
from esupy.remote import make_url_request

from wmlci.log import log
from wmlci.metadata import set_meta, write_metadata
from wmlci.settings import source_data_path

EXTRACTPATH = Path(__file__).resolve().parent
API_KEYS_ENV_PATH = EXTRACTPATH / "API_Keys.env"


class APIError(Exception):
    """Raised when a required API key is missing from API_Keys.env."""

    def __init__(self, api_source: str) -> None:
        super().__init__(
            f"API key '{api_source}' not found in {API_KEYS_ENV_PATH}. "
            "Add key to API_Keys.env."
        )


def _load_config(method_name: str) -> dict[str, Any]:
    path = EXTRACTPATH / f"{method_name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Extract config not found: {path}")
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _api_key(config: dict[str, Any]) -> str:
    load_dotenv(API_KEYS_ENV_PATH, verbose=True)
    name = config["api_name"]
    value = os.getenv(name)
    if value is None:
        raise APIError(name)
    return value


def _replace_url_params(url: str, subs: dict[str, str]) -> str:
    for key, value in subs.items():
        url = url.replace(f"__{key}__", value)
    return url


def _build_url(urlinfo: dict[str, Any], subs: dict[str, str]) -> str:
    base = _replace_url_params(urlinfo.get("base_url", ""), subs)
    path = _replace_url_params(urlinfo.get("api_path", ""), subs)
    if path and not path.startswith("/"):
        path = "/" + path
    url = base.rstrip("/") + path

    url_params = urlinfo.get("url_params")
    if url_params:
        encoded = {
            k: _replace_url_params(str(v), subs) for k, v in url_params.items()
        }
        url = url + "?" + parse.urlencode(encoded, safe="=&%", quote_via=parse.quote)
    return url


def _request(url: str) -> Any:
    log.info(f"Calling {url}")
    resp = make_url_request(url, verify=False)
    if resp is None:
        raise RuntimeError(f"No response from {url}")
    if resp.status_code != 200:
        raise RuntimeError(f"Request failed ({resp.status_code}): {resp.text[:500]}")
    return resp


def _read_token(resp) -> str:
    try:
        payload = resp.json()
        if isinstance(payload, dict) and payload.get("token"):
            return str(payload["token"])
    except ValueError:
        pass
    token = resp.text.strip()
    if not token:
        raise RuntimeError("No token in prepare response")
    return token


def _call_url_and_download_data(config: dict[str, Any], out_dir: Path) -> Path:
    shared_url = config.get("url") or {}
    steps = config.get("download_steps")
    subs: dict[str, str] = {}
    if config.get("api_name"):
        subs["apiKey"] = _api_key(config)

    if steps:
        for step in steps[:-1]:
            prepare_url = _build_url({**shared_url, **(step.get("url") or {})}, subs)
            token_key = step.get("response_as", "token")
            subs[token_key] = parse.quote(_read_token(_request(prepare_url)), safe="")

        last = steps[-1]
        url = _build_url({**shared_url, **(last.get("url") or {})}, subs)
        filename = (
            last.get("filename")
            or config.get("filename")
            or f"{method_name}.zip"
        )
        unzip = last.get("unzip", config.get("unzip", False))
    else:
        url = _build_url(shared_url, subs)
        filename = config.get("filename") or f"{method_name}.zip"
        unzip = config.get("unzip", False)

    out_path = out_dir / filename
    resp = _request(url)
    out_path.write_bytes(resp.content)
    log.info(f"Saved {out_path} ({len(resp.content)} bytes)")

    if unzip:
        if out_path.suffix.lower() != ".zip":
            raise ValueError(f"unzip: true but {filename} is not a zip file")
        extract_dir = out_path.parent / out_path.stem
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True)
        with zipfile.ZipFile(out_path, "r") as zf:
            zf.extractall(extract_dir)
        log.info(f"Extracted {out_path.name} to {extract_dir}")

    return out_path


def download_source_data(method_name: str) -> Path:
    """Download (and optionally unzip) source data for an extract method yaml."""
    config = _load_config(method_name)
    out_dir = source_data_path / method_name
    mkdir_if_missing(out_dir)
    out_path = _call_url_and_download_data(config, out_dir)

    meta = set_meta(method_name)
    meta.ext = out_path.suffix.lstrip(".") or "zip"
    meta_path = write_metadata(
        method_name,
        config,
        meta,
        str(out_dir),
    )
    log.info(f"Wrote metadata to {meta_path}")

    return out_path
