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
from datetime import datetime
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


def source_data_dir(method_name: str, version: str | None = None) -> Path:
    """Return the parent directory for a method's source data."""
    if version is None:
        version = _load_config(method_name).get("version")
    if version:
        return source_data_path / f"{method_name}_v{version.replace('.', '_')}"
    return source_data_path / method_name


def _fetch_flcac_source_metadata(
    config: dict[str, Any], version: str
) -> tuple[str, str | None]:
    """Return FLCAC source metadata for a version
       returns: commit_id, date_published"""
    source_url = config.get("source_url", "")
    if "/lca-collaboration/" not in source_url:
        raise ValueError(f"Cannot parse group/repo from source_url: {source_url}")
    parts = source_url.split("/lca-collaboration/", 1)[1].split("/")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse group/repo from source_url: {source_url}")
    group, repo = parts[0], parts[1]
    repo_url = _build_url(
        {
            **(config.get("url") or {}),
            "api_path": f"/repository/{group}/{repo}",
            "url_params": {"api_key": "__apiKey__"},
        },
        {"apiKey": _api_key(config)},
    )
    repo_info = _request(repo_url).json()
    for release in repo_info.get("releases", []):
        if release.get("version") != version:
            continue
        commit_id = str(release["id"])
        settings = repo_info.get("settings", {})
        release_date = release.get("releaseDate") or (
            settings.get("releaseDate")
            if settings.get("version") == version
            else None
        )
        if release_date:
            date_published = datetime.fromtimestamp(
                int(release_date) / 1000
            ).strftime("%Y-%m-%d %H:%M:%S")
            return commit_id, date_published
        return commit_id, None
    group_repo = repo_info.get("settings", {}).get("repositoryPath", f"{group}/{repo}")
    raise ValueError(f"Version {version!r} not found in releases for {group_repo}")


def _call_url_and_download_data(
    config: dict[str, Any],
    out_dir: Path,
    method_name: str,
    commit_id: str | None = None,
) -> Path:
    shared_url = config.get("url") or {}
    steps = config.get("download_steps")
    subs: dict[str, str] = {}
    if config.get("api_name"):
        subs["apiKey"] = _api_key(config)

    if steps:
        for step in steps[:-1]:
            step_url = {**shared_url, **(step.get("url") or {})}
            if commit_id:
                url_params = dict(step_url.get("url_params") or {})
                url_params["commitId"] = commit_id
                step_url["url_params"] = url_params
            prepare_url = _build_url(step_url, subs)
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


def download_source_data(method_name: str, version: str | None = None) -> Path:
    """Download (and optionally unzip) source data for an extract method yaml."""
    config = _load_config(method_name)
    version = version or config.get("version")
    out_dir = source_data_dir(method_name, version)
    mkdir_if_missing(out_dir)

    commit_id = None
    date_published = None
    if version:
        commit_id, date_published = _fetch_flcac_source_metadata(config, version)
        source_name = config.get("source_name", method_name)
        log.info(f"Returning version {version} of {source_name}")

    out_path = _call_url_and_download_data(
        config, out_dir, method_name, commit_id=commit_id
    )

    if date_published:
        config["date_published"] = date_published

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
