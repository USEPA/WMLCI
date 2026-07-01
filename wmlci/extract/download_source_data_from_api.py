"""
Download source files from HTTP APIs.

Loads YAML configs from ``wmlci/extract/``, assembles URLs (bedrock /
flowsa ``generateflowbyactivity`` pattern), and saves responses under
``wmlci/data/source_data/{source}/``.
"""

from __future__ import annotations

import argparse
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib import parse

import yaml
from dotenv import load_dotenv
from esupy.processed_data_mgmt import mkdir_if_missing
from esupy.remote import make_url_request

from wmlci.log import log
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


def api_key(config: dict[str, Any]) -> str:
    """Load ``config['api_name']`` from ``API_Keys.env``."""
    load_dotenv(API_KEYS_ENV_PATH, verbose=True)
    name = config["api_name"]
    value = os.getenv(name)
    if value is None:
        raise APIError(name)
    return value


def load_extract_config(source: str) -> dict[str, Any]:
    """Load ``wmlci/extract/{source}.yaml``."""
    path = EXTRACTPATH / f"{source}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Extract config not found: {path}")
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def assemble_urls_for_query(
    *,
    source: str,
    year: str | None,
    config: dict[str, Any],
) -> list[str | None]:
    """
    replace parts of the url string
    :param source: str, data source
    :param year: str, year
    :param config: dictionary, FBA yaml
    :return: list, urls to call data from
    """
    # if there are url parameters defined in the yaml,
    # then build a url, else use "base_url"
    urlinfo = config.get('url', 'None')
    if urlinfo == 'None':
        return [None]

    if 'url_params' in urlinfo:
        params = parse.urlencode(
            urlinfo['url_params'], safe='=&%', quote_via=parse.quote
        )
        build_url = urlinfo['base_url'] + urlinfo['api_path'] + params
    else:
        build_url = urlinfo['base_url']

    # substitute year from arguments and users api key into the url
    build_url = build_url.replace('__year__', str(year))
    if '__apiKey__' in build_url:
        userAPIKey = api_key(config)
        build_url = build_url.replace('__apiKey__', userAPIKey)

    fxn = config.get('url_replace_fxn')
    if callable(fxn):
        urls = fxn(build_url=build_url, source=source, year=year, config=config)
        return urls
    return [build_url]


def _filename_from_response(resp, url: str, source: str) -> str:
    """Use API-provided filename; fall back to the yaml source name."""
    content_disposition = resp.headers.get('Content-Disposition') or ''
    match = re.search(
        r"filename\*?=(?:UTF-8''|utf-8'')?\"?([^\";]+)\"?",
        content_disposition,
        re.I,
    )
    if match:
        return parse.unquote(match.group(1).strip())

    path_name = Path(parse.unquote(parse.urlparse(url).path)).name
    generic_names = {'search', 'json', 'prepare', 'browse', 'file', 'usage'}
    if path_name and path_name.lower() not in generic_names:
        if '.' in path_name:
            return path_name
        return f"{path_name}.jsonld"

    content_type = (resp.headers.get('Content-Type') or '').lower()
    if 'zip' in content_type:
        return f"{source}.zip"
    if 'json' in content_type:
        return f"{source}.jsonld"
    return source


def call_urls(
    *,
    url_list: list[str | None],
    source: str,
    year: str | None,
    config: dict[str, Any],
) -> list[Path]:
    """
    Call each URL and save the raw response to disk.

    Bedrock ``call_urls`` without ``call_response_fxn`` — save only, no parsing.
    """
    if not url_list or url_list[0] is None:
        return []

    out_dir = source_data_path / source
    mkdir_if_missing(out_dir)
    pause = config.get('time_delay', 0)
    set_cookies = config.get('allow_http_request_cookies')

    saved: list[Path] = []
    for url in url_list:
        log.info(f"Calling {url}")
        resp = make_url_request(
            url,
            set_cookies=set_cookies,
            verify=False,
        )
        if resp is None:
            log.warning(f"No response for {url}")
            continue

        out_path = out_dir / _filename_from_response(resp, url, source)
        mkdir_if_missing(out_path.parent)
        out_path.write_bytes(resp.content)
        log.info(f"Saved {out_path} ({len(resp.content)} bytes)")
        saved.append(out_path)
        if pause:
            time.sleep(pause)

    return saved


def parse_args() -> dict[str, Any]:
    """Make year and source script parameters (bedrock / flowsa pattern)."""
    ap = argparse.ArgumentParser()
    ap.add_argument(
        '-y', '--year', default=None, help='Year for data pull and save'
    )
    ap.add_argument(
        '-s', '--source', required=True, help='Data source code to pull and save'
    )
    return vars(ap.parse_args())


def main(**kwargs: Any) -> list[Path]:
    if not kwargs:
        kwargs = parse_args()

    source = kwargs['source']
    year = kwargs.get('year')
    config = load_extract_config(source)
    urls = assemble_urls_for_query(source=source, year=year, config=config)
    return call_urls(url_list=urls, source=source, year=year, config=config)


if __name__ == '__main__':
    main()
