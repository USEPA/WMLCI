"""
Functions to document metadata
"""

from __future__ import annotations

import json
from datetime import datetime

from esupy.processed_data_mgmt import FileMeta

from wmlci.settings import (
    GIT_HASH,
    GIT_HASH_LONG,
    PKG,
    PKG_VERSION_NUMBER,
    WRITE_FORMAT,
)


def set_meta(name_data):
    """
    Create metadata for downloaded source data.
    :param name_data: string, name of data
    :return: object, WMLCI metadata
    """
    wmlci_meta = FileMeta()
    wmlci_meta.tool = PKG
    wmlci_meta.name_data = name_data
    wmlci_meta.tool_version = PKG_VERSION_NUMBER
    wmlci_meta.git_hash = GIT_HASH
    wmlci_meta.ext = WRITE_FORMAT
    wmlci_meta.date_created = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return wmlci_meta


def return_method_meta(config):
    """
    Return metadata fields from an extract method yaml.
    :return: dict for tool_meta
    """
    method_meta = {}

    for k, v in config.items():
        if k in (
            'author',
            'source_name',
            'source_url',
            'api_name',
            'format',
            'api_key_required',
        ):
            if isinstance(v, dict):
                continue
            method_meta[k] = str(v)

    method_meta['date_accessed'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return method_meta


def write_metadata(source_name, config, wmlci_meta, pth):
    """
    Write metadata as JSON - stored locally within repo
    :param source_name: string, extract method name
    :param config: dictionary, extract yaml
    :param wmlci_meta: object, WMLCI metadata
    :param pth: str, output directory
    :return: path to metadata json
    """
    wmlci_meta.tool_meta = return_method_meta(config)
    wmlci_meta.tool_meta['method_url'] = (
        f'https://github.com/USEPA/WMLCI/blob/{GIT_HASH_LONG}/wmlci/'
        f'extract/{source_name}.yaml'
    )
    fname = f'{wmlci_meta.name_data}_metadata.json'
    meta_path = f'{pth}/{fname}'
    with open(meta_path, 'w', encoding='utf-8') as fi:
        fi.write(json.dumps(wmlci_meta.__dict__, indent=4))
    return meta_path
