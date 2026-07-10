"""
Functions common across datasets
"""

import zipfile
from pathlib import Path

from bw2io.importers.json_ld import JSONLDImporter
from bw2io.importers.json_ld_lcia import JSONLDLCIAImporter

from wmlci.settings import epa_data_commons_path, extractpath, paths
from wmlci.extract.download_source_data_from_api import (
    download_source_data,
    source_data_dir,
)
from wmlci.log import log
from wmlci.editImporter import *
from wmlci.errorLogging import *

from esupy.remote import make_url_request
from esupy.processed_data_mgmt import mkdir_if_missing


def _jsonld_source_dir(fname: str, version: str | None = None) -> Path:
    """Local directory containing JSON-LD for ``fname``."""
    if (extractpath / f"{fname}.yaml").exists():
        return source_data_dir(fname, version) / fname
    return epa_data_commons_path / fname


def download_source_data_from_remote(fname):
    """
    Download source data stored from USEPA's data commons to local directory
    (``wmlci/data/source_data/epa_data_commons/``)
    :param fname: str, filename, must include extension (such as .zip)
    :return:
    """

    status = False
    base_url = f"{paths.remote_path}WMLCI/sourceData/"
    url = base_url + fname
    r = make_url_request(url)
    if r is not None:
        status = True
        # set subdirectory
        folder = epa_data_commons_path
        mkdir_if_missing(folder)
        file = folder / fname
        with file.open('wb') as fi:
            fi.write(r.content)
        log.info(f'{fname} downloaded from '
                 f'{paths.remote_path}index.html?prefix=WMLCI/sourceData'
                 f' and saved to {folder}')

    return status


def load_JSONLD_sourceData(
    fname, datatype="jsonld", bw_database_name="db", data_version=None
):
    """
    Load sourceData file. If missing locally, download from an extract API yaml
    when ``wmlci/extract/{fname}.yaml`` exists, otherwise from EPA Data Commons.
    """
    filepath = _jsonld_source_dir(fname, version=data_version)

    if not filepath.exists():
        if (extractpath / f"{fname}.yaml").exists():
            download_source_data(fname, version=data_version)
        else:
            download_source_data_from_remote(f"{fname}.zip")
            with zipfile.ZipFile(epa_data_commons_path / f"{fname}.zip", 'r') as zip_ref:
                zip_ref.extractall(filepath)
            log.info(f"Unzipped {fname} to {epa_data_commons_path}")

    if datatype == 'jsonld':
        log.info(f"Loading {filepath}")
        jsonld = JSONLDImporter(filepath, bw_database_name)
    elif datatype == 'jsonld_lcia':
        log.info(f"Loading {filepath}")
        jsonld = JSONLDLCIAImporter(filepath)
    else:
        log.error("Specify data type as 'jsonld' or 'jsonld_lcia'")

    return jsonld


def clean_JSONLD_sourceData(jsonld):
    """
    Apply standard cleaning functions after loading JSONLD data that address common issues in imported data.
    This function should be run before bw apply_strategies().
    :param fname:
    :param datatype:
    :param bw_database_name:
    :return:
    """
    # map UUIDs to the federal elementary flowlist UUIDs
    jsonld = map_to_fedelemflowlist_UUIDs(jsonld, sourcelistname="WARM")
    # Apply the Opposite Direction Approach for waste management
    jsonld = apply_opposite_direction_approach(jsonld)
    # Replace location dictionary with a single entry for the US
    jsonld = reset_location_dict(jsonld)
    # Set all process locations to US
    jsonld = replace_process_location(jsonld)
    # Set all exchange locations to US
    jsonld = replace_exchange_locations(jsonld)
    # drop allocation factors of 1 due to missing exchange info causing error
    jsonld = remove_process_allocation_factors(jsonld)
    # Remove exchanges and processes with no impacts
    remove_impact_free_objects(jsonld)
    # Convert parameters list to dictionary
    jsonld = convert_param_list_to_dict(jsonld)

    return jsonld
