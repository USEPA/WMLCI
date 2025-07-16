from datetime import datetime
import os
from pathlib import Path

from esupy.processed_data_mgmt import Paths, mkdir_if_missing, FileMeta, write_metadata_to_file
from esupy.util import get_git_hash, return_pkg_version


MODULEPATH = Path(__file__).resolve().parent

datapath = MODULEPATH / 'data'
sourcedatapath = datapath / 'sourceData'

# "Paths()" are a class defined in esupy
paths = Paths()
paths.local_path = paths.local_path / 'wmlci'
outputpath = paths.local_path
externalData = outputpath / 'externalData'

# ensure directories exist
mkdir_if_missing(externalData)

# Common declaration of write format for package data products
WRITE_FORMAT = "csv" #todo: change to parquet?
# Identify python package version
PKG_VERSION_NUMBER = return_pkg_version(MODULEPATH, 'WMLCI')

GIT_HASH_LONG = os.environ.get('GITHUB_SHA') or get_git_hash('long')
if GIT_HASH_LONG:
    GIT_HASH = GIT_HASH_LONG[0:7]
else:
    GIT_HASH = None

def set_wmlci_meta(file_name, wmlciformat=''):
    """Create a class of esupy FileMeta with stewiformat assigned as category."""
    wmlci_meta = FileMeta()
    wmlci_meta.name_data = file_name
    wmlci_meta.category = wmlciformat
    wmlci_meta.tool = "WMLCI"
    wmlci_meta.tool_version = PKG_VERSION_NUMBER
    wmlci_meta.ext = WRITE_FORMAT
    wmlci_meta.git_hash = GIT_HASH
    wmlci_meta.date_created = datetime.now().strftime('%d-%b-%Y')
    return wmlci_meta


def write_wmlci_metadata(source_name, df_meta):
    """
    Write the metadata and output as a JSON in a local directory
    :param source_name: string, source name for either a FBA or FBS dataset
    :param df_meta: object, metadata
    :return: object, metadata that includes package url at time of development
    """
    # create empty dictionary
    df_dict = {}
    # add url of method at time of commit
    df_dict['method_url'] = \
        f'https://github.com/USEPA/WMLCI/blob/{GIT_HASH_LONG}/lfg_calc_py/' \
        f'methods/{source_name}.yaml'

    # append url to df metadata
    df_meta.tool_meta = df_dict

    write_metadata_to_file(paths, df_meta)

    return df_dict
