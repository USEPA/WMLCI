"""
Functions to document metadata
"""

from datetime import datetime

from esupy.processed_data_mgmt import FileMeta, write_metadata_to_file
from wmlci.settings import PKG_VERSION_NUMBER, WRITE_FORMAT, GIT_HASH, GIT_HASH_LONG, paths


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
