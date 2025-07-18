
import zipfile

from bw2io.importers.json_ld import JSONLDImporter

from wmlci.settings import paths, sourcedatapath
from wmlci.wmlci_log import log

from esupy.remote import make_url_request
from esupy.util import make_uuid
from esupy.processed_data_mgmt import download_from_remote, Paths, mkdir_if_missing
"""
Functions common across datasets
"""

def assign_uuid():
    """
    Assign new UUID based on name/location/category fields. Uses EPA's useupy package for assignment
    :return:
    """

    # todo: edit for bw code - pulled example code from fedelemflowlist repo

    # # option 1 - class
    # # set the UUID or generate it from the attributes
    # if self.uid is None:
    #     flow_ref.id = make_uuid("Flow", self.category, self.name)
    # else:
    #     flow_ref.id = self.uid
    #
    # # option 2 - for loop
    # # Loop through flows generating UUID for each
    # flowids = []
    # log.info('Generating unique UUIDs for each flow...')
    # for index, row in flows.iterrows():
    #     flowid = make_uuid(row['Flowable'], row['Context'], row['Unit'])
    #     flowids.append(flowid)
    # flows['Flow UUID'] = flowids

    # return

def download_source_data_from_remote(fname):
    """
    Download source data stored from USEPA's data commons to local directory
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
        folder = paths.local_path / 'sourceData'
        mkdir_if_missing(folder)
        file = folder / fname
        with file.open('wb') as fi:
            fi.write(r.content)
        log.info(f'{fname} downloaded from '
                 f'{paths.remote_path}index.html?prefix=WMLCI/sourceData'
                 f' and saved to {folder}')

    return status


def load_JSONLD_sourceData(fname, bw_database_name='db'):
    """
    Load sourceData file. Checks for file in local directory, if does not exist, pulls file from USEPA's Data Commons
    :param fname: str, filename for source data
    :param bw_database_name: str, set database name, default name set to 'db'
    :return:
    """
    # define path to source data
    filepath = sourcedatapath / fname

    # load jsonld source data from local directory. If data not found locally, download first, then load
    if not filepath.exists():
        download_source_data_from_remote(f"{fname}.zip")
        with zipfile.ZipFile(f"{filepath}.zip", 'r') as zip_ref:
            zip_ref.extractall(filepath)
        log.info(f"Unzipped {fname} to {sourcedatapath}")

    jsonld = JSONLDImporter(filepath, bw_database_name)

    return jsonld




