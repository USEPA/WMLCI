import os
from pathlib import Path

from esupy.processed_data_mgmt import Paths, mkdir_if_missing
from esupy.util import get_git_hash, return_pkg_version


MODULEPATH = Path(__file__).resolve().parent

datapath = MODULEPATH / 'data'

# "Paths()" are a class defined in esupy
paths = Paths()
paths.local_path = paths.local_path / 'wmlci'
outputpath = paths.local_path
sourcedatapath = outputpath / 'sourceData'
wmlcioutputpath = outputpath / 'WMLCI'
logoutputpath = outputpath / 'Logs'

# ensure directories exist
for d in [sourcedatapath,wmlcioutputpath, logoutputpath]:
    mkdir_if_missing(d)

# Common declaration of write format for package data products
WRITE_FORMAT = "csv" #todo: change to parquet?
# Identify python package version
PKG_VERSION_NUMBER = return_pkg_version(MODULEPATH, 'WMLCI')

GIT_HASH_LONG = os.environ.get('GITHUB_SHA') or get_git_hash('long')
if GIT_HASH_LONG:
    GIT_HASH = GIT_HASH_LONG[0:7]
else:
    GIT_HASH = None
