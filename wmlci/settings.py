import os
from pathlib import Path

from esupy.processed_data_mgmt import Paths, mkdir_if_missing
from esupy.util import get_git_hash, return_pkg_version

MODULEPATH = Path(__file__).resolve().parent

datapath = MODULEPATH / "data"

# Extract configs and API scripts
extractpath = MODULEPATH / "extract"

source_data_path = datapath / "source_data"
epa_data_commons_path = source_data_path / "epa_data_commons"
resultspath = datapath / "results"
logoutputpath = datapath / "logs"
error_logs_path = datapath / "error_logs"

# "Paths()" are a class defined in esupy
# esupy Paths — remote Data Commons URL only (not local storage)
paths = Paths()

# ensure directories exist
for d in [
    source_data_path,
    epa_data_commons_path,
    resultspath,
    logoutputpath,
    error_logs_path,
]:
    mkdir_if_missing(d)

# Common declaration of write format for package data products
WRITE_FORMAT = "csv"  # todo: change to parquet?
# Identify python package version
PKG_VERSION_NUMBER = return_pkg_version(MODULEPATH, "WMLCI")

GIT_HASH_LONG = os.environ.get("GITHUB_SHA") or get_git_hash("long")
if GIT_HASH_LONG:
    GIT_HASH = GIT_HASH_LONG[0:7]
else:
    GIT_HASH = None
