import os
import subprocess
import tomllib
from importlib.metadata import version
from pathlib import Path

from esupy.processed_data_mgmt import Paths, mkdir_if_missing

MODULEPATH = Path(__file__).resolve().parent

datapath = MODULEPATH / "data"

extractpath = MODULEPATH / "extract"

source_data_path = datapath / "source_data"
epa_data_commons_path = source_data_path / "epa_data_commons"
resultspath = datapath / "results"
logoutputpath = datapath / "logs"
error_logs_path = datapath / "error_logs"

# "Paths()" are a class defined in esupy
paths = Paths()
paths.local_path = datapath

# ensure directories exist
for d in [
    source_data_path,
    epa_data_commons_path,
    resultspath,
    logoutputpath,
    error_logs_path,
]:
    mkdir_if_missing(d)


def return_pkg_version(MODULEPATH: Path, package_name: str) -> str:
    """
    Return package version, first look for git tag, then look for installed package version
    :param MODULEPATH: str, package path
    :param packagename: str, such as "wmlci"
    """

    # return version with git describe
    try:
        # set path to package repository, necessary if running method files
        # outside the package repo
        tags = (
            subprocess.check_output(
                ['git', 'describe', '--tags', '--always', '--match', 'v[0-9]*'],
                cwd=MODULEPATH,
            )
            .decode()
            .strip()
        )

        if tags.startswith('v'):
            return tags.split('-', 1)[0].replace('v', '')

    # If it's a hash, pass
    except subprocess.CalledProcessError:
        pass

    # else return installed package version
    try:
        return version(package_name)
    except Exception:
        with (MODULEPATH.parent / 'pyproject.toml').open('rb') as f:
            return tomllib.load(f)['project']['version']


def get_git_hash(MODULEPATH: Path, length: str = 'short') -> str | None:
    """
    Returns git_hash of current directory or None if no git found
    :param MODULEPATH: Path, module path
    :param length: str, 'short' for 7-digit, 'long' for full git hash
    :return git_hash: str
    """
    try:
        git_hash = (
            subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=MODULEPATH)
            .decode()
            .strip()
        )

        return git_hash if length == 'long' else git_hash[:7]

    except Exception:
        return None


# metadata
PKG = 'wmlci'
PKG_VERSION_NUMBER = return_pkg_version(MODULEPATH, PKG)
GIT_HASH_LONG = os.environ.get('GITHUB_SHA') or get_git_hash(MODULEPATH, 'long')
GIT_HASH = GIT_HASH_LONG[:7] if GIT_HASH_LONG else None

# Common declaration of write format for package data products
WRITE_FORMAT = "csv"  # todo: change to parquet?
