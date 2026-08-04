import os
import subprocess
import tomllib
from importlib.metadata import version
from pathlib import Path

from esupy.processed_data_mgmt import Paths, mkdir_if_missing

MODULEPATH = Path(__file__).resolve().parent

datapath = MODULEPATH / "data"

extractpath = MODULEPATH / "extract"

model_defaults_path = MODULEPATH / "utils" / "model_defaults"

source_data_path = datapath / "source_data"
resultspath = datapath / "results"
logoutputpath = datapath / "logs"
error_logs_path = datapath / "error_logs"

# "Paths()" are a class defined in esupy
paths = Paths()
paths.local_path = datapath

# ensure directories exist
for d in [
    source_data_path,
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


def versioned_filename(name: str) -> str:
    """
    Append package version and git hash (esupy / flowsa style).

    ``name.csv`` -> ``name_v{version}_{githash}.csv``
    """
    path = Path(name)
    stem = path.stem
    suffix = path.suffix
    out = f"{stem}_v{PKG_VERSION_NUMBER}"
    if GIT_HASH:
        out = f"{out}_{GIT_HASH}"
    return f"{out}{suffix}"


def find_versioned_file(directory: Path, base_name: str) -> Path | None:
    """
    Locate a result file written with ``versioned_filename``.

    Preference order: newest versioned match by mtime, then the unversioned
    base name.
    """
    directory = Path(directory)
    base = Path(base_name)
    stem, suffix = base.stem, base.suffix

    matches = sorted(
        directory.glob(f"{stem}_v*{suffix}"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if matches:
        return matches[0]

    plain = directory / base_name
    if plain.exists():
        return plain
    return None
