from pathlib import Path
from esupy.processed_data_mgmt import Paths, mkdir_if_missing


MODULEPATH = Path(__file__).resolve().parent

datapath = MODULEPATH / 'data'
# warmdatapath = datapath / 'warm_v16_openlca_database_Mar2022'

# "Paths()" are a class defined in esupy
paths = Paths()
paths.local_path = paths.local_path / 'wmlci'
outputpath = paths.local_path
externalData = outputpath / 'externalData'

# ensure directories exist
mkdir_if_missing(externalData)
