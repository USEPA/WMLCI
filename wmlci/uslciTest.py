"""
import USLCI process: Crude oil, production mixture, at extraction
"""
## Dependencies ##
import bw2data as bd
from bw2io.importers.json_ld import JSONLDImporter
from wmlci.settings import datapath
from bw2io.strategies import *

from wmlci.common import *

## Project Initiation ##

# Set active project
bd.projects.set_current("usLciTest")
# Import crude oilt process
path = f'{datapath}/crudeOilProductionMixtureAtExtraction'
uslci_crudeOil = JSONLDImporter(path, "crudeOilProductionMixtureAtExtraction")
uslci_crudeOil.extractor() # Parse JSON-LD

# Checking basic data requirements
print(type(uslci_crudeOil.data)) # confirming data type is a dictionary
print(len(uslci_crudeOil.data)) # confirming the expected number (7) of components have been extracted to 'data'
print(uslci_crudeOil.data.keys()) # listing the keys


## Error fixing ##
# fixing issues in JSON-LD before calling apply_strategies()

# change 'isInput' key in exchanges to 'input' as expected by BW
uslci_crudeOil = correct_jsonld_input_key(uslci_crudeOil)

# change output exchanges that are not ref flow to TECHNOSPHERE_FLOW
uslci_crudeOil = edit_non_quant_ref_flow_type(uslci_crudeOil)

# apply opposite direction approach to waste treatment processes
uslci_crudeOil = apply_opposite_direction_approach(uslci_crudeOil)

# add default 'Global' location when process is missing location attribute
uslci_crudeOil = append_jsonld_location(uslci_crudeOil)


## Apply strategies ##
# Standard JSON-LD strategies
uslci_crudeOil.apply_strategies()


## Merge biosphere flows ###
uslci_crudeOil.merge_biosphere_flows()


## Check degree of linkage within model graph
uslci_crudeOil.statistics()

############################################################################
######### Testing importing a database containing USLCI w/ elci installed #########
############################################################################

## Dependencies ##
import bw2data as bd
from bw2io.importers.json_ld import JSONLDImporter
from bw2io.importers.json_ld_lcia import JSONLDLCIAImporter
from wmlci.settings import datapath
from bw2io.strategies import *
from bw2io.strategies.special import *

from wmlci.common import *

## Project Initiation ##

# Set active project
bd.projects.set_current("usLciTest")

# Import USLCI & FEDEFL combined dataset
path = f'C:/Users/mchristie/OneDrive - Eastern Research Group/defaultFolders/Desktop/Databases/WARM refactor/USLCI'
uslci = JSONLDImporter(path,'uslci')

#uslci_crudeOil.extractor() # Parse JSON-LD
uslci.extractor() # Parse JSON-LD

# Checking basic data requirements
print(type(uslci.data)) # confirming data type is a dictionary
print(len(uslci.data)) # confirming the expected number (7) of components have been extracted to 'data'
print(uslci.data.keys()) # listing the keys

## Error fixing ##
# fixing issues in JSON-LD before calling apply_strategies()

# change 'isInput' key in exchanges to 'input' as expected by BW
uslci = correct_jsonld_input_key(uslci)

# change output exchanges that are not ref flow to TECHNOSPHERE_FLOW
uslci = edit_non_quant_ref_flow_type(uslci)

# apply opposite direction approach to waste treatment processes
uslci = apply_opposite_direction_approach(uslci)

# add default 'Global' location when process is missing location attribute
uslci = append_jsonld_location(uslci)


## Apply strategies and Linkages ##
# Standard JSON-LD strategies
uslci.apply_strategies()
uslci.merge_biosphere_flows()

# See In [8]: https://github.com/brightway-lca/brightway25/blob/main/notebooks/IO%20-%20Importing%20the%20US%20LCI%20database.ipynb
uslci.apply_strategy(special.add_dummy_processes_and_rename_exchanges)

# Check degree of linkage within model graph
uslci.statistics()

# Print unlinked contents
uslci.write_excel(only_unlinked=True)
find_dict_locations(uslci)
clean_locations_to_print(uslci)
clean_all_locations(uslci)


## Import LCIA Methods

# Importing IPCC via the JSONLDLCIAImporter
lciapath = f'C:/Users/mchristie/OneDrive - Eastern Research Group/defaultFolders/Desktop/Databases/WARM refactor/IPCC'
IPCC = JSONLDLCIAImporter(lciapath)

# Apply strategies to LCIA
IPCC.apply_strategies()
IPCC.statistics()