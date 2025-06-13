"""
import USLCI process: Crude oil, production mixture, at extraction
"""
## Dependencies ##
import bw2data as bd
from bw2io.importers.json_ld import JSONLDImporter
from wmlci.settings import datapath
from bw2io.strategies import *

from wmlci.common import correct_jsonld_input_key, edit_non_quant_ref_flow_type, apply_opposite_direction_approach, \
    append_jsonld_location


## Project Initiation ##

# Set active project
bd.projects.set_current("usLciTest")
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