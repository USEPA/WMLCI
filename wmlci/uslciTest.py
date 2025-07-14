"""
import USLCI process: Crude oil, production mixture, at extraction
"""
'''
## Dependencies ##
import bw2data as bd
from bw2data import databases
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
'''

############################################################################
######### Testing importing a database containing USLCI w/ elci installed #########
############################################################################

## Dependencies ##

from bw2calc import LCA, LeastSquaresLCA
import bw2data as bd
from bw2io.importers.json_ld import JSONLDImporter
from bw2io.importers.json_ld_lcia import JSONLDLCIAImporter
from bw2io.strategies import *
from bw2io.strategies.special import *
import multifunctional

from wmlci.common import *

## Project Initiation ##

# Set active project
bd.projects.set_current("usLciTest")

## Import USLCI w/ elci installed ##
path = f'C:/Users/mchristie/OneDrive - Eastern Research Group/defaultFolders/Desktop/Databases/WARM refactor/USLCI'
uslci = JSONLDImporter(path,'uslci')

# Checking basic data requirements
print(type(uslci.data)) # confirming data type is a dictionary
print(len(uslci.data)) # confirming the expected number (7) of components have been extracted to 'data'
print(uslci.data.keys()) # listing the keys


## Error fixing ##
# Fixing issues in JSON-LD before calling apply_strategies()
uslci = correct_jsonld_input_key(uslci) # change 'isInput' key in exchanges to 'input' as expected by BW
uslci = edit_non_quant_ref_flow_type(uslci) # change output exchanges that are not ref flow to TECHNOSPHERE_FLOW
uslci = apply_opposite_direction_approach(uslci) # apply opposite direction approach to waste treatment processes
uslci = add_process_location(uslci) # add default 'Global' location when process is missing location attribute


## Apply strategies, evaluate linkages, write databases ##
uslci.apply_strategies()
uslci.apply_strategy(special.add_dummy_processes_and_rename_exchanges) # See In [8]: https://github.com/brightway-lca/brightway25/blob/main/notebooks/IO%20-%20Importing%20the%20US%20LCI%20database.ipynb
uslci.statistics()
#uslci.write_separate_biosphere_database()
uslci.merge_biosphere_flows()
uslci.drop_unlinked(i_am_reckless=True) # temporary to be able to write database; more work required for linking flows
uslci.write_database()

# Fix missing location attributes or nonetype values
# This must be done to write unlinked flows to spreadsheet
clean_all_locations(uslci)
# call function write_unlinked_flows_to_excel(uslci, outputPath)


## Import LCIA Methods ##

# Importing IPCC via the JSONLDLCIAImporter
lciapath = f'C:/Users/mchristie/OneDrive - Eastern Research Group/defaultFolders/Desktop/Databases/WARM refactor/IPCC'
IPCC = JSONLDLCIAImporter(lciapath)

# Apply strategies to LCIA, evaluate linkages, match biosphere flows to characterization factors
IPCC.apply_strategies()
IPCC.match_biosphere_by_id('uslci')
IPCC.statistics()
IPCC.drop_unlinked(verbose=True)
IPCC.statistics()
IPCC.write_methods(overwrite=True) # uncomment if running for the first time


'''
# Export IPCC.data to a json file after running apply_strategies()
import json
# Convert the dict_values to a list
data_list = list(IPCC.data)
path = r'C:\Users\mchristie\OneDrive - Eastern Research Group\Projects\Brightway\IPCC_data'
# Export to a JSON file
with open(path, 'w', encoding='utf-8') as f:
    json.dump(data_list, f, ensure_ascii=False, indent=4)
'''

## Calculate LCA Results ##
# https://learn.brightway.dev/en/latest/content/chapters/BW25/BW25_introduction.html

# Choose activity (process)
# Selecting 'Crude oil, production mixture, at extraction by 'name' attribute
USLCI = bd.Database('uslci')

# Get activity
# Inspect contents and select the activity (i.e. process not the product flow)
activity = [act for act in USLCI if "crude oil, production mixture, at extraction" in act["name"].lower()]
activity = activity[1] # This selects the activity
#print(activity)

# Print all exchanges
for exc in activity.exchanges():
   print(f"{exc['amount']} {exc['unit']} of {exc.input['name']} ({exc.input['location']}) - type: {exc['type']}")

# Select LCIA method to use for running LCA
# Calculate and print results
results = []
method = ('IPCC','AR6-20')
funcUnt, data_objs, _ = bd.prepare_lca_inputs(
    {activity: 1},
    method=method,
)
print(method)
# Non square matrix, was prompted to use LeastSquaresLCA instead of just LCA
lca = LeastSquaresLCA(funcUnt, data_objs = data_objs) # {activity:1} is the selected process and a reference flow quant of 1 unit
lca.lci()
lca.lcia()
results.append((method, lca.score))
print(f"Method: {method}\nScore: {lca.score}\n")




