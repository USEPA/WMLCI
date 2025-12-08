"""
import openLCA data exported as JSON-LD

documentation
https://docs.brightway.dev/en/latest/content/api/bw2io/importers/json_ld_lcia/index.html#bw2io.importers.json_ld_lcia.JSONLDLCIAImporter
bw25 tutorial https://learn.brightway.dev/en/latest/content/chapters/BW25/BW25_introduction.html
"""
####################
### DEPENDENCIES ###
####################

from boto3.docs.action import WARNING_MESSAGES

# from wmlci.settings import sourcedatapath,wmlcioutputpath
from wmlci.common import load_JSONLD_sourceData, clean_JSONLD_sourceData
from wmlci.disaggregation import disaggregate_multifunctional_processes, get_multifunctional_processes
from wmlci.editImporter import *
# from wmlci.disaggregation import *
from wmlci.errorLogging import *

import bw2analyzer as ba
import bw2data as bd
import bw2calc as bc
import bw2io as bi
from bw2io.importers.json_ld import JSONLDImporter
from bw2io.importers.json_ld_lcia import JSONLDLCIAImporter
import matrix_utils as mu
import bw_processing as bp

from bw2io.importers.base_lcia import LCIAImporter

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

############################
# DATABASE & METHODS SETUP #
############################

# importing data exported from openlca
#openlca_sourceData = 'warm_v16_openlca_database_Mar2022_fw'  # food waste data
#openlca_sourceData = 'warm_v16_openlca_database_2025-06-13'  # all warm data
#openlca_sourceData = 'USLCI_1_2025_06_0'  # USLCI w/ elci installed
openlca_sourceData = 'warm613_v16_pilot_square_2025-11-18' # three pilot processes w/ square tech. matrix
lcia_sourceData = 'IPCC_LCIA_methods_1.2024-12.0'

# initiate project
bd.projects.set_current("openlca-eval")

# import warm openlca data
# if 'openlca_db' in bd.databases:
#      print('openlca_db is already present in the project.')
# else:
jsonld = load_JSONLD_sourceData(openlca_sourceData, datatype= 'jsonld', bw_database_name='openlca_db')
jsonldlcia = load_JSONLD_sourceData(lcia_sourceData, datatype= 'jsonld_lcia', bw_database_name='lcia_db')

# check for errors in imported data - these checks do not fix the errors
check_for_errors_in_jsonld_import(jsonld)

# apply common clean up procedures
jsonld = clean_JSONLD_sourceData(jsonld)
jsonldlcia = convert_lcia_param_list_to_dict(jsonldlcia)

# check for errors again
log.info("Checking errors are fixed")
check_for_errors_in_jsonld_import(jsonld)

# fixing issues when ecoinvent and brightway have to talk by manipulating data sets
jsonld.apply_strategies()
# check for unlinked flows - output errors
write_unlinked_flows_to_excel(jsonld, errorlogsoutputpath)

# merge biosphere flows
#jsonld.write_separate_biosphere_database()
jsonld.merge_biosphere_flows()
# checking if everything worked out with strategies and linking
jsonld.statistics()
# write to excel
# warm.write_excel(only_unlinked=False) # set to True if errors
# save the database to our hard drive
jsonld.write_database()

# assign database to variable
openlca_db = bd.Database("openlca_db")

# print type/length of db
print(
    "The imported openlca database is of type {} and has a length of {}.".format(
        type(openlca_db), len(openlca_db)
    )
)

# only works if square technosphere matrix
# openlca_db.graph_technosphere()

# Prepare LCIA database
jsonldlcia.apply_strategies()
jsonldlcia.match_biosphere_by_id('openlca_db')
jsonldlcia.statistics()
jsonldlcia.drop_unlinked(verbose=True)
jsonldlcia.statistics()
jsonldlcia.write_methods(overwrite=True) # uncomment if running for the first time

### LCA CALCULATION ###

# Get activity
# Inspect contents and select the activity (i.e. process not the product flow)
activity = [act for act in openlca_db if "" in act["name"].lower()]
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
lca = LCA(funcUnt, data_objs = data_objs) # {activity:1} is the selected process and a reference flow quant of 1 unit
lca.lci()
lca.lcia()
results.append((method, lca.score))
print(f"Method: {method}\nScore: {lca.score}\n")