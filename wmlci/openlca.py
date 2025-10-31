"""
import openLCA data exported as JSON-LD

documentation
https://docs.brightway.dev/en/latest/content/api/bw2io/importers/json_ld_lcia/index.html#bw2io.importers.json_ld_lcia.JSONLDLCIAImporter
bw25 tutorial https://learn.brightway.dev/en/latest/content/chapters/BW25/BW25_introduction.html
"""
from boto3.docs.action import WARNING_MESSAGES

# from wmlci.settings import sourcedatapath,wmlcioutputpath
from wmlci.common import load_JSONLD_sourceData, clean_JSONLD_sourceData
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

# importing data exported from openlca
# openlca_sourceData = 'warm_v16_openlca_database_Mar2022_fw'  # food waste data
#openlca_sourceData = 'warm_v16_openlca_database_2025-06-13'  # all warm data
openlca_sourceData = 'USLCI_1_2025_06_0'  # USLCI w/ elci installed

# initiate project
bd.projects.set_current("openlca-eval")

# import warm openlca data
# if 'openlca_db' in bd.databases:
#     print('openlca_db is already present in the project.')
# else:
jsonld = load_JSONLD_sourceData(openlca_sourceData,
                              bw_database_name='openlca_db')

# check for errors in imported data - these checks do not fix the errors
check_for_errors_in_jsonld_import(jsonld)

# change 'isInput' key in exchanges to 'input' as expected by BW, must do this change before checking for errors
#jsonld = correct_jsonld_input_key(jsonld)

# apply common clean up procedures
jsonld = clean_JSONLD_sourceData(jsonld)

# check for errors again
log.info("Checking errors are fixed")
check_for_errors_in_jsonld_import(jsonld)



# check for multi processes - food waste data does not have multifunctional processes, even though not a square matrix
# jsonld = disaggregate_multifunctional_processes(jsonld)

# fixing issues when ecoinvent and brightway have to talk by manipulating data sets
jsonld.apply_strategies()
# check for unlinked flows - output errors
write_unlinked_flows_to_excel(jsonld, errorlogsoutputpath)

# merge biosphere flows
jsonld.write_separate_biosphere_database()
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
openlca_db.graph_technosphere()

