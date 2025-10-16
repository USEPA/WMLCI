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

# importing food waste data exported from openlca
warmfilename = 'warm_v16_openlca_database_Mar2022_fw'

# initiate project
bd.projects.set_current("warm-eval")

# import warm openlca data - data was originally exported from WARM openLCA in May 2025, then modified to address
# data issues in June 2025
# if 'warm_openlca' in bd.databases:
#     print('warm_openlca is already present in the project.')
# else:
warm = load_JSONLD_sourceData(warmfilename,
                              bw_database_name='warm_openlca')

# change 'isInput' key in exchanges to 'input' as expected by BW, must do this change before checking for errors
# warm = correct_jsonld_input_key(warm)

# check for errors in imported data - these checks do not fix the errors
# todo: incorporate error fixes into checks?
check_for_errors_in_jsonld_import(warm)

# apply common clean up procedures
warm = clean_JSONLD_sourceData(warm)

# check for errors again
log.info("Checking errors are fixed")
check_for_errors_in_jsonld_import(warm)



# # check for multi processes - food waste data does not have multifunctional processes, even though not a square matrix
# # warm = disaggregate_multifunctional_processes(warm)

# fixing issues when ecoinvent and brightway have to talk by manipulating data sets
warm.apply_strategies()
# check for unlinked flows - output errors # todo: check when to run this - after apply_strategies or write_database?
write_unlinked_flows_to_excel(warm, errorlogsoutputpath)

# merge biosphere flows
warm.write_separate_biosphere_database()
warm.merge_biosphere_flows()
# checking if everything worked out with strategies and linking
warm.statistics()
# write to excel
# warm.write_excel(only_unlinked=False) # set to True if errors
# save the database to our hard drive
warm.write_database()

# assign database to variable
warmdb = bd.Database("warm_openlca")

# print type/length of db
print(
    "The imported warm openlca database is of type {} and has a length of {}.".format(
        type(warmdb), len(warmdb)
    )
)

# only works if square technosphere matrix
warmdb.graph_technosphere()

