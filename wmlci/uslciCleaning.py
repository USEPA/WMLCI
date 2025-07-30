"""
Script for fixing incompatibilities between USLCI v1.2025-06.0 and Brightway25 (BW25)

The specific goals of this script are to fix missing data or structural differences that:
    1) causes failure of BW25's apply_strategies() applied to the JSONLDImporter object.
    2) contribute to unlinked edges within the JSONLDImporter object
    3) yield a non-square technosphere matrix (one product flow per process/activity)

List of implemented fixes:
    -


"""
####################
### DEPENDENCIES ###
####################

# Repository
from wmlci.common import *
from wmlci.editImporter import *
from wmlci.errorLogging import *
from wmlci.settings import datapath

# Brightway
import bw2data as bd
import bw2calc as bc
from bw2io.importers.json_ld import JSONLDImporter
from bw2io.importers.json_ld_lcia import JSONLDLCIAImporter
from bw2io.strategies import *
from bw2io.strategies.special import *

# Other
import pandas as pd
import numpy as np
import sys
import pickle
import uuid

##################################
### PROJECT INITIATION & SETUP ###
##################################

## Deleting unneeded projects ##
# Gets a list of current projects excluding the 'default' key
# Deletes all projects within the BW ecosystem
# **TAKE CARE when running this code**
projects = [p for p in bd.projects if getattr(p, 'name', None) != 'default']
for project in projects:
    print(f"Deleting project: {project.name}")
    bd.projects.delete_project(project.name, delete_dir=True)

## Set active project ##
bd.projects.set_current("uslciCleaning")

## Set database and biosphere database names ##
nameDB = 'uslci'
pathLCIA = r'C:\Users\mchristie\OneDrive - Eastern Research Group\defaultFolders\Desktop\Databases\WARM refactor\IPCC'
path = r'C:\Users\mchristie\OneDrive - Eastern Research Group\defaultFolders\Desktop\Databases\WARM refactor\USLCI_Q2_2025_elci_merged'

## Import USLCI as JSON-LD
#uslci = load_JSONLD_sourceData('USLCI_Q2_2025_elci_merged', bw_database_name=nameDB)
uslci = JSONLDImporter(path, nameDB, preferred_allocation="PHYSICAL_ALLOCATION")

###################################################################################
### Run debugging and cleaning functions such that apply_strategies() will work ###
###################################################################################

## Changing the key 'isInput' to 'input' ##
uslci = correct_jsonld_input_key(uslci)  # BW expects the key 'input

## Apply the Opposite Direction Approach ##
uslci = apply_opposite_direction_approach(uslci)

## Remove exchanges and processes with no impacts ##
remove_impact_free_objects(uslci)

## Check for processes with no reference flow or outputs
processes_with_no_outputs_or_ref_flow(uslci)

## Run default provider QA/QC ##
# Save errors to spreadsheet at location defined in path
errorPath = r'C:\Users\mchristie\OneDrive - Eastern Research Group\Projects\Brightway\providerErrorLogging.xlsx'
check_default_providers(uslci, errorPath, debug=True)

## Fix location errors ##
uslci = add_process_location(uslci) # Add default 'United States' location when process is missing location attribute
uslci = fix_exchange_locations(uslci) # For exchanges missing location, inherit parent process location data


## Apply strategies ##
uslci.apply_strategies()
