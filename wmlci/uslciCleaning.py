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

# Cleaning functions
from wmlci.common import *

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
nameBioDB = 'uslci biosphere'

## Paths for JSON-LD files requiring import ##
pathDB = r'C:\Users\mchristie\OneDrive - Eastern Research Group\defaultFolders\Desktop\Databases\WARM refactor\USLCI'
pathLCIA = r'C:\Users\mchristie\OneDrive - Eastern Research Group\defaultFolders\Desktop\Databases\WARM refactor\IPCC'

## Extract USLCI data from JSON-LD into importer object
uslci = JSONLDImporter(
    pathDB,
    nameDB,
    # Most USLCI allocations are based on physical allocations (source: Rebe Feraldi)
    preferred_allocation="PHYSICAL_ALLOCATION"
)

###################################################################################
### Run debugging and cleaning functions such that apply_strategies() will work ###
###################################################################################

## Changing the key 'isInput' to 'input' ##
uslci = correct_jsonld_input_key(uslci) # BW expects the key 'input

## Apply the Opposite Direction Approach ##
# Converting non-cutoff waste outputs to inputs in the producing process
uslci = apply_opposite_direction_approach(uslci)

## Fix location errors ##
uslci = add_process_location(uslci) # add default 'United States' location when process is missing location attribute
uslci = fix_exchange_locations(uslci)

uslci = edit_non_quant_ref_flow_type(uslci) # change output exchanges that are not ref flow to TECHNOSPHERE_FLOW



