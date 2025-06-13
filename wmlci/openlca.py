"""
import openLCA data exported as JSON-LD

documentation
https://docs.brightway.dev/en/latest/content/api/bw2io/importers/json_ld_lcia/index.html#bw2io.importers.json_ld_lcia.JSONLDLCIAImporter

"""

from wmlci.settings import datapath
from wmlci.common import append_jsonld_location
from bw2io.importers.base_lcia import LCIAImporter
from bw2io.importers.json_ld import JSONLDImporter
import bw2io as bi
import bw2data as bd
# import bw2calc as bc


# initiate project
bd.projects.set_current("warm")

# print out databases - might be empty
bd.databases

# import warm openlca data - data was originally exported from WARM openLCA in May 2025, then modified to address
# data issues in June 2025
if 'warm_openlca' in bd.databases:
    print('warm_openlca is already present in the project.')
else:
    warm = JSONLDImporter(datapath / "warm_v16_openlca_database_2025-06-13",
                          "warm_openlca")

    # append location where missing in jsonld for json_ld_location_name()
    warm = append_jsonld_location(warm)

    # fixing issues when ecoinvent and brightway have to talk by manipulating data sets
    warm.apply_strategies()
    # merge biosphere flows
    warm.merge_biosphere_flows()
    # checking if everything worked out with strategies and linking
    warm.statistics()
    # save the database to our hard drive
    warm.write_database()

# print out databases - should include warm_openlca
bd.databases

