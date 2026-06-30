"""
Import WARM openLCA data exported as JSON-LD and run the life cycle impact assessment.

documentation
https://docs.brightway.dev/en/latest/content/api/bw2io/importers/json_ld_lcia/index.html#bw2io.importers.json_ld_lcia.JSONLDLCIAImporter
bw25 tutorial https://learn.brightway.dev/en/latest/content/chapters/BW25/BW25_introduction.html
"""

####################
### DEPENDENCIES ###
####################

from wmlci.common import load_JSONLD_sourceData, clean_JSONLD_sourceData
from wmlci.disaggregation import split_multi_product_processes
from wmlci.editImporter import (
    convert_lcia_param_list_to_dict,
    map_lcia_to_fedelemflowlist_UUIDs,
)
from wmlci.errorLogging import check_for_errors_in_jsonld_import
from wmlci.settings import wmlcioutputpath
from wmlci.log import log

import bw2data as bd
import bw2calc as bc
from bw2calc import LCA

import numpy as np
import pandas as pd

############################
# DATABASE & METHODS SETUP #
############################

# importing data exported from openlca
#openlca_sourceData = 'warm_v16_openlca_database_Mar2022_fw'  # food waste data
#openlca_sourceData = 'warm_v16_openlca_database_2025-06-13'  # all warm data
#openlca_sourceData = 'USLCI_1_2025_06_0'  # USLCI w/ elci installed
#openlca_sourceData = 'warm613_v16_pilot_square_2025-11-18' # three pilot processes w/ square tech. matrix
openlca_sourceData = 'warm613_v16_pilot_square_2025_12_11' # three pilot processes w/ square tech. matrix; reduced elem flows
#openlca_sourceData = 'petro-refining-wProv' # petroleum refining process w/ no providers for testing disaggregation
lcia_sourceData = 'IPCC_LCIA_methods_1.2024-12.0'

# define Brightway database names
openlca_db_name = 'openlca_db'
lcia_db_name = 'IPCC'

# initiate project
bd.projects.set_current("openlca-eval")

###############################################
# import and clean WARM openlca data  #
###############################################
# If database exists, check that database is not empty - which occurs when there are errors in the run
# otherwise import warm openlca data and prep for brightway analysis
if openlca_db_name in bd.databases and len(bd.Database(openlca_db_name)) > 0:
    log.info(f"'{openlca_db_name}' is already present in the project - skipping import.")
else:
    jsonld = load_JSONLD_sourceData(
        openlca_sourceData, datatype='jsonld', bw_database_name=openlca_db_name
    )

    # split multi-product processes so the technosphere matrix is square
    jsonld = split_multi_product_processes(jsonld)

    # check for errors in imported data - these checks do not fix the errors
    check_for_errors_in_jsonld_import(jsonld)

    # apply common clean up procedures
    jsonld = clean_JSONLD_sourceData(jsonld)

    # check for errors again
    log.info("Checking errors are fixed")
    check_for_errors_in_jsonld_import(jsonld)

    # fix issues when openLCA and brightway have to talk by manipulating data sets
    jsonld.apply_strategies()
    # check for unlinked flows - output errors
    #write_unlinked_flows_to_excel(jsonld, errorlogsoutputpath)

    # merge biosphere flows
    #jsonld.write_separate_biosphere_database()
    jsonld.merge_biosphere_flows()
# checking if everything worked out with strategies and linking
    jsonld.statistics()
    # write to excel
    # warm.write_excel(only_unlinked=False) # set to True if errors

    # save the database
    jsonld.write_database()

# assign database to variable
openlca_db = bd.Database(openlca_db_name)

# print type/length of db
print(
    "The imported openlca database is of type {} and has a length of {}.".format(
        type(openlca_db), len(openlca_db)
    )
)

###########################
# Import LCIA methods #
###########################

# skip import if methods exist and contain CFs
existing_ipcc_methods = [m for m in bd.methods if lcia_db_name in m]
methods_have_cfs = any(len(bd.Method(m).load()) > 0 for m in existing_ipcc_methods)

if existing_ipcc_methods and methods_have_cfs:
    log.info(f"IPCC methods found - skipping LCIA import.")
else:
    jsonldlcia = load_JSONLD_sourceData(
        lcia_sourceData, datatype='jsonld_lcia', bw_database_name=lcia_db_name
    )
    # convert parameter lists to dicts
    jsonldlcia = convert_lcia_param_list_to_dict(jsonldlcia)

    # prepare LCIA - apply strategies, harmonize CF flows to FEDEFL,
    # link to inventory by UUID
    jsonldlcia.apply_strategies()
    jsonldlcia = map_lcia_to_fedelemflowlist_UUIDs(jsonldlcia, sourcelistname="IPCC")
    jsonldlcia.match_biosphere_by_id(openlca_db_name)
    # drop the CFs that do not match a flow
    jsonldlcia.drop_unlinked(verbose=True)
    jsonldlcia.statistics()
    jsonldlcia.write_methods(overwrite=True)

# list available methods
ipcc_methods = [m for m in bd.methods if lcia_db_name in m]
print(f"{len(ipcc_methods)} IPCC methods available: {ipcc_methods[:5]}")

##############################
# Select functional unit #
##############################

def return_product(activity):
    """
    Return the product node for an activity's reference flow.

    In Brightway 2.5, openLCA imports create separate process and product
    nodes. The technosphere demand vector is indexed by products (rows),
    not processes (columns). The production exchange links the process to
    its reference product via exc.input.
    """
    production_exchanges = list(activity.production())
    if len(production_exchanges) != 1:
        raise ValueError(
            f"Activity '{activity['name']}' has {len(production_exchanges)} "
            "production exchanges; expected exactly one."
        )
    product = production_exchanges[0].input
    if product.get("type") != "product":
        raise ValueError(
            f"Production exchange for '{activity['name']}' does not link to a "
            f"product node (got type '{product.get('type')}')."
        )
    return product


def return_process_product(db):
    """
    Return (process, product) pairs for processes with a single reference
    product. Use the product as the functional-unit key for LCA calculations.
    """
    pairs = []
    for act in db:
        if act.get("type") != "process":
            continue
        production_exchanges = list(act.production())
        if len(production_exchanges) != 1:
            continue
        product = production_exchanges[0].input
        if product.get("type") == "product":
            pairs.append((act, product))
    return pairs


process_product = return_process_product(openlca_db)
print(f"Found {len(process_product)} process/product pairs.")
for act, product in process_product:
    print(f"  - {act['name']} -> product id {product.id}")

#######################
# LCA calculation #
#######################

# Choose IPCC impact category
method = ('IPCC', 'AR6-20')
if method not in bd.methods:
    log.error('Select available IPCC method.')

# Select process to assess and its reference product.
activity, product = process_product[0]

# Print exchanges of the selected process
print(f"\nExchanges for process: {activity['name']}")
for exc in activity.exchanges():
    print(
        f"  {exc['amount']} {exc.get('unit', '')} of {exc.input['name']} "
        f"({exc.input.get('location', 'n/a')}) - type: {exc['type']}"
    )
print(f"Reference product for demand: {product['name']} (id {product.id})")

# Build Brightway 2.5 inputs and run the LCA. Based on product node id.
funcUnt, data_objs, _ = bd.prepare_lca_inputs({product: 1}, method=method)
lca = LCA(funcUnt, data_objs=data_objs)
lca.lci()   # life cycle inventory: solves A^-1 f
lca.lcia()  # life cycle impact assessment: C B A^-1 f
print(f"\nMethod: {method}\nFunctional unit: {activity['name']}\nScore: {lca.score}\n")

##################################################
# LCA for every product (single method) #
##################################################
# Loop over valid functional units for the chosen method and collect scores.

results = []
for act, product in process_product:
    try:
        fu, data_objs, _ = bd.prepare_lca_inputs({product: 1}, method=method)
        act_lca = LCA(fu, data_objs=data_objs)
        act_lca.lci()
        act_lca.lcia()
        results.append({
            "activity": act["name"],
            "reference_product": product.get("name", act.get("reference product", "")),
            "product_id": str(product.id),
            "location": act.get("location", ""),
            "method": str(method),
            "unit": bd.methods[method].get("unit", ""),
            "score": act_lca.score,
        })
    except (ValueError, bc.errors.OutsideTechnosphere) as err:
        log.warning(f"Skipping '{act['name']}': {err}")

results_df = pd.DataFrame(results)
results_df["product_id"] = results_df["product_id"].astype(str)
print("\nLCA results:")
print(results_df.to_string(index=False))

# write results to the WMLCI output directory
results_path = wmlcioutputpath / "openlca_lcia_results.csv"
results_df.to_csv(results_path, index=False)
log.info(f"LCA results written to {results_path}")

############################
# Assess Results #
############################

print(f"\nContribution analysis for: {activity['name']} ({method})")

ci = lca.characterized_inventory  # sparse: biosphere flows x processes

# top contributing processes - sum over biosphere rows of each column
process_scores = np.asarray(ci.sum(axis=0)).ravel()
process_order = np.argsort(np.abs(process_scores))[::-1]
print("\nTop contributing processes:")
for idx in process_order[:10]:
    if process_scores[idx] == 0:
        continue
    node = bd.get_activity(lca.dicts.activity.reversed[idx])
    print(f"  {process_scores[idx]: .6g}  {node['name']}")

# top contributing elementary flows (sum over process cols of each row)
flow_scores = np.asarray(ci.sum(axis=1)).ravel()
flow_order = np.argsort(np.abs(flow_scores))[::-1]
print("\nTop contributing elementary flows:")
for idx in flow_order[:10]:
    if flow_scores[idx] == 0:
        continue
    node = bd.get_activity(lca.dicts.biosphere.reversed[idx])
    print(f"  {flow_scores[idx]: .6g}  {node['name']}")
