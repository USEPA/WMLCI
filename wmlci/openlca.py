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


def return_system_processes(db):
    """
    Return the end-of-life material-pathway scenarios to run LCAs on.
    """
    consumed = set()
    for act in db:
        if act.get("type") != "process":
            continue
        for exc in act.technosphere():
            consumed.add(exc.input.id)

    systems = []
    for act, product in return_process_product(db):
        if product.id not in consumed:
            systems.append((act, product))
    return systems


systems = return_system_processes(openlca_db)
print(f"Found {len(systems)} product systems (material-pathway scenarios):")
for act, product in systems:
    print(f"  - {act['name']} -> {product['name']} (product id {product.id})")

#######################
# LCA calculation #
#######################

# Choose IPCC impact category
method = ('IPCC', 'AR6-20')
if method not in bd.methods:
    log.error('Select available IPCC method.')

# IPCC GWP methods are kg CO2 equivalents
method_unit = "kg CO2e"

# Functional unit: demand passed to Brightway in reference-product units (WARM = kg).
# Use 1 for 1 kg, or SHORT_TON_KG for one US short ton (~907.18 kg in WARM).
SHORT_TON_KG = 907.18474
functional_unit_demand = SHORT_TON_KG  # set to SHORT_TON_KG to run per short ton

_UNIT_LABEL = {
    "kilogram": "kg",
    "short ton": "short ton",
    "megajoule": "MJ",
    "ton kilometer": "ton km",
    "cubic meter": "m3",
}


def functional_unit_label(reference_product, product_unit, demand):
    """functional unit string for CSV output."""
    product_label = reference_product.replace(",", "").strip().lower()
    if demand == 1:
        unit_short = _UNIT_LABEL.get(product_unit, product_unit)
        return f"1 {unit_short} {product_label}"
    if abs(demand - SHORT_TON_KG) < 0.01:
        return f"1 short ton {product_label}"
    unit_short = _UNIT_LABEL.get(product_unit, product_unit)
    return f"{demand} {unit_short} {product_label}"


# Calc reference-product metadata for each process (units, production amount).
process_meta = {}
for proc, prod in return_process_product(openlca_db):
    production_exchanges = list(proc.production())
    prod_exc = production_exchanges[0] if production_exchanges else {}
    process_meta[proc.id] = {
        "reference_product": prod.get("name", ""),
        "location": proc.get("location", ""),
        "supply_unit": prod_exc.get("unit", ""),
        "production_amount": prod_exc.get("amount") or 1,
    }

# Run LCA for each product system
results = []        # one row per system (summary)
detail_rows = []    # one row per process within each system (detailed)
for activity, product in systems:
    try:
        funcUnt, data_objs, _ = bd.prepare_lca_inputs(
            {product: functional_unit_demand}, method=method
        )
        lca = LCA(funcUnt, data_objs=data_objs)
        lca.lci()   # life cycle inventory: solves A^-1 f
        lca.lcia()  # life cycle impact assessment: C B A^-1 f
    except (ValueError, bc.errors.OutsideTechnosphere) as err:
        log.warning(f"Skipping system '{activity['name']}': {err}")
        continue

    sys_prod_exc = list(activity.production())[0]
    fu_unit = sys_prod_exc.get("unit") or product.get("unit") or ""
    fu_label = functional_unit_label(
        product.get("name", ""), fu_unit, functional_unit_demand
    )

    results.append({
        "system": activity["name"],
        "reference_product": product.get("name", ""),
        "functional_unit": fu_label,
        "location": activity.get("location", ""),
        "method": str(method),
        "score": lca.score,
        "score_unit": method_unit,
        "score_metric_ton_co2e": lca.score / 1000,
    })
    print(f"\nSystem: {activity['name']}\n  Score: {lca.score} {method_unit}")

    # decompose the system score by process: characterized_inventory column sums
    # give each process's contribution to the system total, and supply_array
    # gives how much of each process the system uses. Both are indexed by the
    # technosphere columns (processes).
    ci = lca.characterized_inventory  # biosphere flows x processes
    col_contributions = np.asarray(ci.sum(axis=0)).ravel()
    supply = np.asarray(lca.supply_array).ravel()
    for idx in np.argsort(np.abs(col_contributions))[::-1]:
        direct_contribution = col_contributions[idx]
        proc = bd.get_activity(lca.dicts.activity.reversed[idx])
        meta = process_meta.get(proc.id, {})

        # scale process supply to physical reference-product amount per
        # functional unit (e.g. 1 kg food waste landfilled)
        process_supply = supply[idx]
        production_amount = meta.get("production_amount") or 1
        product_amount = process_supply * production_amount
        product_unit = meta.get("supply_unit", "")
        emissions_per_unit_of_product = (
            direct_contribution / product_amount if product_amount else None
        )

        detail_rows.append({
            "location": meta.get("location", proc.get("location", "")),
            "system": activity["name"],
            "activity": proc["name"],
            "reference_product": meta.get("reference_product", ""),
            "functional_unit": fu_label,
            "product_amount": product_amount,
            "product_amount_unit": product_unit,
            "emissions_per_unit_of_product": emissions_per_unit_of_product,
            "emissions_per_unit_of_product_unit": (
                f"{method_unit} / {product_unit}" if product_unit else method_unit
            ),
            "FlowAmount": direct_contribution,
            "FlowAmount_unit": method_unit,
            "method": str(method),
        })

# write the system-level summary to CSV
results_df = pd.DataFrame(results)
print("\nLCA results (all systems):")
print(results_df.to_string(index=False))
results_path = wmlcioutputpath / "openlca_lcia_results.csv"
results_df.to_csv(results_path, index=False)
log.info(f"System summary for {len(results_df)} systems written to {results_path}")

DETAIL_COLUMNS = [
    "location",
    "system",
    "activity",
    "reference_product",
    "functional_unit",
    "product_amount",
    "product_amount_unit",
    "emissions_per_unit_of_product",
    "emissions_per_unit_of_product_unit",
    "FlowAmount",
    "FlowAmount_unit",
    "method",
]

# write individual process results for all systems to csv
detail_df = pd.DataFrame(detail_rows, columns=DETAIL_COLUMNS)
detail_path = wmlcioutputpath / "openlca_lcia_results_detailed.csv"
detail_df.to_csv(detail_path, index=False)
log.info(
    f"Detailed results ({len(detail_df)} process rows across "
    f"{detail_df['system'].nunique()} systems) written to {detail_path}"
)
