from bw2io.importers.json_ld import JSONLDImporter

from wmlci.wmlci_log import log

from esupy.remote import make_url_request
from esupy.util import make_uuid
from esupy.processed_data_mgmt import download_from_remote, Paths, mkdir_if_missing

####################################
### Remove flows with no impacts ###
####################################

def delete_input_flow_category(importer, categories_to_delete):
    """
    Deletes input exchanges and flows from the importer based on matching flow categories,
    excluding exchanges that have a 'defaultProvider' dictionary.
    Does not delete input CUTOFF flows if they have a default provider assigned

    Parameters:
    - importer: JSONLDImporter object
    - categories_to_delete: list of category strings to match against flow categories
    """
    flows_to_delete = set()  # using a set so that only unique elements are stored

    # Loop through all processes and their exchanges
    for pid, process in importer.data.get("processes", {}).items():
        exchanges = process.get("exchanges", [])
        new_exchanges = []
        for exchange in exchanges:
            if exchange.get("input") is True: # target input exchanges
                flow = exchange.get("flow", {})
                category = flow.get("category")
                # Do not store CUTOFF for deletion if the input exchange has a defaultProvider
                has_default_provider = "defaultProvider" in exchange and isinstance(exchange["defaultProvider"], dict)
                if category in categories_to_delete and not has_default_provider:
                    flow_id = flow.get("@id")
                    if flow_id:
                        flows_to_delete.add(flow_id)
                    continue  # Skip adding this exchange to new_exchanges
            # Filtered exchanges
            new_exchanges.append(exchange)
        # Update the exchanges list after filtering
        process["exchanges"] = new_exchanges
    # Delete the flows from the importer flow dictionary
    flows = importer.data.get("flows", {})
    for fid in flows_to_delete:
        if fid in flows:
            del flows[fid]
    log.info(f"Deleted {len(flows_to_delete)} flows and associated exchanges matching categories: {categories_to_delete} (excluding those with defaultProvider)")


def delete_output_flow_category(importer, categories_to_delete):
    """
    Deletes output exchanges and flows from the importer based on matching flow categories.
    Will not delete the exchange if 'isQuantitativeReference' = True
    - This is required for USLCI to USEEIO bridge processes that use cutoffs as outputs

    Parameters:
    - importer: JSONLDImporter object
    - categories_to_delete: list of category strings to match against flow categories
    """
    flows_to_delete = set()  # using a set to store unique flow IDs
    # Loop through all processes and their exchanges
    for pid, process in importer.data.get("processes", {}).items():
        exchanges = process.get("exchanges", [])
        new_exchanges = []
        for exchange in exchanges:
            if exchange.get("input") is False:  # target output exchanges
                flow = exchange.get("flow", {})
                category = flow.get("category")
                # Will not store uuid if the exchange is the quantitative reference
                if category in categories_to_delete and exchange.get("isQuantitativeReference") != True:
                    flow_id = flow.get("@id")
                    if flow_id:
                        flows_to_delete.add(flow_id)
                    continue  # Skip adding this exchange to new_exchanges
            # Filtered exchanges
            new_exchanges.append(exchange)
        # Update the exchanges list after filtering
        process["exchanges"] = new_exchanges
    # Delete the flows from the importer flow dictionary
    flows = importer.data.get("flows", {})
    for fid in flows_to_delete:
        if fid in flows:
            del flows[fid]
    log.info(f"Deleted {len(flows_to_delete)} output flows and associated exchanges matching categories: {categories_to_delete}")

def delete_product_flow_category(importer, categories_to_delete):
    """
    Deletes products from importer.products based on matching categories,
    and removes associated flows from importer.data["flows"] using product 'code' as '@id'.

    Parameters:
    - importer: JSONLDImporter object
    - categories_to_delete: list of category strings to match against product categories
    """
    uuids_to_delete = set()
    new_products = []
    for product in importer.products:
        product_categories = product.get("categories", [])
        if any(category in categories_to_delete for category in product_categories):
            code = product.get("code")
            if code:
                uuids_to_delete.add(code)
            continue  # Skip this product (i.e., delete it)
        new_products.append(product)
    # Replace products with filtered products list
    importer.products = new_products
    # Delete flows from importer.data["flows"] where the flow's '@id' is in codes_to_delete
    flows = importer.data.get("flows", {})
    for fid in list(flows):  # list() to avoid modifying dict during iteration
        if flows[fid].get("@id") in uuids_to_delete:
            del flows[fid]
    log.info(f"Deleted {len(uuids_to_delete)} products and associated flows matching categories: {categories_to_delete}")

###################################
### Opposite direction approach ###
###################################

def apply_opposite_direction_approach(jsonld):
    '''
    https://greendelta.github.io/openLCA2-manual/waste_modelling.html#opposite-direction-approach
    fix for strategy json_ld_add_activity_unit() from strategies/json_ld.py
    use the opposite direction approach for waste treatment processes
    this is required so that production exchanges are present in waste treatment processes
    this must be done to avoid "Failed Allocation" assertion error when zero production exchanges are present in a process
    :param jsonld:
    :return:
    '''
    for process_id, process in jsonld.data.get("processes", {}).items():
        if process.get("type") in {"emission", "product"}:
            continue
        for exchange in process.get("exchanges", []):
            flow = exchange.get("flow", {})
            if not isinstance(flow, dict):
                continue
            if flow.get("flowType") != "WASTE_FLOW":
                continue
            flow_category = flow.get("category", "")
            if "CUTOFF Waste Flows" in flow_category:
                continue
            else:
                # Edit waste outputs from processes that are inputs to waste treatment
                if flow.get("flowType") == 'WASTE_FLOW' and exchange.get("input") == False:
                    exchange["amount"] *= -1  # make value negative
                    flow["input"] = True  # make input
                    exchange["input"] = True # make input
                # Edit waste input to waste treatment
                if flow.get("flowType") == 'WASTE_FLOW' and exchange.get("input") == True:
                    exchange["amount"] *= -1  # make value negative
                    exchange["isQuantitativeReference"] = True  # make quantitative reference
                    flow["input"] = False  # make output
                    exchange["input"] = False  # make output
                    flow["flowType"] = "PRODUCT_FLOW"  # make product flow
    return jsonld

###########################
### Fix location issues ###
###########################

def add_process_location(jsonld):
    """
    fix for strategy json_ld_location_name() from strategies/json_ld.py
    *** need to create some logic to infer location based on the location of the exchanges in the process (?)
    :param jsonld:
    :return:
    """
    for process_id, process in jsonld.data.get("processes", {}).items():
        if process.get("type") in {"emission", "product"}:
            continue
        location = process.get("location")
        if isinstance(location, dict) and "name" in location:
            continue  # Location is already in the correct format
        else:
            process["location"] = {
                "@id": '0b3b97fa-6688-3c56-88ee-4ae80ec0c3c2',
                "@type": "Location",
                "name": "United States"
            }
    return jsonld

def fix_exchange_locations(jsonld):
    """
    Ensures all exchanges in each process have a valid 'location' field.
    If an exchange has a missing, None, or non-string 'location', it inherits
    the location from its parent process.

    Every 5000th fix prints:
        1. Original exchange location value
        2. Parent process location
        3. Updated exchange location value

    Parameters
    ----------
    jsonld : JSONLDImporter
        An instance of the JSONLDImporter class containing `.data`.

    Returns
    -------
    JSONLDImporter
        The modified JSONLDImporter object with fixed exchange locations.
    """
    count_fixed = 0

    for pid, process in jsonld.data.get("processes", {}).items():
        parent_location = process.get("location")

        for exc in process.get("exchanges", []):
            original_location = exc.get("location")
            if original_location is None or not isinstance(original_location, str):
                exc["location"] = parent_location
                count_fixed += 1

                if count_fixed % 1000 == 0:
                    print(f"\n🔧 Fix #{count_fixed}")
                    print(f"  - Original exchange location: {original_location}")
                    print(f"  - Parent process location:    {parent_location}")
                    print(f"  - Updated exchange location:  {exc['location']}")

    print(f"\n✅ Total exchange locations fixed by inheriting from parent process: {count_fixed}")
    return jsonld

###########################
### Miscellaneous fixes ###
###########################

def remove_process_allocation_factors(jsonld):
    """
    The WARM openLCA processes include allocationFactors that are at times missing "exchange" information.
    This missing information results in the error:
    bw2io.errors.UnallocatableDataset: We currently only support exchange-specific CAUSAL_ALLOCATION
    As all have factors of "1" - this information can be removed
    :param jsonld:
    :return:
    """

    for pid, process in jsonld.data.get("processes", {}).items():
        # pull allocation factors where value is not equal to 1, these factors are kept
        filtered = [
            af for af in process.get("allocationFactors", []) if af.get("value") != 1
        ]

        if filtered:
            process["allocationFactors"] = filtered
        else:
            process.pop("allocationFactors", None)

    return jsonld

def correct_jsonld_input_key(jsonld):
    '''
    fix for strategy json_ld_allocate_datasets() from strategies/json_ld.py
    changes 'isInput' key in exchanges to 'input' in all exchanges within processes
    :param jsonld:
    :return:
    '''
    for process_key, process in jsonld.data.get("processes", {}).items():
        for exc in process.get("exchanges", []):
            if "isInput" in exc:
                exc["input"] = exc.pop("isInput")
    return jsonld