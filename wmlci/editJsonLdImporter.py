from wmlci.wmlci_log import log

from bw2io.importers.json_ld import JSONLDImporter

from wmlci.wmlci_log import log
from esupy.remote import make_url_request
from esupy.util import make_uuid
from esupy.processed_data_mgmt import download_from_remote, Paths, mkdir_if_missing

def delete_flows_of_category(importer, categories_to_delete):
    """
    Deletes exchanges and flows from the importer based on matching flow categories.

    Parameters:
    - importer: JSONLDImporter object
    - categories_to_delete: list of category strings to match against flow categories
    """
    flows_to_delete = set() # using a set so that only unique elements are stored
    # Loop through all processes and their exchanges
    for pid, process in importer.data.get("processes", {}).items():
        exchanges = process.get("exchanges", [])
        new_exchanges = []
        for exchange in exchanges:
            if exchange.get("input") is True:
                flow = exchange.get("flow", {})
                category = flow.get("category")
                if category in categories_to_delete:
                    flow_id = flow.get("@id")
                    if flow_id:
                        flows_to_delete.add(flow_id)
                    continue  # Skip adding this exchange to new_exchanges
            new_exchanges.append(exchange)
        # Update the exchanges list after filtering
        # Cant delete list items while iterating over them
        # Replaces 'exchanges' excluding the entries we dont want
        process["exchanges"] = new_exchanges
    # Delete the flows from the importer's flow dictionary
    flows = importer.data.get("flows", {})
    for fid in flows_to_delete:
        if fid in flows:
            del flows[fid]
    log.info(f"Deleted {len(flows_to_delete)} flows and associated exchanges matching categories: {categories_to_delete}")

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

def edit_non_quant_ref_flow_type(jsonld):
    '''
    fix for strategy json_ld_add_activity_unit() from strategies/json_ld.py
    changing flowType for any PRODUCT_FLOW that isn't the quantitative reference to TECHNOSPHERE_FLOW
    this must be done to avoid "Failed Allocation" assertion error when multiple exchanges have a flowType of PRODUCT_FLOW
    :param jsonld:
    :return:
    '''
    for process_id, process in jsonld.data.get("processes", {}).items():
        exchanges = process.get("exchanges", [])
        for exchange in exchanges:
            # Skip the reference flow
            if exchange.get("isQuantitativeReference", False):
                continue
            flow = exchange.get("flow", {})
            if isinstance(flow, dict) and flow.get("flowType") == "PRODUCT_FLOW" and not flow.get("input"):
                flow["flowType"] = "TECHNOSPHERE_FLOW"
    return jsonld

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
                if flow.get("flowType") == 'WASTE_FLOW' and not exchange.get("input"):
                    exchange["amount"] *= -1  # make value negative
                    flow["input"] = True  # make input
                # Edit waste input to waste treatment
                if flow.get("flowType") == 'WASTE_FLOW' and exchange.get("input"):
                    exchange["amount"] *= -1  # make value negative
                    exchange["isQuantitativeReference"] = True  # make quantitative reference
                    flow["input"] = False  # make output
                    flow["flowType"] = "PRODUCT_FLOW"  # make reference flow
    return jsonld

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