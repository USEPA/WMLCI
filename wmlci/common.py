"""
Functions common across datasets
"""
########################################################################################################################
## Methods for locating and understanding errors which occur when calling .apply_strategies() on JSONLDImporter objects
########################################################################################################################

def find_missing_unit_group_id(ug_id, jsonld):
    """
    search for missing unit group uuid(s) in importer object
    print process and exchange info for missing unit group id
    :param ug_id:
    :param jsonld:
    :return:
    """
    print("🔍 Scanning processes for unit group issues...\n")
    for pid, process in jsonld.data.get("processes", {}).items():
        for exc in process.get("exchanges", []):
            unit = exc.get("unit", {})
            if isinstance(unit, dict) and unit.get("@id") == ug_id:
                print(f"\n⚠️ Problem in activity: \n--Process: {pid}; Exchange: {exc}")
                print("-" * 60)
    return "\n✅ Search for unit group id issues is complete."

def find_production_exchange_errors(jsonld):
    """
    search for processes with zero or more than one exchanges of flowType PRODUCT_FLOW
    print process and exchange info for missing unit group id
    this is a debugging function that should help find causes of the 'Failed Allocation' assertion error

    running methods edit_non_quant_ref_flow_type() and apply_opposite_direction_approach() should result
    in this method producing no outputs

    :param jsonld:
    :return:
    """
    print("🔍 Scanning processes for production exchange issues...\n")
    for process_id, process in jsonld.data.get("processes", {}).items():
        if process.get("type") in {"emission", "product"}:
            continue
        exchanges = process.get("exchanges", [])
        production_exchanges = [
            exc for exc in exchanges
            if exc.get("flow", {}).get("flowType") == "PRODUCT_FLOW" and not exc.get("isInput")
        ]
        if len(production_exchanges) != 1:
            print(f"⚠️  Problem in activity: {process.get('name', 'Unnamed')} (ID: {process_id})")
            print(f"Found {len(production_exchanges)} production exchanges:")
            for exc in production_exchanges:
                flow_name = exc.get("flow", {}).get("name", "Unknown")
                is_input = exc.get("isInput")
                print(f"  - Flow: {flow_name}, isInput: {is_input}")
            print("-" * 60)
        else:
            process["unit"] = production_exchanges[0]["unit"]
    return "\n✅ Search for production exchange issues is complete."

def find_location_issues(jsonld):
    """
    search for processes and exchanges with missing location info
    print process and exchange info where location info is missing
    this is a debugging function that should help find causes 'location' key errors

    running methods append_jsonld_location(jsonld) should result in no outputs

    :param jsonld:
    :return:
    """
    print("🔍 Scanning processes for location issues...\n")
    for process_id, process in jsonld.data.get("processes", {}).items():
        if process.get("type") in {"emission", "product"}:
            continue
        location = process.get("location")
        if isinstance(location, dict) and "name" in location:
            continue
        else:
            print(f"⚠️ Problem with location in process ID '{process_id}':")
            print(f"  Name: {process.get('name', 'Unnamed')}")
            print(f"  Location value: {location}")
            print("-" * 60)
    return "\n✅ Search for location issues is complete."

def find_faulty_allocation_factors(jsonld):
    """
    used to solve: "We currently only support exchange-specific CAUSAL_ALLOCATION"

    search for processes with allocationFactors that are missing  'exchange', 'product', or both
    print the process uuid and other useful information

    deleting entries in allocationFactors which has the allocation type of 'CAUSAL_ALLOCATION' but no exchange attribute
    will solve this problem.

    :param jsonld:
    :return:
    """
    faulty_processes = []

    for process_id, process in jsonld.data.get("processes", {}).items():
        if process.get("type") in {"emission", "product"}:
            continue

        for idx, factor in enumerate(process.get("allocationFactors", [])):
            allocation_type = factor.get("allocationType", "UNKNOWN_ALLOCATION")
            missing = []

            if "product" not in factor:
                missing.append("product")
            if allocation_type == "CAUSAL_ALLOCATION" and "exchange" not in factor:
                missing.append("exchange")

                faulty_processes.append({
                    "process_id": process_id,
                    "allocation_type": allocation_type,
                    "missing_components": missing,
                    "factor_index": idx
                })

    print("📋 Summary of faulty processes:")
    for entry in faulty_processes:
        print(
            f"- Process ID: {entry['process_id']}, "
            f"Allocation Type: {entry['allocation_type']}, "
            f"Missing: {', '.join(entry['missing_components'])}, "
            f"Factor Index: {entry['factor_index']}"
        )

    return faulty_processes

def find_unallocatable_processes(jsonld):
    """
    used to solve: "Default allocation chosen, but allocation factors for this method not provided"

    Identify and print processes in a JSONLDImporter object that require allocation
    but lack allocation factors for their default allocation method. This also occurs when the default allocation is set
    to 'NO_ALLOCATION" but allocation is required.

    changing the default allocation to an allocation type which match allocation factors included in attribute
    'allocationFactors'.

    :param jsonld:
    :return:
    """
    from collections import defaultdict
    for process_id, process in jsonld.data.get("processes", {}).items():
        # Skip if not allocatable
        if process.get("@type") in ("product", "emission"):
            continue
        if not process.get("allocationFactors"):
            continue
        allocation_dict = defaultdict(dict)
        try:
            for factor in process["allocationFactors"]:
                allocation_type = factor.get("allocationType")
                if allocation_type == "CAUSAL_ALLOCATION":
                    try:
                        product = factor["product"]["@id"]
                        flow = factor["exchange"]["flow"]["@id"]
                    except KeyError:
                        print(f"Skipping malformed CAUSAL_ALLOCATION in process {process_id}")
                        continue
                    if product not in allocation_dict["CAUSAL_ALLOCATION"]:
                        allocation_dict["CAUSAL_ALLOCATION"][product] = {}
                    allocation_dict["CAUSAL_ALLOCATION"][product][flow] = factor["value"]
                else:
                    product = factor.get("product", {}).get("@id")
                    if product:
                        allocation_dict[allocation_type][product] = factor["value"]
        except Exception as e:
            print(f"Error processing allocation factors in process {process_id}: {e}")
            continue
        default_method = process.get("defaultAllocationMethod")
        if default_method and default_method not in allocation_dict:
            print(f"❌ Unallocatable process: {process_id}")
            print(f"   Name: {process.get('name', 'Unnamed')}")
            print(f"   Default method: {default_method}")
            print(f"   Available methods: {list(allocation_dict.keys())}")
            print("-" * 60)




########################################################################################################################
## Methods for fixing errors which occur when calling .apply_strategies() on JSONLDImporter objects
########################################################################################################################

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

def append_jsonld_location(jsonld):
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
                "@id": "56bca136-90bb-3a77-9abb-7ce558af711e",
                "@type": "Location",
                "name": "Global"
            }
    return jsonld

########################################################################################################################
## Methods for fixing errors occuring when trying to write unlinked edges to excel
########################################################################################################################

def clean_all_locations(jsonld):
    """
    Clean and standardize 'location' fields in a JSONLDImporter object.

    This function addresses a specific issue encountered when exporting data to Excel
    using the `xlsxwriter` library, where a `TypeError: unhashable type: 'dict'` occurs.
    The root cause of this error is that some entries in the dataset contain a 'location'
    field that is either:
        - A dictionary (which is unhashable and cannot be used in Excel shared string tables),
        - `None`, or
        - Missing entirely.

    These invalid 'location' values are passed to `sheet.write_string()` in the
    `write_lci_matching()` function, which expects a string. When a non-string value
    (dict or NoneType) is passed, `xlsxwriter` fails when trying to store it in its
    internal shared string table.

    This function resolves the issue by:
        - Iterating over all entries in `uslci.data` and `uslci.products`,
        - Checking each dictionary for the presence and type of the 'location' field,
        - Replacing any non-string, missing, or None 'location' values with the string
          "no location",
        - Recursively checking all nested 'exchanges' within each dataset entry,
        - Logging each fix with the process ID and original value for traceability.

    Parameters
    ----------
    uslci : JSONLDImporter
        An instance of the JSONLDImporter class containing `.data` and `.products`
        attributes, each of which is a list of dictionaries representing LCI data.

    Returns
    -------
    None
        The function modifies the `uslci` object in-place and prints a summary of
        the changes made.
    """
    count_fixed = 0

    def clean_entry(entry, context=""):
        nonlocal count_fixed
        location = entry.get("location")
        if location is None or not isinstance(location, str):
            process_id = entry.get("id") or entry.get("code") or "(unknown ID)"
            print(f"[{context}] FIXING Process ID: {process_id}")
            print(f"Original location type: {type(location).__name__}")
            print(f"Original location content: {location}")
            print("-" * 40)
            entry["location"] = "no location"
            count_fixed += 1

    for entry in jsonld.data:
        clean_entry(entry, "DATA")
        for exc in entry.get("exchanges", []):
            clean_entry(exc, "EXCHANGE")

    for product in jsonld.products:
        clean_entry(product, "PRODUCT")

    print(f"✅ Total entries fixed: {count_fixed}")





