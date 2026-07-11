"""
Functions to clean up imported olca data and generate square technosphere matrix
"""

import fedelemflowlist
import pandas as pd

from bw2io.importers.json_ld import JSONLDImporter

from wmlci.errorLogging import validate_jsonld_exchanges
from wmlci.log import log

from typing import Set

# values that mean "no FEDEFL target" in the fedelemflowlist mapping tables
_NO_TARGET = {"n.a.", "nan", "none", ""}


######################################################
### Remove exchanges and processes with no impacts ###
######################################################

def remove_impact_free_objects(importer) -> None:
    """
    Recursively identifies and removes exchanges and processes that have no environmental impacts.

    A process is considered impact-free if:
    - It has no output exchanges that are elementary flows.
    - All its input exchanges either:
        - Have no default provider, or
        - Reference other processes that are also impact-free.

    The function modifies the input `data` dictionary in-place by:
    - Removing exchanges from processes that are impact-free.
    - Removing processes that are impact-free and no longer referenced.

    Parameters:
    - data (dict): A dictionary containing a 'processes' key with process definitions.

    Returns:
    - None
    """
    process_dict_by_id = importer.data.get('processes', {}) # Get the dictionary of all processes by their unique ID
    impact_free_status_by_id = {} # Cache to store whether each process is impact-free (True/False)
    all_referenced_provider_ids = set() # Set to track all process IDs that are referenced as default providers
    exchange_removal_count = 0 # Counter for removed exchanges
    process_removal_count = 0 # Counter for removed processes

    def check_if_process_is_impact_free(target_process_id: str, visited_process_ids: Set[str]) -> bool:
        """
        Recursively determines if a process is impact-free.

        Parameters:
        - target_process_id (str): The ID of the process to evaluate.
        - visited_process_ids (set): Set of process IDs visited in the current recursion stack to avoid cycles.

        Returns:
        - bool: True if the process is impact-free, False otherwise.
        """
        # If this process has already been evaluated, return the cached result
        if target_process_id in impact_free_status_by_id:
            return impact_free_status_by_id[target_process_id]
        # If this process is already in the current recursion stack, assume it's impact-free to avoid infinite loops
        if target_process_id in visited_process_ids:
            return True
        visited_process_ids.add(target_process_id) # Mark this process as visited
        # Get the process data and its exchanges
        target_process_data = process_dict_by_id.get(target_process_id, {})
        target_process_exchanges = target_process_data.get('exchanges', [])
        # Check if the process has any output exchange that is an elementary flow
        for exchange_out in target_process_exchanges:
            # If the exchange is an output and its flow type is ELEMENTARY_FLOW, the process has impacts
            if not exchange_out.get('isInput') and exchange_out.get('flow', {}).get('flowType') == 'ELEMENTARY_FLOW':
                impact_free_status_by_id[target_process_id] = False
                return False
        # Check all input exchanges for default providers
        for exchange_in in target_process_exchanges:
            # If the exchange is an input and its flow type is PRODUCT_FLOW, it may reference another process
            if exchange_in.get('isInput') and exchange_in.get('flow', {}).get('flowType') == 'PRODUCT_FLOW':
                input_default_provider = exchange_in.get('defaultProvider')
                # If the exchange has a default provider, we need to check if that provider has impacts
                if input_default_provider:
                    input_provider_process_id = input_default_provider.get('@id')
                    # Track that this provider is referenced
                    all_referenced_provider_ids.add(input_provider_process_id)
                    # If the provider process is not impact-free, then this process is not impact-free
                    if not check_if_process_is_impact_free(input_provider_process_id, visited_process_ids):
                        impact_free_status_by_id[target_process_id] = False
                        return False
        # If no impacts found, mark this process as impact-free
        impact_free_status_by_id[target_process_id] = True
        return True

    # Pass 1: Evaluate all processes and cache their impact-free status
    for process_id_to_check in list(process_dict_by_id.keys()):
        check_if_process_is_impact_free(process_id_to_check, set())

    # Pass 2: Remove input exchanges that reference impact-free providers
    for process_id_to_clean, process_data_to_clean in process_dict_by_id.items():
        retained_exchanges = []
        for exchange_to_check in process_data_to_clean.get('exchanges', []):
            # If the exchange is an input and its flow type is PRODUCT_FLOW, it may reference a provider
            if exchange_to_check.get('isInput') and exchange_to_check.get('flow', {}).get('flowType') == 'PRODUCT_FLOW':
                provider_info_to_check = exchange_to_check.get('defaultProvider')
                # If the exchange has a default provider, check if it's impact-free
                if provider_info_to_check:
                    provider_id_to_check = provider_info_to_check.get('@id')
                    # If the provider is impact-free, skip this exchange
                    if impact_free_status_by_id.get(provider_id_to_check, False):
                        log.info(f"Removing exchange from process '{process_id_to_clean}' because its provider '{provider_id_to_check}' is impact-free.")
                        exchange_removal_count += 1
                        continue
            # Keep the exchange if not removed
            retained_exchanges.append(exchange_to_check)
        # Update the process with the filtered list of exchanges
        process_data_to_clean['exchanges'] = retained_exchanges

    # Pass 3: Remove processes that are impact-free and not referenced by any other process
    for process_id_to_remove in list(process_dict_by_id.keys()):
        # If the process is impact-free and not referenced by any other process, delete it
        if impact_free_status_by_id.get(process_id_to_remove, False) and process_id_to_remove not in all_referenced_provider_ids:
            log.info(f"Removing process '{process_id_to_remove}' because it is impact-free and unreferenced.")
            process_removal_count += 1
            del process_dict_by_id[process_id_to_remove]

    # Final summary of removals
    log.info(f"Total exchanges removed: {exchange_removal_count}")
    log.info(f"Total processes removed: {process_removal_count}")

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
    log.info('\nApplying the Opposite Direction Approach...')
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
                if flow.get("flowType") == 'WASTE_FLOW' and exchange.get("isInput") == False:
                    exchange["amount"] *= -1  # make value negative
                    exchange["isInput"] = True # make input
                # Edit waste input to waste treatment
                if flow.get("flowType") == 'WASTE_FLOW' and exchange.get("isInput") == True:
                    exchange["amount"] *= -1  # make value negative
                    exchange["isQuantitativeReference"] = True  # make quantitative reference
                    exchange["isInput"] = False  # make output
                    flow["flowType"] = "PRODUCT_FLOW"  # make product flow
    return jsonld

###########################
### Fix location issues ###
###########################

def reset_location_dict(jsonld):
    """
    Reset the 'locations' dictionary in a JSON-LD importer:
    - Delete all existing entries.
    - Add a single entry with predefined data.

    Parameters
    ----------
    jsonld : bw2io.importers.json_ld.JSONLDImporter
        The JSON-LD importer instance with `data` attribute.

    Returns
    -------
    bw2io.importers.json_ld.JSONLDImporter
        The same importer instance, with updated locations.
    """
    # Reset locations dictionary
    jsonld.data["locations"] = {
        "0b3b97fa-6688-3c56-88ee-4ae80ec0c3c2": {
            "@type": "Location",
            "@id": "0b3b97fa-6688-3c56-88ee-4ae80ec0c3c2",
            "name": "United States",
            "category": "Country",
            "version": "00.00.000",
            "code": "US",
            "latitude": 45.68811936470228,
            "longitude": -112.49616351105776
        }
    }
    return jsonld

def replace_process_location(jsonld):
    """
    Ensure each process in jsonld.data['processes'] has a location dictionary
    with the required keys and values. Replace existing location or add if missing.

    Parameters
    ----------
    jsonld : bw2io.importers.json_ld.JSONLDImporter
        The JSON-LD importer instance.

    Returns
    -------
    bw2io.importers.json_ld.JSONLDImporter
        The same importer instance with updated process locations.
    """
    log.info("\nAdding or replacing locations in processes...")

    # Define the standard location dictionary
    standard_location = {
        "@type": "Location",
        "@id": "0b3b97fa-6688-3c56-88ee-4ae80ec0c3c2",
        "name": "United States",
        "category": "Country"
    }

    for process_id, process in jsonld.data.get("processes", {}).items():
        # Skip processes of type emission or product
        if process.get("type") in {"emission", "product"}:
            continue

        # Replace existing location or add new one
        process["location"] = standard_location.copy()

    return jsonld

def replace_exchange_locations(jsonld):
    """
    Ensures all exchanges in each process have a location dictionary
    matching the parent process location dictionary.
    - If an exchange has a location (string or dict), replace it with the parent's location dict.
    - If an exchange has no location, add the parent's location dict.

    Parameters
    ----------
    jsonld : JSONLDImporter
        An instance of the JSONLDImporter class containing `.data`.

    Returns
    -------
    JSONLDImporter
        The modified JSONLDImporter object with updated exchange locations.
    """
    log.info("\nReplacing exchange locations with parent process location dictionary...")

    for pid, process in jsonld.data.get("processes", {}).items():
        for exc in process.get("exchanges", []):
            # Always replace or add location with parent's location dict
            exc["location"] = "United States"

    return jsonld

###########################
### Miscellaneous fixes ###
###########################

def remove_process_allocation_factors(jsonld):
    """
    The Waste Reduction Model openLCA processes include allocationFactors that are at times missing "exchange" information.
    This missing information results in the error:
    bw2io.errors.UnallocatableDataset: We currently only support exchange-specific CAUSAL_ALLOCATION
    As all have factors of "1" - this information can be removed
    :param jsonld:
    :return:
    """
    log.info("\n Removing faulty allocation factors...")
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
            if "IsInput" in exc:
                exc["input"] = exc.pop("IsInput")
    return jsonld

def convert_param_list_to_dict(jsonld):
    """
    Convert parameter lists inside each process of a JSON-LD importer
    to dictionaries keyed by uuid, and return the updated importer.

    Parameters
    ----------
    jsonld : bw2io.importers.json_ld.JSONLDImporter
        The JSON-LD importer instance with `data` attribute.

    Returns
    -------
    bw2io.importers.json_ld.JSONLDImporter
        The same importer instance, with updated data.
    """
    processes = jsonld.data.get("processes", {})
    for process_id, process in processes.items():
        params_list = process.get("parameters", [])
        if isinstance(params_list, list):
            # Convert list to dict keyed by 'name'
            params_dict = {param["@id"]: param for param in params_list if "@id" in param}
            process["parameters"] = params_dict

    return jsonld

def convert_lcia_param_list_to_dict(jsonld):
    """
    Convert parameter lists inside each lcia_category of a JSON-LD LCIA importer
    to dictionaries keyed by uuid, and return the updated importer.

    Parameters
    ----------
    jsonld : bw2io.importers.json_ld.JSONLDLCIAImporter
        The JSON-LD importer instance with `data` attribute.

    Returns
    -------
    bw2io.importers.json_ld.JSONLDLCIAImporter
        The same importer instance, with updated data.
    """
    lcia_cats = jsonld.data.get("lcia_categories", {})
    for cat, lcia_cat in lcia_cats.items():
        params_list = lcia_cat.get("parameters", [])
        if isinstance(params_list, list):
            # Convert list to dict keyed by 'name'
            params_dict = {param["@id"]: param for param in params_list if "@id" in param}
            lcia_cat["parameters"] = params_dict

    return jsonld


def map_to_fedelemflowlist_UUIDs(jsonld, sourcelistname="WARM"):
    """
    Harmonize inventory elementary flows to the EPA Federal Elementary Flow
    List (FEDEFL) using a fedelemflowlist mapping table (matched by source UUID).

    https://github.com/FLCAC-admin/fedelemflowlist

    Parameters
    ----------
    jsonld : bw2io.importers.json_ld.JSONLDImporter
        Inventory importer, before ``apply_strategies()``.
    sourcelistname : str
        FEDEFL mapping table to use for this inventory (default 'WARM').

    Returns
    -------
    jsonld : the same importer with elementary flows rewritten to FEDEFL UUIDs.
    """
    mapping = fedelemflowlist.get_flowmapping(sourcelistname)

    # keep only elementary-flow mappings that have a source UUID and a
    # FEDEFL target UUID (drop economic flows and 'n.a.' targets)
    mapping = mapping.dropna(subset=["SourceFlowUUID", "TargetFlowUUID"])
    mapping = mapping[
        mapping["SourceFlowContext"]
        .astype(str)
        .str.contains("Elementary", case=False, na=False)
    ]
    mapping = mapping[
        ~mapping["TargetFlowUUID"].astype(str).str.strip().str.lower().isin(_NO_TARGET)
    ]
    mapping = mapping.drop_duplicates(subset=["SourceFlowUUID"], keep="first")

    mapping_dict = (
        mapping.set_index("SourceFlowUUID")[
            [
                "TargetFlowUUID",
                "TargetFlowName",
                "TargetFlowContext",
                "ConversionFactor",
                "TargetUnit",
            ]
        ].to_dict(orient="index")
    )
    log.info(
        f"Using {len(mapping_dict)} '{sourcelistname}' -> FEDEFL elementary "
        "flow mappings."
    )

    # rewrite the top-level flows dict, re-keying by the FEDEFL target UUID.
    # Several source flows can collapse onto one FEDEFL flow (e.g. multiple
    # carbon sources -> Carbon dioxide).
    flows = jsonld.data.get("flows", {})
    updated_flows = {}
    flows_remapped = 0
    for key, value in flows.items():
        target = mapping_dict.get(key)
        if target:
            target_id = target["TargetFlowUUID"]
            value["@id"] = target_id
            value["name"] = target["TargetFlowName"]
            value["category"] = target["TargetFlowContext"]
            updated_flows[target_id] = value
            flows_remapped += 1
        else:
            updated_flows[key] = value
    jsonld.data["flows"] = updated_flows

    # rewrite exchange flows and apply the conversion factor to amounts/units
    exchanges_remapped = 0
    for process_k, process in jsonld.data.get("processes", {}).items():
        for idx, exchange in enumerate(process.get("exchanges", [])):
            flow = exchange.get("flow", {})
            if not isinstance(flow, dict):
                continue
            target = mapping_dict.get(flow.get("@id"))
            if not target:
                continue
            flow["@id"] = target["TargetFlowUUID"]
            flow["name"] = target["TargetFlowName"]
            flow["category"] = target["TargetFlowContext"]
            exchanges_remapped += 1

            try:
                conversion_factor = float(target.get("ConversionFactor") or 1)
            except (TypeError, ValueError):
                conversion_factor = 1.0
            if conversion_factor != 1 and "amount" in exchange:
                if "amountFormula" in exchange:
                    original_formula = exchange["amountFormula"]
                    unit = exchange.get("unit")
                    if isinstance(unit, dict) and unit.get("name"):
                        original_unit = unit["name"]
                    else:
                        original_unit = flow.get("refUnit") or "unknown unit"
                    target_unit = target.get("TargetUnit")
                    new_formula = f"({original_formula}) * {conversion_factor}"
                    exchange["amountFormula"] = new_formula
                    exchange["amount"] = exchange["amount"] * conversion_factor
                    if "refUnit" in flow and target_unit:
                        flow["refUnit"] = target_unit
                    log.info(
                        f"Formula '{original_formula}' [{original_unit}] updated to "
                        f"'{new_formula}' [{target_unit or original_unit}]"
                    )
                else:
                    exchange["amount"] = exchange["amount"] * conversion_factor
                    target_unit = target.get("TargetUnit")
                    if "refUnit" in flow and target_unit:
                        flow["refUnit"] = target_unit

    log.info(
        f"Harmonized {flows_remapped} flows and {exchanges_remapped} exchange "
        f"flows to FEDEFL UUIDs using '{sourcelistname}'."
    )

    # rebuild snapshot from the harmonized flows so exchanges (FEDEFL codes)
    # link to biosphere nodes (FEDEFL codes).
    if hasattr(jsonld, "biosphere_database") and hasattr(
        jsonld, "flows_as_biosphere_database"
    ):
        jsonld.biosphere_database[:] = jsonld.flows_as_biosphere_database(
            jsonld.data, jsonld.db_name
        )
        log.info(
            f"Rebuilt biosphere node snapshot with "
            f"{len(jsonld.biosphere_database)} FEDEFL-harmonized flows."
        )

    issues = validate_jsonld_exchanges(jsonld)
    if issues:
        log.warning("Validation found problems:")
        for issue in issues:
            log.warning(" - " + issue)
    else:
        log.info("All exchanges validated successfully.")

    return jsonld


def map_lcia_to_fedelemflowlist_UUIDs(
    jsonld_lcia,
    sourcelistname="IPCC",
    preferred_target_context="emission/air",
):
    """
    Harmonize LCIA characterization factors to FEDEFL using a fedelemflowlist
    mapping table (matched by source flow NAME).

    Parameters
    ----------
    jsonld_lcia : bw2io.importers.json_ld_lcia.JSONLDLCIAImporter
        LCIA importer after ``apply_strategies()``.
    sourcelistname : str
        FEDEFL mapping table for the method (default 'IPCC').
    preferred_target_context : str
        FEDEFL target compartment to prefer when a flow name has several
        context variants (default 'emission/air').

    Returns
    -------
    jsonld_lcia : the same importer with CF flow UUIDs rewritten to FEDEFL.
    """
    mapping = fedelemflowlist.get_flowmapping(sourcelistname)
    mapping = mapping.dropna(subset=["SourceFlowName", "TargetFlowUUID"])
    mapping = mapping[
        ~mapping["TargetFlowUUID"].astype(str).str.strip().str.lower().isin(_NO_TARGET)
    ]

    # choose a single FEDEFL target per source flow name, preferring the general
    # air compartment, otherwise the most general (shortest) context string.
    name_to_target = {}
    for _, row in mapping.iterrows():
        name = str(row["SourceFlowName"]).strip().lower()
        context = str(row.get("TargetFlowContext") or "")
        candidate = {
            "TargetFlowUUID": row["TargetFlowUUID"],
            "TargetFlowName": row["TargetFlowName"],
            "TargetFlowContext": context,
        }
        current = name_to_target.get(name)
        if current is None:
            name_to_target[name] = candidate
        elif context == preferred_target_context:
            name_to_target[name] = candidate
        elif (
            current["TargetFlowContext"] != preferred_target_context
            and len(context) < len(current["TargetFlowContext"])
        ):
            name_to_target[name] = candidate

    matched = 0
    duplicates_dropped = 0
    for method in jsonld_lcia.data:
        seen = {}  # FEDEFL UUID -> cf, to avoid double-characterizing a flow
        kept = []
        for cf in method.get("exchanges", []):
            flow = cf.get("flow", {})
            name = (flow.get("name") or "").strip().lower()
            target = name_to_target.get(name)
            if not target:
                kept.append(cf)
                continue
            flow["@id"] = target["TargetFlowUUID"]
            flow["name"] = target["TargetFlowName"]
            flow["category"] = target["TargetFlowContext"]
            matched += 1

            target_id = target["TargetFlowUUID"]
            existing = seen.get(target_id)
            if existing is None:
                seen[target_id] = cf
                kept.append(cf)
            else:
                # two source names collapsed onto one FEDEFL flow; keep the
                # larger-magnitude CF so we neither double count nor lose signal
                duplicates_dropped += 1
                if abs(cf.get("amount", 0)) > abs(existing.get("amount", 0)):
                    existing.update(
                        {k: v for k, v in cf.items() if k != "flow"}
                    )
                    existing["flow"] = cf["flow"]
        method["exchanges"] = kept

    log.info(
        f"Harmonized {matched} LCIA characterization factors to FEDEFL UUIDs "
        f"using '{sourcelistname}' ({duplicates_dropped} duplicate CFs merged)."
    )
    return jsonld_lcia
