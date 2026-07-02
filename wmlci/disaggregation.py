
"""
Disaggregate multifunctional processes (JSONLDImporter-only)
- Global validation gate (with burden-free byproducts support)
- Allocation method resolver: PHYSICAL -> ECONOMIC -> CAUSAL
- Process splitting and exchange scaling by allocation factors
- defaultProvider updates using 3-key matching
"""

import copy
import math
from typing import Dict, List, Optional, Tuple
from esupy.util import make_uuid
from wmlci.log import log

#######################################
### Exchange classification helpers ###
#######################################

def is_product_exchange(exchange: Dict) -> bool:
    """Returns True if the exchange represents a product flow."""
    return exchange.get("flow", {}).get("flowType") == "PRODUCT_FLOW"


def get_product_exchanges(process: Dict) -> List[Dict]:
    """Returns all PRODUCT_FLOW output exchanges for a process."""
    return [
        exc for exc in process.get("exchanges", [])
        if not exc.get("isInput", False) and is_product_exchange(exc)
    ]

##################################
### Allocation method resolver ###
##################################

def _collect_allocation_values_for_method(
    process: Dict,
    method: str,
    products: List[Dict]
) -> Optional[List[float]]:
    """
    Return allocation values for each product under `method`, or None if incomplete/invalid.
    """
    factors = process.get("allocationFactors", [])
    by_product = {
        af.get("product", {}).get("@id"): af.get("value")
        for af in factors
        if af.get("allocationType") == method
    }
    values: List[float] = []
    for p in products:
        pid = p.get("flow", {}).get("@id")
        if pid not in by_product:
            return None
        v = by_product[pid]
        if v is None or v < 0:
            return None
        values.append(v)
    return values

def _is_sum_one(values: List[float], tolerance: float) -> bool:
    return math.isclose(sum(values), 1.0, rel_tol=0.0, abs_tol=tolerance)

def _is_burden_free_byproduct_case(values: List[float], tolerance: float) -> bool:
    """
    Valid burden-free byproduct case:
      - sum ~ 1.0
      - exactly one value ~ 1.0
      - all remaining values ~ 0.0
    """
    if not _is_sum_one(values, tolerance):
        return False
    ones = sum(1 for v in values if math.isclose(v, 1.0, abs_tol=tolerance))
    zeros = sum(1 for v in values if math.isclose(v, 0.0, abs_tol=tolerance))
    return ones == 1 and (ones + zeros) == len(values)

def _is_normal_allocation(values: List[float], tolerance: float) -> bool:
    """Valid normal allocation: all values > 0, sum ~ 1.0."""
    if not _is_sum_one(values, tolerance):
        return False
    return all(v > 0 for v in values)

def resolve_allocation_method_with_priority(
    process: Dict,
    tolerance: float = 0.01,
    priority: Optional[List[str]] = None
) -> Optional[str]:
    """
    Choose the best available allocation method for `process` using a priority order.

    Priority default: ["PHYSICAL_ALLOCATION", "ECONOMIC_ALLOCATION", "CAUSAL_ALLOCATION"]

    Valid if either:
      - normal allocation (all > 0, sum ~1), OR
      - burden-free byproducts (one 1.0, rest 0.0, sum ~1)

    Returns:
      - method string if a valid set is found in priority order
      - None if no method is valid
    """
    if priority is None:
        priority = ["PHYSICAL_ALLOCATION", "ECONOMIC_ALLOCATION", "CAUSAL_ALLOCATION"]

    products = get_product_exchanges(process)
    if len(products) <= 1:
        # Not multifunctional; no need to resolve beyond default
        return process.get("defaultAllocationMethod")

    for method in priority:
        values = _collect_allocation_values_for_method(process, method, products)
        if values is None:
            continue
        if _is_normal_allocation(values, tolerance) or _is_burden_free_byproduct_case(values, tolerance):
            return method

    return None

#############################
### Allocation validation ###
#############################

def validate_allocation_factors_for_process(
    process: Dict,
    tolerance: float = 0.01,
    allocation_method: Optional[str] = None
) -> bool:
    """
    Validate allocation factors for a multifunctional process.

    Supports two valid cases:
      (A) Normal allocation: all product factors > 0, sum ~ 1
      (B) Burden-free byproducts: exactly one product ~ 1, others ~ 0, sum ~ 1

    If `allocation_method` is given, validate specifically under that method;
    otherwise use process['defaultAllocationMethod'].
    """
    if allocation_method is None:
        allocation_method = process.get("defaultAllocationMethod")

    products = get_product_exchanges(process)
    if not products:
        return True  # nothing to validate

    values = _collect_allocation_values_for_method(process, allocation_method, products)
    if values is None:
        return False

    if _is_normal_allocation(values, tolerance):
        return True

    if _is_burden_free_byproduct_case(values, tolerance):
        log.info(
            f"Process '{process.get('name', '<unnamed>')}' "
            f"uses burden-free byproduct allocation under {allocation_method}"
        )
        return True

    return False

def validate_allocation_factors_globally(
    importer,
    tolerance: float = 0.01
) -> bool:
    """
    Global gate: validate all multifunctional processes before splitting.
    - Resolves best method via priority PHYSICAL -> ECONOMIC -> CAUSAL.
    - Overrides process['defaultAllocationMethod'] when the resolved method differs.
    - Logs method overrides and validation warnings.
    - Returns False (abort) only if no valid method can be found for a multifunctional process.
    """
    process_map: Dict[str, Dict] = importer.data.get("processes", {})

    for process_id, process in process_map.items():
        products = get_product_exchanges(process)
        if len(products) <= 1:
            continue  # Not multifunctional

        default_method = process.get("defaultAllocationMethod")
        resolved_method = resolve_allocation_method_with_priority(process, tolerance)

        if resolved_method is None:
            process_name = process.get("name", "<unnamed>")
            log.info(
                f"Global allocation validation failed: no valid method found "
                f"for process {process_name} ({process_id}); default={default_method}"
            )
            return False

        if resolved_method != default_method:
            process["defaultAllocationMethod"] = resolved_method
            log.info(
                f"Allocation method override: process '{process.get('name','<unnamed>')}' "
                f"({process_id}) default '{default_method}' → '{resolved_method}'"
            )

        # Validate under resolved method (now set as default)
        if not validate_allocation_factors_for_process(process, tolerance, allocation_method=resolved_method):
            process_name = process.get("name", "<unnamed>")
            log.info(
                f"Global allocation validation failed under method '{resolved_method}' "
                f"for process {process_name} ({process_id})"
            )
            return False

    return True

##########################
### Allocation helpers ###
##########################

def get_allocation_factor(
    process: Dict,
    product_flow_id: str
) -> float:
    """
    Retrieve allocation factor value for a product flow using the process's
    current default allocation method (possibly overridden by the global gate).
    """
    allocation_method = process.get("defaultAllocationMethod")
    for af in process.get("allocationFactors", []):
        if (
            af.get("allocationType") == allocation_method
            and af.get("product", {}).get("@id") == product_flow_id
        ):
            return af.get("value", 0.0)
    raise ValueError(f"Missing allocation factor for product {product_flow_id}")

#############################
### Exchange edit helpers ###
#############################

def scale_exchange_amount(exchange: Dict, factor: float) -> None:
    """Scale exchange amount by allocation factor."""
    exchange["amount"] = exchange.get("amount", 0.0) * factor

def set_quantitative_reference(exchange: Dict) -> None:
    """Mark an exchange as the quantitative reference."""
    exchange["isQuantitativeReference"] = True

def update_process_identity(process: Dict, product_exchange: Dict) -> None:
    """
    Update process name and UUID for product-specific copy.
    Child @id = make_uuid(child_name) for deterministic UUIDs based on name.
    """
    original_name = process.get("name", "")
    product_name = product_exchange.get("flow", {}).get("name", "")
    process["name"] = f"{original_name}; {product_name}"
    process["@id"] = make_uuid(process["name"])

def filter_and_scale_exchanges(
    exchanges: List[Dict],
    keep_product_flow_id: str,
    allocation_factor: float
) -> List[Dict]:
    """
    Keep only the target PRODUCT_FLOW exchange (as the quantitative reference)
    and scale all non-product exchanges by the allocation factor.
    """
    updated_exchanges = []
    for exc in exchanges:
        if is_product_exchange(exc):
            if exc.get("flow", {}).get("@id") == keep_product_flow_id:
                set_quantitative_reference(exc)
                updated_exchanges.append(exc)
            # Drop other product flows
        else:
            scale_exchange_amount(exc, allocation_factor)
            updated_exchanges.append(exc)
    return updated_exchanges

#################################################
### Child-mapping for defaultProvider updates ###
#################################################

def build_child_mappings_for_process(
    parent_process: Dict,
    parent_process_id: str,
    new_children: List[Dict]
) -> List[Dict]:
    """
    Build mapping entries that connect:
      (product_flow_id, product_flow_name, parent_process_id)
        -> (child_process_id, child_process_name)

    These mappings drive defaultProvider updates elsewhere in the database.
    """
    mappings = []
    parent_name = parent_process.get("name", "")

    for child in new_children:
        child_name = child.get("name", "")
        child_id = child.get("@id", "")
        # Retrieve the kept product exchange (quant ref) in the child
        kept_products = [
            exc for exc in child.get("exchanges", [])
            if is_product_exchange(exc) and exc.get("isQuantitativeReference", False)
        ]
        if not kept_products:
            continue  # defensive guard

        product_flow = kept_products[0].get("flow", {})
        product_flow_id = product_flow.get("@id", "")
        product_flow_name = product_flow.get("name", "")

        mappings.append({
            "parent_id": parent_process_id,           # original process uuid
            "parent_name": parent_name,               # optional (for logging)
            "product_flow_id": product_flow_id,       # product flow uuid
            "product_flow_name": product_flow_name,   # product flow name
            "child_id": child_id,                     # new child process uuid
            "child_name": child_name,                 # new child process name
        })

    return mappings

def update_default_providers_for_children(
    importer,
    child_mappings: List[Dict]
) -> None:
    """
    Update exchanges' defaultProvider to point to the new child process where ALL match:
      - exchange.flow['@id']  == mapping['product_flow_id']
      - exchange.flow['name'] == mapping['product_flow_name']
      - exchange.defaultProvider['@id'] == mapping['parent_id']

    On match, sets:
      - defaultProvider['@id']   = mapping['child_id']
      - defaultProvider['name']  = mapping['child_name']
    """
    process_map: Dict[str, Dict] = importer.data.get("processes", {})

    idx: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
    for m in child_mappings:
        key = (m["product_flow_id"], m["product_flow_name"], m["parent_id"])
        idx[key] = (m["child_id"], m["child_name"])

    for _, proc in process_map.items():
        exchanges = proc.get("exchanges", [])
        for exc in exchanges:
            flow = exc.get("flow")
            default_provider = exc.get("defaultProvider")
            if not (flow and default_provider):
                continue

            flow_id = flow.get("@id")
            flow_name = flow.get("name")
            parent_id = default_provider.get("@id")

            key = (flow_id, flow_name, parent_id)
            child = idx.get(key)
            if child:
                child_id, child_name = child
                default_provider["@id"] = child_id
                default_provider["name"] = child_name
                # Optional: copy/sync other fields if needed
                # default_provider["processType"] = "UNIT_PROCESS"
                # default_provider["category"] = default_provider.get("category")
                # default_provider["location"] = default_provider.get("location")

#########################
### Process splitting ###
#########################

def split_process_by_products(process: Dict) -> List[Dict]:
    """
    Split a multifunctional process into single-product processes
    using validated/overridden allocation factors.

    If the process is not multifunctional or fails validation, returns [process].
    """
    product_exchanges = get_product_exchanges(process)
    if len(product_exchanges) <= 1:
        return [process]

    # Defensive local validation (global gate should have resolved/overridden already)
    if not validate_allocation_factors_for_process(process):
        process_name = process.get("name", "<unnamed>")
        process_id = process.get("@id", "<missing-id>")
        log.info(
            f"Allocation validation failed for process {process_name} ({process_id})"
        )
        return [process]

    new_processes = []
    for product_exc in product_exchanges:
        product_flow_id = product_exc.get("flow", {}).get("@id")
        allocation_factor = get_allocation_factor(process, product_flow_id)

        new_process = copy.deepcopy(process)
        update_process_identity(new_process, product_exc)

        new_process["exchanges"] = filter_and_scale_exchanges(
            new_process.get("exchanges", []),
            product_flow_id,
            allocation_factor
        )
        new_processes.append(new_process)

    return new_processes

def split_multi_product_processes(importer):
    """
    Orchestrates:
      1) Global validation with method resolution (priority)
      2) Splitting into child processes
      3) defaultProvider updates based on 3-key child mappings

    Mutates importer.data['processes'] in place and returns the importer.
    Expected shape: importer.data['processes'] == {uuid: process_dict}
    """
    # 1) Global gate: abort only if a multifunctional process has no valid allocation method
    if not validate_allocation_factors_globally(importer):
        log.info("Aborting process splitting due to allocation validation failure.")
        return importer

    process_map: Dict[str, Dict] = importer.data.get("processes", {})

    updated_processes: Dict[str, Dict] = {}
    all_child_mappings: List[Dict] = []

    # 2) Split each process; collect child mappings
    for parent_id, parent_proc in process_map.items():
        new_children = split_process_by_products(parent_proc)

        # Build mapping entries for defaultProvider updates
        all_child_mappings.extend(
            build_child_mappings_for_process(parent_proc, parent_id, new_children)
        )

        # Add each new (or unchanged) process into result dict keyed by its @id
        for child in new_children:
            updated_processes[child["@id"]] = child

    # Write the split processes back
    importer.data["processes"] = updated_processes

    # 3) Update defaultProvider references across the entire DB using 3-key matching
    update_default_providers_for_children(importer, all_child_mappings)

    return importer
