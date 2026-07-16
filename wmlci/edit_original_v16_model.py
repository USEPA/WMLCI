"""
Import unedited Waste Reduction Model v16.

Prioritize key waste treatment pathways by removing all processes except:
    1) MSW combustion of Mixed Plastics
    2) MSW landfilling of Food Waste; National average LFG recovery, typical collection, National average conditions
    3) MSW recycling of Mixed Plastics

Fix known issues.
"""

from wmlci.jsonld_loader import load_JSONLD_sourceData

from collections import deque

from wmlci.log import log

# Import JSON file
json_ld = load_JSONLD_sourceData('waste_reduction_model_v16', datatype="jsonld", bw_database_name='db')

# key processes that won't be removed from the database
processes_keep = ['e847ff05-48e3-4df0-ae4d-db2bafe56baf', # mixed plastics combustion
                  'b7eb29f9-d173-4ec5-9710-5f451b6bbfce', # food waste landfilling
                  '1b246a12-d894-4381-ab43-f93c86c59b6f'] # mixed plastics recycling

### Method definitions

def prune_to_supply_chain(importer, process_ids):
    """
    Takes jsonld importer and a list of uuids as arguments.
    Retains processes in list, default providers of exchanges from processes in list, and flows from both prior items.
    Removes all other processes and flows.
    """
    processes = importer.data["processes"]
    flows = importer.data["flows"]

    processes_to_keep = set()
    flows_to_keep = set()

    # Initialize queue
    queue = deque(process_ids)

    # Traverse process dependency graph
    while queue:
        process_id = queue.popleft()
        # Skip previously visited processes
        if process_id in processes_to_keep:
            continue
        # Skip missing references
        if process_id not in processes:
            print(f"Warning: Process '{process_id}' not found.")
            continue

        processes_to_keep.add(process_id)
        process = processes[process_id]

        # Collect flows and discover providers
        for exchange in process.get("exchanges", []):
            flow = exchange.get("flow")
            if flow and "@id" in flow:
                flows_to_keep.add(flow["@id"])
            provider = exchange.get("defaultProvider")
            if provider and "@id" in provider:
                provider_id = provider["@id"]
                if provider_id not in processes_to_keep:
                    queue.append(provider_id)

        # Collect additional flow references from allocation factors
        for allocation_factor in process.get("allocationFactors", []):
            product = allocation_factor.get("product")
            if product and "@id" in product:
                flows_to_keep.add(product["@id"])
            allocation_exchange = allocation_factor.get("exchange")
            if allocation_exchange:
                flow = allocation_exchange.get("flow")
                if flow and "@id" in flow:
                    flows_to_keep.add(flow["@id"])

    # Remove unused processes
    importer.data["processes"] = {
        process_id: process_data
        for process_id, process_data in processes.items()
        if process_id in processes_to_keep
    }
    # Remove unused flows
    importer.data["flows"] = {
        flow_id: flow_data
        for flow_id, flow_data in flows.items()
        if flow_id in flows_to_keep
    }
    return importer

def remove_processes(importer, process_uuids):
    """
    Remove processes and exchanges that reference them as default providers.
    """
    process_uuids = set(process_uuids)
    processes = importer.data["processes"]
    exchanges_removed = 0

    # Remove exchanges with matching default providers
    for process in processes.values():

        exchanges = process.get("exchanges", [])

        filtered_exchanges = [
            exchange
            for exchange in exchanges
            if exchange.get("defaultProvider", {}).get("@id")
            not in process_uuids
        ]

        exchanges_removed += len(exchanges) - len(filtered_exchanges)

        process["exchanges"] = filtered_exchanges

    # Remove processes
    processes_removed = 0

    for process_uuid in process_uuids:
        if processes.pop(process_uuid, None) is not None:
            processes_removed += 1

    log.info(
        f"Removed {processes_removed} process(es) and "
        f"{exchanges_removed} exchange(s)."
    )

    return importer

def remove_flows(importer, flow_uuids):
    """
    Remove flows and exchanges that reference them.
    """

    flow_uuids = set(flow_uuids)

    processes = importer.data["processes"]
    flows = importer.data["flows"]

    exchanges_removed = 0

    # Remove exchanges with matching flow ids
    for process in processes.values():

        exchanges = process.get("exchanges", [])

        filtered_exchanges = [
            exchange
            for exchange in exchanges
            if exchange.get("flow", {}).get("@id")
            not in flow_uuids
        ]

        exchanges_removed += len(exchanges) - len(filtered_exchanges)

        process["exchanges"] = filtered_exchanges

    # Remove flows
    flows_removed = 0

    for flow_uuid in flow_uuids:
        if flows.pop(flow_uuid, None) is not None:
            flows_removed += 1

    log.info(
        f"Removed {flows_removed} flow(s) and "
        f"{exchanges_removed} exchange(s)."
    )

    return importer

def avoided_product_to_technosphere(importer):
    """
    Convert avoided product exchanges into technosphere exchanges.
    Where 'isAvoidedProduct' = True, the following edits are made:
        - isAvoidedProduct -> False
        - isInput -> True
        - flow["flowType"] -> "PRODUCT_FLOW"
        - amountFormula -> "-1*" + original amountFormula
    """

    exchanges_modified = 0

    for process in importer.data["processes"].values():

        for exchange in process.get("exchanges", []):

            if exchange.get("isAvoidedProduct") is True:

                # Convert avoided product to technosphere exchange
                exchange["isAvoidedProduct"] = False
                exchange["isInput"] = True

                # Ensure flow is a product flow
                flow = exchange.get("flow")
                if flow:
                    flow["flowType"] = "PRODUCT_FLOW"

                # Get amountFormula and amount
                amount_formula = exchange.get("amountFormula")
                amount = exchange.get("amount")

                if amount_formula:
                    # Only make edit if the amount is greater than 0
                    if not amount_formula.startswith("-1*") and amount>0:
                        exchange["amountFormula"] = f"-1*{amount_formula}"
                        exchange["amount"] = -1 * amount

                exchanges_modified += 1

    log.info(f"Modified {exchanges_modified} avoided product exchange(s).")

    return importer

def fix_transport_equation(importer):
    """
    Update transport equations by removing the hard-coded subtraction
    of 20 miles.

    Replacements:
    1) 1.0*(transport_distance_combustion-20) to 1.0*(transport_distance_combustion)
    2) 1.0*(transport_distance_recycling-20) to 1.0*(transport_distance_recycling)
    """

    replacements = {
        "1.0*(transport_distance_combustion-20)":
            "1.0*(transport_distance_combustion)",
        "1.0*(transport_distance_recycling-20)":
            "1.0*(transport_distance_recycling)",
    }

    exchanges_modified = 0

    for process in importer.data["processes"].values():
        for exchange in process.get("exchanges", []):
            amount_formula = exchange.get("amountFormula")
            if amount_formula in replacements:
                exchange["amountFormula"] = replacements[amount_formula]
                exchanges_modified += 1

    log.info(f"Modified {exchanges_modified} transport equation(s).")

    return importer

def rename_parameters(importer, parameter_updates):
    """
    Rename parameters in importer.data["parameters"] using a mapping of
    parameter UUIDs to new parameter names.
    """

    parameters = importer.data["parameters"]

    parameters_modified = 0

    for parameter_uuid, new_name in parameter_updates.items():

        parameter = parameters.get(parameter_uuid)

        if parameter is None:
            log.info(f"Warning: Parameter '{parameter_uuid}' not found.")
            continue

        parameter["name"] = new_name
        parameters_modified += 1

    log.info(f"Renamed {parameters_modified} parameter(s).")

    return importer

### Apply methods

# Remove processes that are not key processes or their upstream processes
json_ld = prune_to_supply_chain(json_ld, processes_keep)

# Remove processes and references to them in defaultProviders
processes_remove = ['bfabc9c0-f0e1-432b-ba2e-600d3119f2d1'] # Wood chipping process
json_ld = remove_processes(json_ld, processes_remove)

# Remove exchanges from processes and data.flows
flows_remove = [
    "f45fbcf0-24f0-4ce1-b1d9-78f2ed0d5a34", # Jobs flow
    "c9f2c667-eeab-448e-9c0c-6da04000fda6", # Taxes flow
    "8c2daef6-fd94-410b-a258-7df1f02c1bce", # Wages flow
    "24ecda7d-7c05-45f6-94d2-005924940d26", # Wood chipping flow
    "e3b06ae8-cedf-4bb7-a864-ff8c4c0950e9", # Steel, recycled flow
    "bad888b4-4615-4904-953f-c67d2ca5f41f"  # Other means of transport flow
]
json_ld = remove_flows(json_ld, flows_remove)

# Convert avoided products to negative technosphere flows
json_ld = avoided_product_to_technosphere(json_ld)

# Fix transport exchange formula
json_ld = fix_transport_equation(json_ld)

# fix parameter names
param_fixes = {"fbf4145a-5f38-4b45-aa7c-ff4d5a44f95d":'Fugitive_CH4_diesel'}
json_ld = rename_parameters(json_ld, param_fixes)