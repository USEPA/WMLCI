"""
Import unedited Waste Reduction Model v16.

Prioritize key waste treatment pathways by removing all processes except:
    1) MSW combustion of Mixed Plastics
    2) MSW landfilling of Food Waste; National average LFG recovery, typical
       collection, National average conditions
    3) MSW recycling of Mixed Plastics

These processes are specified in waste_reduction_model_v16_pilot.yaml.

Fix known issues.
"""

from __future__ import annotations

import json
import shutil
from collections import deque
from pathlib import Path

from wmlci.jsonld_loader import load_JSONLD_sourceData
from wmlci.log import log
from wmlci.settings import source_data_path


def prune_to_supply_chain(importer, process_ids):
    """
    Takes jsonld importer and a list of uuids as arguments.
    Retains processes in list, default providers of exchanges from those
    processes, and flows from both prior items.
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
                    if not amount_formula.startswith("-1*") and amount > 0:
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

def make_pilot(
    method_name=None,
    config=None,
    output_dir=None,
    bw_database_name="db",
):
    """Build the pilot JSON-LD source data product from original data input"""
    config = config or {}
    base_source = config.get("base_source")
    json_ld = load_JSONLD_sourceData(
        base_source,
        datatype="jsonld",
        bw_database_name=bw_database_name,
    )

    log.info("The clean up process might take a while")
    if config.get("processes_keep"):
        # Remove processes that are not key processes or their upstream processes
        json_ld = prune_to_supply_chain(json_ld, config["processes_keep"])
    if config.get("processes_remove"):
        # Remove processes and references to them in defaultProviders
        json_ld = remove_processes(json_ld, config["processes_remove"])
    if config.get("flows_remove"):
        # Remove exchanges from processes and data.flows
        json_ld = remove_flows(json_ld, config["flows_remove"])

    # Convert avoided products to negative technosphere flows
    json_ld = avoided_product_to_technosphere(json_ld)

    # Fix transport exchange formula
    json_ld = fix_transport_equation(json_ld)

    # fix parameter names - capitalization
    json_ld = rename_parameters(
        json_ld,
        {"fbf4145a-5f38-4b45-aa7c-ff4d5a44f95d": "Fugitive_CH4_diesel"},
    )

    # Start from a full copy of the source JSON-LD tree, then overwrite any
    # entity folders that exist in both the copy and the in-memory model.
    source_dir = source_data_path / base_source
    output_dir = Path(output_dir or source_data_path / method_name)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, output_dir)

    for folder in sorted(p for p in output_dir.iterdir() if p.is_dir()):
        entities = json_ld.data.get(folder.name)
        if not isinstance(entities, dict):
            continue
        shutil.rmtree(folder)
        folder.mkdir()
        for entity_id, entity in entities.items():
            # Drop bw2io's internal "filename" key before writing.
            entity = {k: v for k, v in entity.items() if k != "filename"}
            with (folder / f"{entity_id}.json").open("w", encoding="utf-8") as f:
                json.dump(entity, f, indent=2)

    log.info(
        f"Wrote {output_dir} with "
        f"{len(json_ld.data['processes'])} process(es) and "
        f"{len(json_ld.data['flows'])} flow(s)."
    )
    return output_dir


if __name__ == "__main__":
    from wmlci.extract.extract_source_data_from_script import (
        extract_source_data_from_script,
    )

    extract_source_data_from_script("waste_reduction_model_v16_pilot")
