"""
This script contains the methods required to import the swolfpy WTE data from the swolfpy_WTE_to_JSON.py and
integrate into the waste reduction model database.
"""
import json
import shutil
from copy import deepcopy
from pathlib import Path

from wmlci.jsonld_loader import load_JSONLD_sourceData
from wmlci.log import log
from wmlci.settings import source_data_path
# %%

## Methods

def replace_exchange_amounts(importer, flow_amounts):
    """
    Update exchange amounts for exchanges whose flow name matches
    a key in flow_amounts.
    """
    for process_id, process in importer.data["processes"].items():

        exchanges = process.get("exchanges", [])

        for exchange in exchanges:
            flow = exchange.get("flow", {})
            flow_name = flow.get("name")

            if flow_name in flow_amounts:
                exchange["amount"] = flow_amounts[flow_name]

    return importer

def edit_wte_electricity(importer):
    """
    Find the process named 'Mixed Plastic WTE' and modify the
    exchange named 'Electricity, AC, 120 V' such that:
    - isInput = True
    - amount is negative
    """
    target_process = "Mixed Plastic WTE"
    target_exchange = "Electricity, AC, 120 V"

    for process_id, process in importer.data["processes"].items():

        if process.get("name") != target_process:
            continue

        for exchange in process.get("exchanges", []):
            flow_name = exchange.get("flow", {}).get("name")

            if flow_name == target_exchange:
                # Ensure the exchange is an input
                exchange["isInput"] = True

                # Ensure amount is negative
                amount = float(exchange.get("amount", 0.0))
                exchange["amount"] = -abs(amount)

    return importer

def delete_processes_by_name(importer, process_names):
    """
    Remove processes whose name matches one of the supplied names.
    """
    process_names = set(process_names)

    processes_to_delete = [
        process_id
        for process_id, process in importer.data["processes"].items()
        if process.get("name") in process_names
    ]

    for process_id in processes_to_delete:
        del importer.data["processes"][process_id]

    return importer

def copy_exchanges_between_processes(
    warm_importer,
    swp_importer,
    origin_process,
    destination_process,
):
    """
    Copy all non-quantitative-reference exchanges from an origin process
    in swpImporter to a destination process in warmImporter. This is to
    update Combustion of Mixed Plastics with swolfpy data.
    """

    try:
        origin = swp_importer.data["processes"][origin_process]
    except KeyError:
        raise ValueError(f"Origin process UUID not found: {origin_process}")

    try:
        destination = warm_importer.data["processes"][destination_process]
    except KeyError:
        raise ValueError(f"Destination process UUID not found: {destination_process}")

    # Copy exchanges; embed flowType from catalog when stub omits it.
    # Drop SwolfPy PRODUCT/WASTE outputs so WARM qref stays the only production
    # exchange (edit_wte_electricity must run first to keep avoided electricity).
    swolfpy_flows = swp_importer.data.get("flows") or {}
    copied_exchanges = []
    for exc in origin.get("exchanges", []):
        exc = deepcopy(exc)
        fl = exc.get("flow")
        if isinstance(fl, dict):
            fid = fl.get("@id")
            if not fl.get("flowType") and fid and fid in swolfpy_flows:
                catalog_ft = swolfpy_flows[fid].get("flowType")
                if catalog_ft:
                    fl["flowType"] = catalog_ft
        ft = (exc.get("flow") or {}).get("flowType")
        is_input = bool(exc.get("isInput", exc.get("input", False)))
        if not is_input and ft in {"PRODUCT_FLOW", "WASTE_FLOW"}:
            continue
        copied_exchanges.append(exc)

    # Keep only quantitative reference exchanges in destination
    destination["exchanges"] = [
        exc
        for exc in destination.get("exchanges", [])
        if exc.get("isQuantitativeReference", False)
    ]

    # Add copied exchanges
    destination["exchanges"].extend(copied_exchanges)

    return warm_importer


def make_pilot_w_swolfpy(
    method_name=None,
    config=None,
    output_dir=None,
    bw_database_name="db",
):
    """
    Build pilot + SwolfPy JSON-LD for extract YAML
    waste_reduction_model_v16_pilot_w_swolfpy.
    """
    config = config or {}
    base_source = config.get("base_source") or "waste_reduction_model_v16_pilot"
    swolfpy_source = config.get("swolfpy_source") or "SwolfPy_WTE_PW_JSON"
    method_name = method_name or "waste_reduction_model_v16_pilot_w_swolfpy"
    output_dir = Path(output_dir or source_data_path / method_name)

    # Import WARM v16 pilot JSON-LD
    json_ld = load_JSONLD_sourceData(
        base_source, datatype="jsonld", bw_database_name=bw_database_name
    )

    # Load SwolfPy WTE JSON-LD (local folder or Data Commons zip)
    swolfpy = load_JSONLD_sourceData(
        swolfpy_source, datatype="jsonld", bw_database_name=bw_database_name
    )

    ## Run methods

    # Set bottom and fly ash exchange values to zero
    flows_to_zero = {'Bottom ash; unspecified origin':0,
                     'Fly ash, unspecified origin': 0}
    swolfpy = replace_exchange_amounts(swolfpy, flows_to_zero)

    # Convert electricity production from an output to negative input
    # This flow is not marked as an avoided product but should be treated
    ### new function added here since the existing avoided product function requires 'isAvoidedProduct' to be TRUE
    swolfpy = edit_wte_electricity(swolfpy)

    # Remove processes from the importer according to name
    ### Removing combustion of HDPE and PET
    ### Edits will be performed in Combustion of Mixed Plastics to retain the reference flow and process uuid
    remove_processes = ['MSW combustion of HDPE', 'MSW combustion of PET']
    json_ld = delete_processes_by_name(json_ld, remove_processes)

    # Update Combustion of Mixed Plastics data with swolfpy
    json_ld = copy_exchanges_between_processes(
        json_ld,
        swolfpy,
        '16c78919-ec73-3993-9c02-66a76ef78bf7',
        'e847ff05-48e3-4df0-ae4d-db2bafe56baf'
    )
    # %% confirm no flow collisions

    warm_ids = set(json_ld.data["flows"])
    swolf_ids = set(swolfpy.data["flows"])

    duplicates = warm_ids & swolf_ids

    print(f"{len(duplicates)} duplicate flow UUIDs")

    # %% merge flow dictionaries

    json_ld.data["flows"].update({
        k: v
        for k, v in swolfpy.data["flows"].items()
        if k not in json_ld.data["flows"]
    })

    # %% combine parameters, process specific 

    swolf_uuid = "16c78919-ec73-3993-9c02-66a76ef78bf7"
    warm_uuid = "e847ff05-48e3-4df0-ae4d-db2bafe56baf"

    swolf_proc = swolfpy.data["processes"][swolf_uuid]
    warm_proc = json_ld.data["processes"][warm_uuid]

    swolf_params = swolf_proc.get("parameters", [])
    warm_params = warm_proc.get("parameters", [])

    print("SwolfPy params:", len(swolf_params))
    print("WARM params before:", len(warm_params))

    warm_proc["parameters"] = warm_params + swolf_params

    json_ld.data["processes"][warm_uuid] = warm_proc

    print("WARM params after:", len(json_ld.data["processes"][warm_uuid].get("parameters", [])))

    # %% write JSON-LD folder (inventory_source for wmlci_pilot_w_air_emissions)

    source_dir = source_data_path / base_source
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
    from wmlci.extract.extract_common import extract_source_data

    extract_source_data("waste_reduction_model_v16_pilot_w_swolfpy")
