"""
This script contains the methods required to import the swolfpy WTE data from the swolfpy_WTE_to_JSON.py and
integrate into the waste reduction model database.
"""
from bw2io.importers.json_ld import JSONLDImporter

from wmlci.jsonld_loader import load_JSONLD_sourceData

from pathlib import Path

import zipfile

from copy import deepcopy

from collections import deque

from wmlci.log import log
# %%


# Import WARM v16 JSON file
json_ld = load_JSONLD_sourceData('waste_reduction_model_v16', datatype="jsonld", bw_database_name='db')

# swolfpy data paths
PATH_PROJECT = Path.cwd()
swolfpy_path = PATH_PROJECT / "data/source_data/swolfpy/SwolfPy_WTE_PW_JSON.zip"
unzip_to_folder = (
    PATH_PROJECT
    / "wmlci/data/source_data/swolfpy"
    / "SwolfPy_WTE_PW_JSON_olca2.0_20260717-114500"
)

# %%


# Make folder with same name as zip file to unzip to
unzip_to_folder.mkdir(parents=True, exist_ok=True)
# unzip file
with zipfile.ZipFile(swolfpy_path, "r") as zip_ref:
    zip_ref.extractall(unzip_to_folder)

# Extract swolfpy data into JSONLDImporter
swolfpy_path = unzip_to_folder
swolfpy = JSONLDImporter(swolfpy_path, 'db')

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

    # Copy all non-quantitative-reference exchanges
    copied_exchanges = [
        deepcopy(exc)
        for exc in origin.get("exchanges", [])
    ]

    # Keep only quantitative reference exchanges in destination
    destination["exchanges"] = [
        exc
        for exc in destination.get("exchanges", [])
        if exc.get("isQuantitativeReference", False)
    ]

    # Add copied exchanges
    destination["exchanges"].extend(copied_exchanges)

    return warm_importer

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


# %%write zip 

import json
import zipfile
from pathlib import Path

root = Path("olca_export")
root.mkdir(exist_ok=True)

for entity_type, entities in json_ld.data.items():

    folder = root / entity_type
    folder.mkdir(exist_ok=True)

    for obj in entities.values():

        filename = Path(obj["filename"]).name

        target = folder / filename

        with open(target, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

n = len(list(root.rglob("*.json")))
print(f"Wrote {n} json files")

with zipfile.ZipFile("olca_export.zip", "w", zipfile.ZIP_DEFLATED) as zf:
    for file in root.rglob("*.json"):
        zf.write(file, file.relative_to(root))

print("Created olca_export.zip")