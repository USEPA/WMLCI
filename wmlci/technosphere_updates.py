"""
Replace input providers using YAML update files.

Reads YAML under technosphere_updates/, loads named processes from the
configured data sources, copies them into the model, and rewires matching
inputs.
"""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from wmlci.editImporter import _exchange_is_input, recalculate_amounts_from_formulas
from wmlci.jsonld_loader import clean_JSONLD_background_data, load_JSONLD_sourceData
from wmlci.log import log

updates_dir = Path(__file__).resolve().parent / "technosphere_updates"

# Cleared at the start of each replace_input_providers call.
# Cleaned source JSON-LD (after keeping only needed processes; before formula recalc).
_CLEANED_SOURCE_CACHE = {}
# Processes keyed by name, after formula amounts are recalculated.
_PROCESSES_BY_NAME_CACHE = {}


######################################################
### Load YAML and source process lists             ###
######################################################

def clear_loaded_source_cache():
    """Clear in-memory source caches so each method run reloads current data."""
    _CLEANED_SOURCE_CACHE.clear()
    _PROCESSES_BY_NAME_CACHE.clear()
    log.info("Cleared method-run source caches")


def load_provider_update_yaml(name: str):
    """Load wmlci/technosphere_updates/{name}.yaml."""
    path = updates_dir / f"{name}.yaml"
    if not path.exists():
        log.warning(f"Technosphere updates file not found: {path}")
        return {}
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not data.get("technosphere_exchanges") and not data.get(
        "technosphere_exchange_drops"
    ):
        log.warning(f"Technosphere updates file is empty: {path}")
    return data


def _replacement_process_names_by_source(method_processes):
    """Map (data_source, data_version) -> set of pinned replacement process names."""
    roots = defaultdict(set)
    for settings in method_processes.values():
        updates_name = settings.get("technosphere_updates")
        if not updates_name:
            continue
        spec = load_provider_update_yaml(updates_name)
        for replacement in (spec.get("technosphere_exchanges") or {}).values():
            data_source = replacement.get("data_source")
            data_version = replacement.get("data_version")
            process_name = replacement.get("process")
            if data_source and data_version and process_name:
                roots[(data_source, data_version)].add(process_name)
    return roots


def _reference_product_output(process, product_name=None):
    """Return the reference-product output exchange for a process."""
    matches = []
    for exc in process.get("exchanges", []):
        if _exchange_is_input(exc):
            continue
        flow = exc.get("flow", {})
        if flow.get("flowType") != "PRODUCT_FLOW":
            continue
        if product_name is None or flow.get("name") == product_name:
            matches.append(exc)
    if not matches:
        return None
    return matches[0]


def _subset_processes_providers(jsonld, root_process_names):
    """
    Subset to roots and upstream defaultProvider

    Drop unused processes/flows/locations
    """
    processes = jsonld.data.get("processes", {})
    n_before = len(processes)
    keep_ids = set()
    missing = []
    for name in root_process_names:
        pid = _process_id_for_name(processes, name)
        if pid is None:
            missing.append(name)
            continue
        keep_ids |= _upstream_provider_ids(pid, processes)
    if missing:
        log.warning(f"Replacement process roots not found: {missing}")

    for pid in list(processes.keys()):
        if pid not in keep_ids:
            del processes[pid]

    keep_flow_ids = set()
    keep_location_ids = set()
    for process in processes.values():
        loc = process.get("location")
        if isinstance(loc, dict) and loc.get("@id"):
            keep_location_ids.add(loc["@id"])
        for exc in process.get("exchanges") or []:
            flow = exc.get("flow") or {}
            if flow.get("@id"):
                keep_flow_ids.add(flow["@id"])
            eloc = exc.get("location")
            if isinstance(eloc, dict) and eloc.get("@id"):
                keep_location_ids.add(eloc["@id"])

    flows = jsonld.data.get("flows", {})
    for flow_id in list(flows.keys()):
        if flow_id not in keep_flow_ids:
            del flows[flow_id]

    locations = jsonld.data.get("locations", {})
    for location_id in list(locations.keys()):
        if location_id not in keep_location_ids:
            del locations[location_id]

    log.info(
        f"Drop unused processes; source reduced to {len(processes)} processes "
        f"(originally {n_before}) for {sorted(root_process_names)}"
    )
    return jsonld


def _load_and_clean_source_jsonld(data_source, data_version, root_names=()):
    """Load source JSON-LD, keep only named roots and their providers, and clean it."""
    roots_key = frozenset(root_names)
    cache_key = (data_source, data_version, roots_key)
    if cache_key in _CLEANED_SOURCE_CACHE:
        log.info(
            f"Using method-run source cache for {data_source}@{data_version} "
            f"({len(roots_key)} root(s))"
        )
        return _CLEANED_SOURCE_CACHE[cache_key]

    log.info(
        f"Loading and cleaning source for {data_source}@{data_version} "
        f"({len(roots_key)} root(s))"
    )
    source = load_JSONLD_sourceData(
        data_source,
        datatype="jsonld",
        bw_database_name=f"_index_{data_source}",
        data_version=data_version,
    )
    if root_names:
        _subset_processes_providers(source, root_names)
    source = clean_JSONLD_background_data(source)
    _CLEANED_SOURCE_CACHE[cache_key] = source
    return source


def load_processes_by_name(
    data_source,
    data_version,
    config: dict[str, Any] | None = None,
    root_names=(),
):
    """
    Load and clean source JSON-LD, recalculate formulas, and return processes
    keyed by name
    """
    roots_key = frozenset(root_names)
    # Stable cache key for parameter overrides that affect formula amounts.
    if config:
        formula_key = (
            tuple(sorted((config.get("global_parameter_overrides") or {}).items())),
            tuple(
                (name, tuple(sorted((params or {}).items())))
                for name, params in sorted(
                    (config.get("process_parameter_overrides") or {}).items()
                )
            ),
        )
    else:
        formula_key = ()
    cache_key = (data_source, data_version, roots_key, formula_key)
    if cache_key in _PROCESSES_BY_NAME_CACHE:
        log.info(
            f"Using data cache for {data_source}@{data_version}"
        )
        return _PROCESSES_BY_NAME_CACHE[cache_key]

    source = deepcopy(
        _load_and_clean_source_jsonld(data_source, data_version, root_names=root_names)
    )
    # Source amountFormulas may reference the same global defaults / overrides.
    source = recalculate_amounts_from_formulas(source, config or {})

    processes = source.data.get("processes", {})
    # Only pin replacements need name lookup; upstream is reached by @id.
    names_to_index = roots_key or {
        p.get("name") for p in processes.values() if p.get("name")
    }
    by_name = {}
    for process_id, process in processes.items():
        name = process.get("name")
        if not name or name not in names_to_index:
            continue
        by_name[name] = {
            "process_id": process_id,
            "process_name": name,
            "production": _reference_product_output(process),
        }

    index = {"source": source, "by_name": by_name}
    _PROCESSES_BY_NAME_CACHE[cache_key] = index
    return index


######################################################
### Copy replacement processes into the model      ###
######################################################

def _provider_ids_still_in_use(processes):
    refs = set()
    for process in processes.values():
        for exc in process.get("exchanges", []):
            if _exchange_is_input(exc) and exc.get("defaultProvider"):
                refs.add(exc["defaultProvider"]["@id"])
    return refs


def _upstream_provider_ids(process_id, processes):
    subtree = set()
    stack = [process_id]
    while stack:
        pid = stack.pop()
        if pid in subtree:
            continue
        subtree.add(pid)
        process = processes.get(pid, {})
        for exc in process.get("exchanges", []):
            if _exchange_is_input(exc) and exc.get("defaultProvider"):
                stack.append(exc["defaultProvider"]["@id"])
    return subtree


def _drop_unused_upstream_providers(processes, provider_id):
    """Delete provider_id's upstream subtree when no remaining exchange references it."""
    if not provider_id:
        return
    subtree = _upstream_provider_ids(provider_id, processes)
    refs = _provider_ids_still_in_use(processes)
    for pid in subtree:
        if pid not in refs and pid in processes:
            del processes[pid]


def _copy_unit_groups(target_data, source_data, unit_id=None):
    """
    Merge unit_groups (and units) from source_data into target_data.

    If unit_id is set, only merge the group that contains that unit.
    Brightway json_ld_convert_unit_to_reference_unit needs every exchange
    unit @id present in unit_groups.
    """
    target_ugs = target_data.setdefault("unit_groups", {})
    n = 0
    for ug_id, ug in source_data.get("unit_groups", {}).items():
        units = ug.get("units") or []
        if unit_id is not None:
            source_unit = next(
                (
                    u
                    for u in units
                    if isinstance(u, dict) and u.get("@id") == unit_id
                ),
                None,
            )
            if source_unit is None:
                continue
            if ug_id not in target_ugs:
                target_ugs[ug_id] = deepcopy(ug)
                n += 1
            else:
                existing_ids = {
                    u.get("@id")
                    for u in (target_ugs[ug_id].get("units") or [])
                    if isinstance(u, dict)
                }
                if unit_id not in existing_ids:
                    target_ugs[ug_id].setdefault("units", []).append(
                        deepcopy(source_unit)
                    )
                    n += 1
            continue

        if ug_id not in target_ugs:
            target_ugs[ug_id] = deepcopy(ug)
            n += 1
            continue
        existing_ids = {
            u.get("@id")
            for u in (target_ugs[ug_id].get("units") or [])
            if isinstance(u, dict)
        }
        for u in units:
            if isinstance(u, dict) and u.get("@id") not in existing_ids:
                target_ugs[ug_id].setdefault("units", []).append(deepcopy(u))
                existing_ids.add(u["@id"])
                n += 1
    return n


def _copy_flow_location_and_unit_for_exchange(target_data, source_data, exchange, copied_flow_ids=None):
    """Copy an exchange's flow, location, and unit group into target_data."""
    flow = exchange.get("flow", {})
    if isinstance(flow, dict) and flow.get("@id"):
        flow_id = flow["@id"]
        target_flows = target_data.setdefault("flows", {})
        if flow_id not in target_flows:
            source_flow = source_data.get("flows", {}).get(flow_id)
            if source_flow:
                target_flows[flow_id] = deepcopy(source_flow)
        if flow_id in target_flows and copied_flow_ids is not None:
            copied_flow_ids.add(flow_id)

    location = exchange.get("location")
    if isinstance(location, dict) and location.get("@id"):
        location_id = location["@id"]
        locations = target_data.setdefault("locations", {})
        if location_id not in locations:
            source_location = source_data.get("locations", {}).get(location_id)
            if source_location:
                locations[location_id] = deepcopy(source_location)

    unit = exchange.get("unit")
    unit_id = unit.get("@id") if isinstance(unit, dict) else None
    if unit_id:
        _copy_unit_groups(target_data, source_data, unit_id=unit_id)


def copy_process_and_its_providers(target_importer, source_importer, process_id):
    """Copy a process and every process it uses as a defaultProvider."""
    target_data = target_importer.data
    source_data = source_importer.data
    source_processes = source_data.get("processes", {})
    target_processes = target_data.setdefault("processes", {})
    copied_flow_ids = set()
    merged = 0
    skipped = 0

    def copy_process(pid):
        nonlocal merged, skipped
        if pid in target_processes:
            skipped += 1
            return
        process = source_processes.get(pid)
        if not process:
            return

        target_processes[pid] = deepcopy(process)
        merged += 1

        loc = process.get("location")
        if isinstance(loc, dict) and loc.get("@id"):
            location_id = loc["@id"]
            locations = target_data.setdefault("locations", {})
            if location_id not in locations:
                source_location = source_data.get("locations", {}).get(location_id)
                if source_location:
                    locations[location_id] = deepcopy(source_location)

        for exc in process.get("exchanges", []):
            _copy_flow_location_and_unit_for_exchange(
                target_data, source_data, exc, copied_flow_ids
            )
            if _exchange_is_input(exc) and exc.get("defaultProvider"):
                copy_process(exc["defaultProvider"]["@id"])

    copy_process(process_id)

    copied_ugs = _copy_unit_groups(target_data, source_data)
    if copied_ugs:
        log.info(f"Copied/merged {copied_ugs} unit group(s)/unit(s) from merged source")
    if skipped:
        log.info(
            f"Skipped {skipped} process node(s) already present while merging subtree"
        )
    if copied_flow_ids:
        add_flows_to_brightway_lists(target_importer, copied_flow_ids)
    return merged


######################################################
### Remove or replace input providers              ###
######################################################

def _process_id_for_name(processes, name):
    for process_id, process in processes.items():
        if process.get("name") == name:
            return process_id
    return None


def _input_exchange_with_provider_name(process, provider_name):
    for index, exc in enumerate(process.get("exchanges", [])):
        if not _exchange_is_input(exc):
            continue
        provider = exc.get("defaultProvider") or {}
        if provider.get("name") == provider_name:
            return index, exc
    return None, None


def _lookup_replacement(index, spec):
    process_name = spec.get("process")
    product_name = spec.get("product")
    if not process_name:
        log.warning("Replacement spec missing process name")
        return None

    entry = index["by_name"].get(process_name)
    if not entry:
        # Roots only are indexed; a miss usually means a bad YAML process name.
        available = sorted(index["by_name"])[:5]
        log.warning(
            f"Replacement process not found in {spec.get('data_source')} "
            f"@{spec.get('data_version')}: {process_name}"
            + (f" (indexed roots sample: {available})" if available else "")
        )
        return None

    process = index["source"].data["processes"][entry["process_id"]]
    production = _reference_product_output(process, product_name)
    if production is None:
        log.warning(
            f"Production exchange not found for process '{process_name}' "
            f"product '{product_name}'"
        )
        return None

    amount = production.get("amount")
    unit = production.get("unit")
    if amount is None or unit is None:
        log.warning(
            f"Production exchange amount/unit missing for process '{process_name}'"
        )
        return None

    return {
        "process_id": entry["process_id"],
        "process_name": process_name,
        "production": production,
        "amount": amount,
        "unit": unit,
    }


def remove_inputs_from_named_provider(importer, foreground_process_name, provider_name):
    """Remove inputs that point at provider_name under the named process"""
    processes = importer.data.get("processes", {})
    fg_id = _process_id_for_name(processes, foreground_process_name)
    if fg_id is None:
        log.warning(f"Foreground process not found: {foreground_process_name}")
        return 0

    dropped = 0
    for process_id in _upstream_provider_ids(fg_id, processes):
        process = processes.get(process_id, {})
        process_name = process.get("name", process_id)
        while True:
            exc_index, exchange = _input_exchange_with_provider_name(
                process, provider_name
            )
            if exchange is None:
                break

            old_provider_id = exchange.get("defaultProvider", {}).get("@id")
            process["exchanges"].pop(exc_index)
            dropped += 1
            log.info(
                f"Dropped technosphere exchange on '{process_name}' "
                f"(under '{foreground_process_name}'): {provider_name}"
            )

            _drop_unused_upstream_providers(processes, old_provider_id)

    if dropped == 0:
        log.warning(
            f"Consumption edge not found under '{foreground_process_name}' "
            f"for provider '{provider_name}'"
        )
    return dropped


def replace_input_provider(
    importer,
    foreground_process_name,
    provider_name,
    spec: dict[str, Any],
    config: dict[str, Any] | None = None,
    root_names=(),
):
    """Replace inputs that use provider_name with the process named in spec."""
    data_source = spec.get("data_source")
    data_version = spec.get("data_version")
    if not data_source or not data_version:
        log.warning(
            f"Replacement for '{provider_name}' missing data_source or data_version"
        )
        return 0

    processes = importer.data.get("processes", {})
    fg_id = _process_id_for_name(processes, foreground_process_name)
    if fg_id is None:
        log.warning(f"Foreground process not found: {foreground_process_name}")
        return 0

    index = load_processes_by_name(
        data_source, data_version, config=config, root_names=root_names
    )
    replacement = _lookup_replacement(index, spec)
    if replacement is None:
        return 0

    replaced = 0
    for process_id in _upstream_provider_ids(fg_id, processes):
        process = processes.get(process_id, {})
        process_name = process.get("name", process_id)
        exc_index, exchange = _input_exchange_with_provider_name(
            process, provider_name
        )
        if exchange is None:
            continue

        old_provider_id = exchange.get("defaultProvider", {}).get("@id")
        old_amount = exchange.get("amount")
        old_unit = exchange.get("unit")

        merged = copy_process_and_its_providers(
            importer, index["source"], replacement["process_id"]
        )
        if merged:
            log.info(
                f"Merged {merged} process(es) from {data_source}@{data_version} "
                f"for '{replacement['process_name']}'"
            )

        production = replacement["production"]
        flow = deepcopy(production.get("flow", {}))
        exchange["flow"] = flow
        # Keep existing demand amount; only swap provider and product flow.
        # category + flowType are required non-empty strings for the provider
        # metadata validator; source them from the provider process and flow.
        provider_process = (
            index["source"].data.get("processes", {}).get(replacement["process_id"], {})
        )
        exchange["defaultProvider"] = {
            "@id": replacement["process_id"],
            "@type": "Process",
            "name": replacement["process_name"],
            "category": provider_process.get("category") or flow.get("category") or "",
            "flowType": flow.get("flowType") or "PRODUCT_FLOW",
        }

        if old_provider_id:
            _drop_unused_upstream_providers(processes, old_provider_id)

        replaced += 1
        log.info(
            f"Replaced technosphere exchange on '{process_name}' "
            f"(under '{foreground_process_name}'): "
            f"{provider_name} -> {replacement['process_name']} / "
            f"{flow.get('name')} "
            f"({old_amount} {old_unit} -> {replacement['amount']} "
            f"{replacement['unit']}) [{data_source}@{data_version}]"
        )

    if replaced == 0:
        log.warning(
            f"Consumption edge not found under '{foreground_process_name}' "
            f"for provider '{provider_name}'"
        )
    return replaced


def update_technosphere_flows(
    importer,
    processes: dict[str, Any],
    config: dict[str, Any] | None = None,
):
    """
    For each method process with a technosphere_updates YAML, remove listed
    providers and replace with processes from identified new data sources
    """
    clear_loaded_source_cache()
    replacement_roots = _replacement_process_names_by_source(processes)

    # Prepare each data source once for the union of all replacement roots in this method.
    for (data_source, data_version), roots in sorted(replacement_roots.items()):
        root_list = sorted(roots)
        load_processes_by_name(
            data_source, data_version, config=config, root_names=roots
        )
        log.info(
            f"Prepared source {data_source}@{data_version} with "
            f"{len(root_list)} root(s): {root_list}"
        )

    total_replaced = 0
    total_dropped = 0
    total_skipped = 0

    for foreground_name, settings in processes.items():
        updates_name = settings.get("technosphere_updates")
        if not updates_name:
            continue

        spec = load_provider_update_yaml(updates_name)
        if not spec:
            total_skipped += 1
            continue

        file_replaced = 0
        file_dropped = 0
        file_skipped = 0

        for provider_name in spec.get("technosphere_exchange_drops") or []:
            dropped = remove_inputs_from_named_provider(
                importer, foreground_name, provider_name
            )
            if dropped:
                file_dropped += dropped
            else:
                file_skipped += 1

        for provider_name, replacement in (
            spec.get("technosphere_exchanges") or {}
        ).items():
            root_names = replacement_roots.get(
                (replacement.get("data_source"), replacement.get("data_version")),
                set(),
            )
            replaced = replace_input_provider(
                importer,
                foreground_name,
                provider_name,
                replacement,
                config=config,
                root_names=root_names,
            )
            if replaced:
                file_replaced += replaced
            else:
                file_skipped += 1

        log.info(
            f"Technosphere updates '{updates_name}' on '{foreground_name}': "
            f"{file_replaced} replaced, {file_dropped} dropped, "
            f"{file_skipped} skipped"
        )
        total_replaced += file_replaced
        total_dropped += file_dropped
        total_skipped += file_skipped

    log.info(
        f"Technosphere updates complete: {total_replaced} replaced, "
        f"{total_dropped} dropped, {total_skipped} skipped"
    )

    if total_replaced:
        # Cut off consumed PRODUCT_FLOW inputs that no merged process produces.
        # These "cutoff" flows add product rows without a
        # producing column, resulting in a non-square technosphere matrix.
        processes = importer.data.get("processes", {})
        produced = set()
        for process in processes.values():
            for exc in process.get("exchanges") or []:
                flow = exc.get("flow") or {}
                flow_id = flow.get("@id")
                if not flow_id:
                    continue
                flow_type = flow.get("flowType")
                if _exchange_is_input(exc):
                    # opposite-direction waste treatment: waste input is the ref
                    if flow_type == "WASTE_FLOW":
                        produced.add(flow_id)
                elif not exc.get("avoidedProduct") and flow_type in {
                    "PRODUCT_FLOW",
                    "WASTE_FLOW",
                }:
                    produced.add(flow_id)

        cutoff_count = 0
        for process in processes.values():
            exchanges = process.get("exchanges") or []
            kept = []
            for exc in exchanges:
                flow = exc.get("flow") or {}
                if (
                    _exchange_is_input(exc)
                    and not exc.get("avoidedProduct")
                    and flow.get("flowType") == "PRODUCT_FLOW"
                    and flow.get("@id") not in produced
                ):
                    cutoff_count += 1
                    continue
                kept.append(exc)
            if len(kept) != len(exchanges):
                process["exchanges"] = kept

        log.info(f"Cut off {cutoff_count} unproduced technosphere input edges.")

    return importer


######################################################
### Register new flows with Brightway              ###
######################################################

def add_flows_to_brightway_lists(jsonld, flow_ids):
    """
    Add newly copied flows to the importer's product and biosphere lists.

    Those lists are built when the base inventory is first loaded; flows
    brought in later from replacement sources added here.
    """
    flows = jsonld.data.get("flows", {})
    existing_product_codes = {p["code"] for p in jsonld.products}
    existing_bio_codes = {b["code"] for b in jsonld.biosphere_database}
    added_products = 0
    added_biosphere = 0

    for flow_id in flow_ids:
        obj = flows.get(flow_id)
        if not obj:
            continue
        flow_type = obj.get("flowType")
        if flow_type in {"PRODUCT_FLOW", "WASTE_FLOW"} and flow_id not in existing_product_codes:
            jsonld.products.append({
                "code": obj["@id"],
                "name": obj["name"],
                "categories": obj.get("category", "Unknown").split("/"),
                "location": obj["location"]["name"] if "location" in obj else None,
                "exchanges": [],
                "unit": "",
                "type": "product",
            })
            existing_product_codes.add(flow_id)
            added_products += 1
        elif flow_type == "ELEMENTARY_FLOW" and flow_id not in existing_bio_codes:
            jsonld.biosphere_database.append({
                "code": obj["@id"],
                "name": obj["name"],
                "categories": obj.get("category", "Unknown").split("/"),
                "CAS number": obj.get("cas"),
                "database": jsonld.db_name + " biosphere",
                "exchanges": [],
                "unit": "",
                "type": "emission",
            })
            existing_bio_codes.add(flow_id)
            added_biosphere += 1

    if added_products or added_biosphere:
        log.info(
            f"Extended importer catalogs: {added_products} product(s), "
            f"{added_biosphere} biosphere flow(s)"
        )
