"""
LCA calculation helpers for Waste Reduction Model openLCA inventories.

documentation
https://docs.brightway.dev/en/latest/content/api/bw2io/importers/json_ld_lcia/index.html#bw2io.importers.json_ld_lcia.JSONLDLCIAImporter
bw25 tutorial https://learn.brightway.dev/en/latest/content/chapters/BW25/BW25_introduction.html
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import bw2calc as bc
import bw2data as bd
import numpy as np
import pandas as pd
from bw2calc import LCA
from scipy import sparse as sp

from wmlci.log import log
from wmlci.settings import resultspath, versioned_filename

_UNIT_LABEL = {
    "kilogram": "kg",
    "short ton": "short ton",
    "megajoule": "MJ",
    "ton kilometer": "ton km",
    "cubic meter": "m3",
}

DETAIL_COLUMNS = [
    "location",
    "process",
    "activity",
    "reference_product",
    "functional_unit",
    "product_amount",
    "product_amount_unit",
    "impact_per_unit_of_product",
    "impact_per_unit_of_product_unit",
    "FlowAmount",
    "FlowAmount_unit",
    "method",
]

CHARACTERIZED_COLUMNS = [
    "process",
    "functional_unit",
    "elementary_flow",
    "inventory_amount",
    "inventory_unit",
    "characterized_amount",
    "characterized_unit",
    "method",
]


def lcia_unit(config: dict[str, Any]) -> str:
    """Impact unit from method config (e.g. kg CO2e, kg SO2e, m3)."""
    return str(config.get("lcia_unit") or "").strip()



def return_process_product(db):
    """
    Return (process, product) pairs for processes with a single reference
    product. Use the product as the functional-unit key for LCA calculations.
    """
    pairs = []
    for act in db:
        if act.get("type") != "process":
            continue
        production_exchanges = list(act.production())
        if len(production_exchanges) != 1:
            continue
        product = production_exchanges[0].input
        if product.get("type") == "product":
            pairs.append((act, product))
    return pairs


def return_foreground_processes(db):
    """
    Return foreground process/product pairs for LCA scenarios.

    Foreground processes have a reference product that is not consumed elsewhere
    in the technosphere.
    """
    consumed = set()
    for act in db:
        if act.get("type") != "process":
            continue
        for exc in act.technosphere():
            consumed.add(exc.input.id)

    fg = []
    for act, product in return_process_product(db):
        if product.id not in consumed:
            fg.append((act, product))
    return fg


def resolve_processes(db, config: dict[str, Any]):
    """
    Match configured process names to database activities.

    Per-process settings are expanded at config load time (``load_method_config``).
    """
    processes_cfg = config.get("processes") or {}
    if not processes_cfg:
        raise ValueError("config must include processes")

    by_name = {act["name"]: (act, prod) for act, prod in return_foreground_processes(db)}
    resolved = []
    missing = []

    for name, settings in processes_cfg.items():
        if name not in by_name:
            missing.append(name)
            continue
        act, prod = by_name[name]
        resolved.append((act, prod, deepcopy(settings)))

    if missing:
        available = sorted(by_name.keys())
        raise ValueError(
            f"Processes not found in database: {missing}. "
            f"Available foreground processes: {available}"
        )
    return resolved


def functional_unit_label(reference_product, fu_config):
    """Human-readable functional unit string for CSV output."""
    amount = fu_config["amount"]
    unit = _UNIT_LABEL.get(fu_config.get("unit", ""), fu_config.get("unit", ""))
    product_label = reference_product.replace(",", "").strip().lower()
    if amount == 1:
        return f"1 {unit} {product_label}"
    return f"{amount} {unit} {product_label}"


def build_process_meta(db) -> dict:
    """Reference-product metadata for each process (units, production amount)."""
    process_meta = {}
    for proc, prod in return_process_product(db):
        production_exchanges = list(proc.production())
        prod_exc = production_exchanges[0] if production_exchanges else {}
        process_meta[proc.id] = {
            "reference_product": prod.get("name", ""),
            "location": proc.get("location", ""),
            "supply_unit": prod_exc.get("unit", ""),
            "production_amount": prod_exc.get("amount") or 1,
        }
    return process_meta


def calculate_lca_results(db, processes, config: dict[str, Any]):
    """Run LCA for each configured process scenario; return summary, detail, and flow dfs."""
    method = tuple(config["lcia_method"])
    unit = lcia_unit(config)
    process_meta = build_process_meta(db)

    results = []        # one row per scenario (summary)
    detail_rows = []    # one row per activity within each scenario (detailed)
    flow_rows = []      # one row per elementary flow within each scenario

    for activity, product, process_settings in processes:
        # Functional unit: demand passed to Brightway in reference-product units (kg).
        # Default in v16.yaml is SHORT_TON_KG (~907.18 kg) per short ton.
        demand = float(process_settings["functional_unit"]["amount"])

        try:
            func_unt, data_objs, _ = bd.prepare_lca_inputs(
                {product: demand}, method=method
            )
            lca = LCA(func_unt, data_objs=data_objs)
            lca.lci()   # life cycle inventory: solves A^-1 f
            lca.lcia()  # life cycle impact assessment: C B A^-1 f
        except (ValueError, bc.errors.OutsideTechnosphere) as err:
            log.warning(f"Skipping scenario '{activity['name']}': {err}")
            continue

        fu_config = process_settings["functional_unit"]
        fu_label = functional_unit_label(product.get("name", ""), fu_config)

        score = lca.score

        results.append({
            "process": activity["name"],
            "reference_product": product.get("name", ""),
            "functional_unit": fu_label,
            "location": activity.get("location", ""),
            "method": str(method),
            "score": score,
            "score_unit": unit,
        })

        # decompose the system score by process: characterized_inventory column sums
        # give each process's contribution to the system total, and supply_array
        # gives how much of each process the system uses. Both are indexed by the
        # technosphere columns (processes).
        ci = lca.characterized_inventory  # biosphere flows x processes
        col_contributions = np.asarray(ci.sum(axis=0)).ravel()
        supply = np.asarray(lca.supply_array).ravel()
        # drop the noise - the values that round to 0 and exist due to sparse matrix after run
        for idx in np.argsort(np.abs(col_contributions))[::-1]:
            direct_contribution = float(col_contributions[idx])
            if abs(direct_contribution) < 1e-9:
                continue
            proc = bd.get_activity(lca.dicts.activity.reversed[idx])
            meta = process_meta.get(proc.id, {})

            # scale process supply to physical reference-product amount per
            # functional unit (e.g. 1 kg food waste landfilled)
            process_supply = supply[idx]
            production_amount = meta.get("production_amount") or 1
            product_amount = process_supply * production_amount
            product_unit = meta.get("supply_unit", "")
            impact_per_unit = (
                direct_contribution / product_amount if product_amount else None
            )

            detail_rows.append({
                "location": meta.get("location", proc.get("location", "")),
                "process": activity["name"],
                "activity": proc["name"],
                "reference_product": meta.get("reference_product", ""),
                "functional_unit": fu_label,
                "product_amount": product_amount,
                "product_amount_unit": product_unit,
                "impact_per_unit_of_product": impact_per_unit,
                "impact_per_unit_of_product_unit": (
                    f"{unit} / {product_unit}" if product_unit else unit
                ),
                "FlowAmount": direct_contribution,
                "FlowAmount_unit": unit,
                "method": str(method),
            })

        # Scenario totals by elementary flow (any impact method: GHG, air, water, …).
        # Sum inventory and characterized inventory across all activities.
        inv = lca.inventory  # biosphere flows x processes
        inv_sp = inv if sp.issparse(inv) else sp.csr_matrix(np.asarray(inv))
        ci_sp = ci if sp.issparse(ci) else sp.csr_matrix(np.asarray(ci))
        inv_by_flow = np.asarray(inv_sp.sum(axis=1)).ravel()
        char_by_flow = np.asarray(ci_sp.sum(axis=1)).ravel()

        for flow_idx, flow_id in lca.dicts.biosphere.reversed.items():
            char_amt = float(char_by_flow[flow_idx])
            if abs(char_amt) < 1e-12:
                continue
            flow = bd.get_activity(flow_id)
            flow_rows.append({
                "process": activity["name"],
                "functional_unit": fu_label,
                "elementary_flow": flow.get("name", ""),
                "inventory_amount": float(inv_by_flow[flow_idx]),
                "inventory_unit": flow.get("unit") or "kilogram",
                "characterized_amount": char_amt,
                "characterized_unit": unit,
                "method": str(method),
            })

    return (
        pd.DataFrame(results),
        pd.DataFrame(detail_rows, columns=DETAIL_COLUMNS),
        pd.DataFrame(flow_rows, columns=CHARACTERIZED_COLUMNS),
    )


def write_lca_outputs(
    results_df,
    detail_df,
    characterized_df,
    config: dict[str, Any],
) -> dict[str, str]:
    """Write summary, detail, and characterized-inventory CSVs; return paths."""
    out = config.get("output_files", {})
    summary_name = versioned_filename(
        out.get("summary_csv", "lcia_results_summary.csv")
    )
    detail_name = versioned_filename(
        out.get("detail_csv", "lcia_results_detail.csv")
    )
    char_name = versioned_filename(
        out.get(
            "characterized_inventory_csv",
            "lcia_results_characterized_inventory.csv",
        )
    )

    results_path = resultspath / summary_name
    detail_path = resultspath / detail_name
    char_path = resultspath / char_name

    results_df.to_csv(results_path, index=False)
    detail_df.to_csv(detail_path, index=False)
    characterized_df.to_csv(char_path, index=False)

    log.info(
        f"Summary for {len(results_df)} scenarios written to {results_path}"
    )
    log.info(
        f"Detailed results ({len(detail_df)} process rows across "
        f"{detail_df['process'].nunique() if len(detail_df) else 0} processes) "
        f"written to {detail_path}"
    )
    log.info(
        f"Characterized inventory ({len(characterized_df)} scenario-flow "
        f"rows) written to {char_path}"
    )

    return {
        "summary": str(results_path),
        "detail": str(detail_path),
        "characterized_inventory": str(char_path),
    }