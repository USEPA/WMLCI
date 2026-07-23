"""
Run Brightway LCA using method YAML configurations.
"""

from __future__ import annotations

from typing import Any

import bw2data as bd

from wmlci.disaggregation import split_multi_product_processes
from wmlci.editImporter import (
    convert_lcia_param_list_to_dict,
    correct_jsonld_input_key,
    map_lcia_to_fedelemflowlist_UUIDs,
)
from wmlci.errorLogging import check_for_errors_in_jsonld_import
from wmlci.jsonld_loader import clean_JSONLD_sourceData, load_JSONLD_sourceData
from wmlci.log import log
from wmlci.method_config import load_method_config
from wmlci.openlca import (
    calculate_lca_results,
    functional_unit_label,
    resolve_processes,
    write_lca_outputs,
)
from wmlci.technosphere_updates import update_technosphere_flows


def run_bw_lca(method_name: str) -> dict[str, Any]:
    """
    Run a full Brightway LCA workflow from a method YAML config.

    Parameters
    ----------
    method_name
        Stem of a file in ``wmlci/methods/`` (e.g. ``v16``, ``wmlci_pilot``).

    Returns
    -------
    dict
        config, summary and detail DataFrames, output paths, and scenarios run.
    """
    config = load_method_config(method_name)
    log.info(
        f"Running LCA method: {config.get('method_name', method_name)}"
    )

    bd.projects.set_current(config["bw_project_name"])

    db_name = config["inventory_database"]
    source = config["inventory_source"]
    jsonld = load_JSONLD_sourceData(
        source,
            datatype="jsonld",
            bw_database_name=db_name,
            data_version=config.get("inventory_source_version"),
    )
    # split multi-product processes so the technosphere matrix is square
    jsonld = split_multi_product_processes(jsonld)
    # check for errors in imported data - these checks do not fix the errors
    check_for_errors_in_jsonld_import(jsonld)
    # apply common clean up procedures
    jsonld = clean_JSONLD_sourceData(jsonld, config)
    # replace input providers using technosphere_updates YAML
    jsonld = update_technosphere_flows(jsonld, config["processes"], config)
    # check for errors again
    log.info("Checking errors are fixed")
    check_for_errors_in_jsonld_import(jsonld)
    # fix issues when openLCA and brightway have to talk by manipulating data sets
    jsonld.apply_strategies()
    # merge biosphere flows
    # jsonld.write_separate_biosphere_database()
    jsonld.merge_biosphere_flows()
    # checking if everything worked out with strategies and linking
    jsonld.statistics()
    # jsonld.write_excel(only_unlinked=False)  # set to True if errors
    # save the database
    jsonld.write_database()

    # LCIA methods import
    lcia_db_name = config["lcia_db_name"]
    jsonldlcia = load_JSONLD_sourceData(
        config["lcia_input"],
        datatype="jsonld_lcia",
        bw_database_name=lcia_db_name,
    data_version=config.get("lcia_input_version"),)
    # convert parameter lists to dicts
    jsonldlcia = convert_lcia_param_list_to_dict(jsonldlcia)
    # prepare LCIA - apply strategies, harmonize CF flows to FEDEFL,
    # link to inventory by UUID
    jsonldlcia.apply_strategies()
    jsonldlcia = map_lcia_to_fedelemflowlist_UUIDs(
        jsonldlcia, sourcelistname="IPCC"
    )
    jsonldlcia.match_biosphere_by_id(config["inventory_database"])
    # drop the CFs that do not match a flow
    jsonldlcia.drop_unlinked(verbose=True)
    jsonldlcia.statistics()
    jsonldlcia.write_methods(overwrite=True)

    db = bd.Database(config["inventory_database"])
    log.info(
        f"Database '{config['inventory_database']}' loaded "
        f"({len(db)} activities)"
    )

    method = tuple(config["lcia_method"])
    if method not in bd.methods:
        available = [m for m in bd.methods if config["lcia_db_name"] in m]
        raise ValueError(
            f"LCIA method {method} not in project. Available: {available[:10]}"
        )

    processes = resolve_processes(db, config)
    scenario_lines = []
    for act, product, process_settings in processes:
        fu_label = functional_unit_label(
            product.get("name", ""), process_settings["functional_unit"]
        )
        scenario_lines.append(f"  - {fu_label} by process: {act['name']}")
    log.info(
        f"Assessing {len(processes)} scenarios:\n" + "\n".join(scenario_lines)
    )

    results_df, detail_df = calculate_lca_results(db, processes, config)
    paths = write_lca_outputs(results_df, detail_df, config)

    print("\nLCA results (all scenarios):")
    print(results_df.to_string(index=False))

    return {
        "method": method_name,
        "config": config,
        "summary": results_df,
        "detail": detail_df,
        "paths": paths,
        "scenarios": [
            (a["name"], p["name"])
            for a, p, s in processes
        ],
    }


if __name__ == "__main__":
    import sys

    name = sys.argv[1] if len(sys.argv) > 1 else "v16"
    run_bw_lca(name)
