"""
Download or build source data from YAMLs in ``wmlci/extract/``.

Not required before running LCA methods: ``run_bw_lca`` /
``examples/run_lca_models.py`` already fetch missing extract data when a
method runs. Use this script to prefetch or refresh sources on their own.

From the repository root:

    python examples/extract_source_data.py ipcc_gwp
    python examples/extract_source_data.py uslci
    python examples/extract_source_data.py uslci waste_reduction_model_v16_pilot

If no extract names are passed, defaults to ``ipcc_gwp``. Prefer passing the
name explicitly as above. FLCAC API sources need ``wmlci/extract/API_Keys.env``
(see ``API_Keys.env.example``). Output goes to ``wmlci/data/source_data/``.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from wmlci.extract.extract_common import extract_source_data
from wmlci.log import log

DEFAULT_EXTRACTS = ("ipcc_gwp",)


def main(names: list[str] | None = None) -> None:
    to_run = names or list(DEFAULT_EXTRACTS)
    for name in to_run:
        log.info(f"=== Extract source: {name} ===")
        out = extract_source_data(name)
        log.info(f"=== Finished: {name} -> {out} ===")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    main(args or None)
