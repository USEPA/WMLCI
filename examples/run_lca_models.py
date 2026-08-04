"""
Run LCA models for the example methods (v16 and wmlci_pilot).

From the repository root (with the package installed, e.g. ``pip install -e .``):

    python examples/run_lca_models.py
    python examples/run_lca_models.py v16
    python examples/run_lca_models.py wmlci_pilot

Writes summary, detail, and characterized-inventory CSVs under
``wmlci/data/results/``.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running without an editable install when invoked as a file path.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from wmlci.lca import run_bw_lca
from wmlci.log import log

DEFAULT_METHODS = ("v16", "wmlci_pilot")


def main(methods: list[str] | None = None) -> None:
    to_run = methods or list(DEFAULT_METHODS)
    for name in to_run:
        log.info(f"=== Running LCA method: {name} ===")
        run_bw_lca(name)
        log.info(f"=== Finished: {name} ===")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    main(args or None)
