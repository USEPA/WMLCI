"""
Generate graphics for example LCA method results.

Default compares ``v16`` vs ``wmlci_pilot`` using CSVs under
``wmlci/data/results/``. Writes SVGs under ``wmlci/data/results/graphics/``.

Pass one method name for single-method charts (no comparison), e.g. smog:

    python examples/generate_graphics.py wmlci_pilot_smog

From the repository root (with the package installed, e.g. ``pip install -e .``):

    python examples/generate_graphics.py
    python examples/generate_graphics.py v16 wmlci_pilot

Run ``examples/run_lca_models.py`` first so result CSVs exist.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running without an editable install when invoked as a file path.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import matplotlib

matplotlib.use("Agg")

from wmlci.log import log
from wmlci.scripts.generate_result_graphics import generate_graphics

DEFAULT_METHOD_A = "v16"
DEFAULT_METHOD_B = "wmlci_pilot"


def main(
    method_a: str = DEFAULT_METHOD_A,
    method_b: str | None = DEFAULT_METHOD_B,
) -> Path:
    out = generate_graphics(method_a, method_b)
    log.info(f"Graphics written to {out}")
    return out


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main(DEFAULT_METHOD_A, DEFAULT_METHOD_B)
    elif len(sys.argv) == 2:
        main(sys.argv[1], None)
    else:
        main(sys.argv[1], sys.argv[2])
