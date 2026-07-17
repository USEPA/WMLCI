"""
Build derived source-data products by running scripts defined in extract YAML configs.
"""

from __future__ import annotations

from pathlib import Path

from wmlci.extract.extract_common import load_extract_yaml, source_data_dir


def extract_source_data_from_script(
    method_name: str,
    version: str | None = None,
) -> Path:
    """Run the extract yaml ``script_function`` and return its output path."""
    config = load_extract_yaml(method_name)
    version = version or config.get("version")

    script_function = config.get("script_function")
    if not callable(script_function):
        raise ValueError(f"{method_name} does not define a script_function")

    output_dir = source_data_dir(method_name, version)
    output_dir.mkdir(parents=True, exist_ok=True)

    return Path(
        script_function(
            method_name=method_name,
            config=config,
            output_dir=output_dir,
        )
        or output_dir
    )
