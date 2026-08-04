# Examples

| Script | Purpose                                                                                                                                                                             |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`run_lca_models.py`](run_lca_models.py) | Run LCA for `v16` and `wmlci_pilot` (or methods passed as args)                                                                                                                     |
| [`generate_graphics.py`](generate_graphics.py) | Compare result CSVs and write SVGs under `wmlci/data/results/graphics/` |
| [`extract_source_data.py`](extract_source_data.py) | Call extract YAMLs directly to download/build raw source data. Running a method YAML already downloads missing extract data, so this function is optional |



```bash
# from repository root
python examples/run_lca_models.py
python examples/generate_graphics.py
python examples/extract_source_data.py ipcc_gwp
```
