# Methods

YAML configs for LCA model runs. Call `run_bw_lca("<name>")`.

| Method | Description                                                                                                                                                |
|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`v16`](v16.yaml) | Replicates Waste Reduction Model Excel Tool v16 GWP (IPCC AR4-100) for landfill (food waste), combustion (mixed plastics), and recycling (mixed plastics). |
| [`wmlci_pilot`](wmlci_pilot.yaml) | Updates model data from the Waste Reduction Model Excel Tool v16 to use newer Federal LCA Commons (FLCAC) data and IPCC AR6-100 GWP.                       |
| [`wmlci_pilot_smog`](wmlci_pilot_smog.yaml) | Appends SwolfPy WTE air emissions to the Waste Reduction Model data and generates LCIA results for TRACI 2.2 smog formation (kg O₃ eq) instead of GWP.     |
