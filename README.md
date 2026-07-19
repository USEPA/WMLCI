# Waste Management Life Cycle Inventory Model Assembler and Calculator (WMLCI)

WMLCI is being developed to update the US Environmental Protection Agency's (USEPA) approach to generating the Waste Reduction Model Excel tool. This revised approach is intended to provide users with a transparent model that is reproducible, editable, and quickly updatable as new data sources are released.

WMLCI prepares life cycle inventory (LCI) data and uses the [Brightway](https://docs.brightway.dev/) LCA framework to import that inventory, apply LCIA methods (e.g., IPCC GWP from [Federal LCA Commons](https://www.lcacommons.gov/)), and calculate life cycle assessment (LCA) results for selected materials and waste management pathways.  
The analysis is not limited to greenhouse gases, WMLCI is designed to assesses other environmental impacts.

At this time, the WMLCI package reproduces and updates the approach to 3 processes in the Waste Reduction Model:
   1. MSW landfilling of Food Waste; National average LFG recovery, typical collection, National average conditions
   2. MSW combustion of Mixed Plastics
   3. MSW recycling of Mixed Plastics

Results are scenario-level LCIA scores (e.g. GWP as kg CO₂e) output as CSVs that break each score down by contributing activity.

## Run an LCI + LCIA calculation

Model methods are defined in YAML files in `wmlci/methods/`.

Available methods:
   1. `v16`: replicates the v16 Excel tool for the 3 material-pathways
   2. `wmlci_pilot`: base v16 Excel tool model with updated LCI (FLCAC technosphere data) and LCIA (newer IPCC GWP) for the 3 material-pathways

To run in Python:

```python
from wmlci.lca import run_bw_lca

run_bw_lca("v16")
```

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. EPA has relinquished control of the information and no longer has responsibility to protect the integrity, confidentiality, or availability of the information. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA. The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
