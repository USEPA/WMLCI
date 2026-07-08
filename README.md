# Waste Management Life Cycle Inventory  Model Assembler and Calculator (WMLCI)

WMLCI is being developed to update the EPA's approach to generating the Waste Reduction Model Excel tool. This updated approach is intended to provide users with a transparent model that is reproduceable, editable, and quickly updatable as new data sources are released. 

The model runs life cycle assessments (LCAs) using the [Brightway](https://docs.brightway.dev/) ecosystem. The base process data are output from the Waste Reduction Model openLCA JSON-LD inventories and published impact methods (e.g., IPCC GWP from [Federal LCA Commons](https://www.lcacommons.gov/)). The original process data, which are the data behind the v16 Excel tool, are updated with data from Federal LCA Commons. 

At this time, the model is designed to reproduce and update the approach to 3 processes in the Waste Reduction Model:
   1. MSW landfilling of Food Waste; National average LFG recovery, typical collection, National average conditions 
   2. MSW combustion of Mixed Plastics
   3. MSW recycling of Mixed Plastics


## Run a life cycle assessment (LCA) model

Methods are defined in YAML files in `wmlci/methods/`.

Available methods:
   1. `v16`: replicates the v16 Excel tool for 3 processes
   2. `wmlci_demo`: updated approach to the 3 processes

To run in Python:

```python
from wmlci.lca import run_bw_lca

run_bw_lca("v16")
```

## Disclaimer

The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. EPA has relinquished control of the information and no longer has responsibility to protect the integrity, confidentiality, or availability of the information. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA. The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
