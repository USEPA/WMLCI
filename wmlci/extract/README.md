# Extract Folder

The scripts and method yamls in the `extract/` folder are used to download source data used to run the waste management LCAs. 

The data is primarily sourced from the Federal LCA Commons API.

To run these data downloads and store the source data locally within `data/source_data/`, you will need to create an API key and store within `extract/API_Keys.env`. See `extract/API_Keys.env.example` to see how to store the keys. Create an API key at https://api.data.gov/signup/. And see the FLCAC user guide to learn how to pull data using the API https://www.lcacommons.gov/lca-commons-api-guide.

## Generating and integrating data from SwolfPy 

The unit process for waste-to-energy combustion of mixed plastic sources data from the Solid Waste Optimization Life-cycle Framework (SwolfPy). SwolfPy is a free and open-source solid waste management LCA framework that runs on Python. Incorporating SwolfPy expands the flow coverage of the combustion process, and maintains the open-sourced data stack that the model requires. 

The SwolfPy data pipeline is split into two distinct steps: 

1. Generating the inventory and converting it into olca-schema JSON. 
2. Combining the flows and parameters with the WMLCI pilot JSON. 

### Generating the JSON with swolfpy_WTE_to_JSON.py

Step 1 is handled in swolfpy_WTE_to_JSON.py-- due to dependency conflicts, it's recommended to install and run this script in a separate environment from the rest of the WMLCI pipeline. The raw inventory data from SwolfPy is modified in the following ways: 

1. Flow uuids from SwolfPy are mapped to fedefl flow names and uuids. This is handled inside the script with a crosswalk dictionary.
2. SwolfPy exchanges are normalized to 1Mg equivalent of mixed plastic combustion. The exchange values are scaled to the basis of 1 short ton equivalent of mixed plastic combustion. 
3. The transportation exchange is parameterized inside the olca-schema, allowing downstream customization of the transport distance. 
4. USLCI flow providers are assigned to a number of input exchanges. 

Notes: 
- Of the available flows in SwolfPy only one lacked a clear fedefl mapping target (biogenic CO2). 
- Of the mapped flows, two were found to have no characterization factors in TRACI (stack hydrocarbons, stack dioxins and furans). 

### Integrating the models with integrate_swolfpy_data.py

Once the SwolfPy JSON is generated, integrate_swolfPy_data.py can be run (inside the wmlci environment), which modifies the inventory to more closely align with WMLCI and meet the requirements of the brightway schema. The following edits are performed: 

1. Bottom ash and fly ash co-products exchange amounts are set to zero. 
2. Electricity co-product output is moved to the inputs and the sign is reversed. 
3. HDPE and PET combustion processes are removed from WMLCI JSON. 

Following the data manipulation, SwolfPy and WMLI flow dictionaries and parameter dictionaries are combined, resulting in a zipped JSON file that contains an intermediate version of WMLCI with SwolfPy integrated. 

Note:
- The combined WMLCI & SwolfPy data output produced by integrate_swolfpy_data.py is considered intermediate because it does not meet the requirements of brightway databases. Specifically, it does not contain a square technosphere matrix (e.g. the JSON contains co-products in the output). This is resolved downstream in the WMLCI pipeline. 
- The electricity flow is not assigned a default provider. To fully capture an "avoided impact of grid electricity" scenario, a provider must be linked manually.