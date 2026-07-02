# Data Descriptions

## source_data

Source data are stored as zip files on [USEPA's Data Commons](https://dmap-data-commons-ord.s3.amazonaws.com/index.html?prefix=flowsa/#WMLCI/sourceData/). Data are automatically downloaded when running scripts. 
Data Commons zips and unzipped JSON-LD inventories are saved to ``wmlci/data/source_data/epa_data_commons/``. 
API downloads (e.g. Federal LCA Commons) are saved to ``wmlci/data/source_data/{source}/``.

### source_data File Descriptions and Metadata

1. IPCC_LCIA_methods_1.2024-12.0

2. USLCI_1_2025_03_0.zip
   - NREL's USLCI v.1.2025-03.0
     - Download location: https://www.lcacommons.gov/lca-collaboration/National_Renewable_Energy_Laboratory/USLCI_Database_Public/datasets?commitId=2d6ba2b50f0d6696f282cddefe614ed870526f3c

3. USLCI_1_2025_06_0.zip
   - NREL's USLCI v.1.2025-06.0
     - Download location: https://www.lcacommons.gov/lca-collaboration/National_Renewable_Energy_Laboratory/USLCI_Database_Public/datasets?commitId=978870b1695caa9519f55aa5904dfd348a823623


4. USLCI_Q2_2025_elci_merged.zip
   - Created internally at Eastern Research 
   - This is the exported database created by merging the following and exporting as a JSON-LD:
     -  USLCI v.1_2025_06_0 
     -  US Electricity Baseline v1.2025-06.0
       - Download location: https://www.lcacommons.gov/lca-collaboration/Federal_LCA_Commons/US_electricity_baseline/datasets  

5. warm_v16_openlca_database_2025-06-13
