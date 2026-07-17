# Extract Folder

The scripts and method yamls in the `extract/` folder are used to download source data used to run the waste management LCAs. 

The data is primarily sourced from the Federal LCA Commons API.

To run these data downloads and store the source data locally within `data/source_data/`, you will need to create an API key and store within `extract/API_Keys.env`. See `extract/API_Keys.env.example` to see how to store the keys. Create an API key at https://api.data.gov/signup/. And see the FLCAC user guide to learn how to pull data using the API https://www.lcacommons.gov/lca-commons-api-guide.
