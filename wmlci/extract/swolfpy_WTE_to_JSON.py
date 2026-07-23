import requests as r
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
from esupy.location import read_iso_3166
from esupy.util import make_uuid
from flcac_utils.util import format_dqi_score
import os
from flcac_utils.util import generate_locations_from_exchange_df
from flcac_utils.generate_processes import build_location_dict
from flcac_utils.util import extract_actors_from_process_meta, \
    extract_sources_from_process_meta, extract_dqsystems
from flcac_utils.generate_processes import build_flow_dict, \
        build_process_dict, write_objects, validate_exchange_data
from flcac_utils.util import assign_year_to_meta
from flcac_utils.commons_api import get_single_object
import copy
import zipfile
from pathlib import Path
# %%

# Directory containing this .py file
PATH_PROJECT = Path(__file__).resolve().parent

PATH_PROJECT = Path(__file__).parent.parent
OUTPUT_PATH = PATH_PROJECT / "data/source_data/swolfpy"
METHODS_PATH = PATH_PROJECT / "methods"

with open(METHODS_PATH / "v16.yaml" , "r") as f:
    config = yaml.safe_load(f)
# %%


import pandas as pd
from swolfpy_processmodels import WTE

print(config['model_defaults']['functional_unit']['amount'])

# --------------------------------------------------
# Create and run model
# --------------------------------------------------


wte = WTE()

print(dir(wte))
print("Running WTE...")
print(type(wte.InputData))
print(wte.InputData.__dict__.keys())

#change transport distance, with mi to km conversion
wte.InputData.Material_Consumption["Distance_from_prod_fac"]["amount"] = config['model_defaults']['transport']['distance_miles'] * 1.60934
wte.calc()
print("Complete")

# %%


# --------------------------------------------------
# Check available material names
# --------------------------------------------------

print("\nAvailable material fractions:")
print(list(wte.Index))

material = "Mixed_Plastic"

if material not in wte.Index:
    raise ValueError(
        f"{material} not found.\nAvailable materials:\n{list(wte.Index)}"
    )

# --------------------------------------------------
# Energy results
# --------------------------------------------------

energy = wte.Energy_Calculations.loc[[material]]

print("\n=== ENERGY ===")
print(energy.T)

# --------------------------------------------------
# Emissions
# --------------------------------------------------

emissions = wte.Combustion_Emission.loc[[material]]

print("\n=== EMISSIONS ===")
print(emissions.T)

# --------------------------------------------------
# Ashes and recovered materials
# --------------------------------------------------

solids = wte.Post_Combustion_Solids.loc[[material]]

print("\n=== POST COMBUSTION SOLIDS ===")
print(solids.T)

# --------------------------------------------------
# APC reagent consumption
# --------------------------------------------------

apc = wte.APC_Consumption.loc[[material]]

print("\n=== APC CONSUMPTION ===")
print(apc.T)

# --------------------------------------------------
# Build reported inventory
# --------------------------------------------------
combustion_emission_unmapped = wte.Combustion_Emission.copy()

wte.report()

# %%
PROCESS_NAME = "Mixed Plastic WTE"
PROCESS_CATEGORY = "SwolfPy / WTE"
LOCATION = "US"

rows = []

schema = [
    "ProcessID",
    "ProcessCategory",
    "ProcessName",
    "FlowUUID",
    "FlowName",
    "Context",
    "IsInput",
    "FlowType",
    "reference",
    "default_provider",
    "default_provider_name",
    "amount",
    "amountFormula",
    "unit",
    "avoided_product",
    "exchange_dqi",
    "location",
]
# %% match uuids to flow names
# ------------------------------------------------------------------
# Biosphere flow mapping from SWOLF WTE.py
# ------------------------------------------------------------------

biosphere_uuid_by_name = {
    "Stack_Ammonia": "87883a4e-1e3e-4c9d-90c0-f1bea36f8014",
    "Sb": "77927dac-dea3-429d-a434-d5a71d92c4f7",
    "As": "dc6dbdaa-9f13-43a8-8af5-6603688c6ad0",
    "Ba": "7e246e3a-5cff-43fc-a8e6-02d191424559",
    "Cd": "1c5a7322-9261-4d59-a692-adde6c12de92",
    "CO2_fossil": "349b29d1-3e58-4c66-98b9-9d1a076efd2e",
    "CO2_biogenic": "eba59fd6-f37e-41dc-9ca3-c7ea22d602c7",
    "Stack_CO": "ba2f3f82-c93a-47a5-822a-37ec97495275",
    "Cr": "e142b577-e934-4085-9a07-3983d4d92afb",
    "Cu": "ec8144d6-d123-43b1-9c17-a295422a0498",
    "Stack_Nitrous_Oxide": "20185046-64bb-4c09-a8e7-e8a9e144ca98",
    "Stack_Dioxins_Furans": "082903e4-45d8-4078-94cb-736b15279277",
    "Stack_Hydrocarbons": "f9abb851-8731-4c5b-b057-863996a1f94a",
    "Stack_HCl": "c941d6d0-a56c-4e6c-95de-ac685635218d",
    "Pb": "8e123669-94d3-41d8-9480-a79211fe7c43",
    "Hg": "71234253-b3a7-4dfe-b166-a484ad15bee7",
    "Stack_Methane": "b53d3744-3629-4219-be20-980865e54031",
    "Ni": "a5506f4b-113f-4713-95c3-c819dde6e48b",
    "Stack_NOx": "c1b91234-6f24-417b-8309-46111d09c457",
    "Stack_PM": "21e46cb8-6233-4c99-bac3-c41d2ab99498",
    "Se": "454c61fd-c52b-4a04-9731-f141bb7b5264",
    "Stack_SO2": "fd7aa71c-508c-480d-81a6-8052aad92646",
    "Zn": "5ce378a0-b48d-471c-977d-79681521efde",
}


# %% load in crosswalk files for exploration

# fedelemflowlist = pd.read_excel(
#     PATH_PROJECT / "FedElemFlowList_1.3.0_all.xlsx"
# )

# UBW_crosswalk = pd.read_csv(
#     PATH_PROJECT / "working_bridge.csv"
# )

# UBW_crosswalk = UBW_crosswalk.drop_duplicates()

# %% uuid lookup for flow map candidates 

# uuid_lookup = dict(
#     zip(
#         fedelemflowlist['Flow UUID'],
#         zip(
#             fedelemflowlist['Context'],
#             fedelemflowlist['Flowable']
#         )
#     )
# )

# uuid_lookup = dict(
#     zip(
#         fedelemflowlist['Flow UUID'],
#         zip(
#             fedelemflowlist['Context'],
#             fedelemflowlist['Flowable']
#         )
#     )
# )

# for k, v in biosphere_uuid_by_name.items():

#     matches = UBW_crosswalk[
#         UBW_crosswalk['biosphere_id'] == v
#     ]

#     if matches.empty:
#         print(f"{k}: no UUID matches")
#         continue

#     # Keep only emission/air rows
#     air_matches = matches[
#         matches['uslci_id']
#         .map(lambda x: uuid_lookup.get(x, (None, None))[0])
#         .eq('emission/air')
#     ]

#     if air_matches.empty:
#         print(f"{k}: UUID found, but no emission/air context")
#         continue

#     print(f"\n{k}: {len(air_matches)} emission/air match(es)")

#     reported_matches = [
#         (
#             uuid_lookup.get(uslci_id, (None, None))[1],  # Flowable
#             uslci_id                                     # Flow UUID
#         )
#         for uslci_id in air_matches['uslci_id']
#     ]

#     print(reported_matches)
# %% flow map v1 manually assembled after using UBW crosswalk and fedefl to ID mapping candidates 

bios_to_fedefl_map = {
    "87883a4e-1e3e-4c9d-90c0-f1bea36f8014": ('Ammonia', '65b5d5dd-95b5-36b2-8cb0-7c5501ff1e32'),
    "77927dac-dea3-429d-a434-d5a71d92c4f7" : ('Antimony', '9de688f4-2302-3557-b50c-0f1d304977f4'),
    "dc6dbdaa-9f13-43a8-8af5-6603688c6ad0" : ('Arsenic', '2b16d0b5-4713-3bb8-be5b-905382f0e8a8'),
    "7e246e3a-5cff-43fc-a8e6-02d191424559" : ('Barium', '2c5aef1c-7cf7-30ee-8a87-ba4682327767'),
    "1c5a7322-9261-4d59-a692-adde6c12de92" : ('Cadmium', 'd5a296be-7219-3921-a430-cff172ac7911'),
    "349b29d1-3e58-4c66-98b9-9d1a076efd2e" : ('Carbon dioxide', 'b6f010fb-a764-3063-af2d-bcb8309a97b7'),
    "eba59fd6-f37e-41dc-9ca3-c7ea22d602c7" : 'no uuid matches',
    "ba2f3f82-c93a-47a5-822a-37ec97495275" : ('Carbon monoxide', '187c525c-3715-388c-b303-a0671524a615'),
    "e142b577-e934-4085-9a07-3983d4d92afb" : ('Chromium', '98bd36e1-fbe4-32ee-ba78-3b6726917c9b'),
    "ec8144d6-d123-43b1-9c17-a295422a0498" : ('Copper', 'b15117ec-3b8e-35de-bef2-00aec1b9636e'),
    "20185046-64bb-4c09-a8e7-e8a9e144ca98" : ('Nitrous oxide', 'cfee0524-7ad6-300b-b050-6249135a2492'),
    "082903e4-45d8-4078-94cb-736b15279277" : ('Chlorinated dioxins and furans -- 2,3,7,8 congeners only','16c208f1-371c-3e27-a0ca-ee18b01d862e'),
    "f9abb851-8731-4c5b-b057-863996a1f94a" : ('Hydrocarbons', '6a8ca31c-ede5-38b1-8c30-2d50fb499a55'),
    "c941d6d0-a56c-4e6c-95de-ac685635218d" : ('Hydrochloric Acid', 'e2230ed6-8d5e-3315-8bc9-ce337d1283ce'),
    "8e123669-94d3-41d8-9480-a79211fe7c43" : ('Lead', 'fe829136-3042-36e6-b4cb-7ff591e8db98'),
    "71234253-b3a7-4dfe-b166-a484ad15bee7" : ('Mercury', 'e2c65e04-f613-33db-9a6e-4cb4577b0005'),
    "b53d3744-3629-4219-be20-980865e54031" : ('Methane','aab83476-ec6c-3742-af85-15d320b7ce80'),
    "a5506f4b-113f-4713-95c3-c819dde6e48b" : ('Nickel', '50f56ea3-e292-3c09-89d2-74466af5f11e'),
    "c1b91234-6f24-417b-8309-46111d09c457" : ('Nitrogen oxides', '4382ba18-dd21-3837-80b2-94283ef5490e'),
    "21e46cb8-6233-4c99-bac3-c41d2ab99498" : ('Particulate matter, ≤ 2.5μm', '49a9c581-7c83-36b0-b1bd-455ea4c665a6'),
    "454c61fd-c52b-4a04-9731-f141bb7b5264" : ('Selenium', 'b6db14bd-73e9-3f02-852a-e8b5e60bbc61'),
    "fd7aa71c-508c-480d-81a6-8052aad92646" : ('Sulfur dioxide', 'f4973035-59f5-3bdc-b257-b274dcc04e0f'),
    "5ce378a0-b48d-471c-977d-79681521efde" : ('Zinc', '435bfa52-d2d3-3760-abf4-27de892972ac'),
}

# %%
    

# ------------------------------------------------------------------
# Technosphere flows that should be inputs to the WTE process
# ------------------------------------------------------------------

technosphere_flows = {
    "Internal_Process_Transportation_Heavy_Duty_Diesel_Truck",
    "Empty_Return_Heavy_Duty_Diesel_Truck",
    "lime_hydrated_loose_weight_RoW_lime_production",
    "ammonia_liquid_RoW_ammonia_production_steam_reforming_liquid",
    "charcoal_GLO_charcoal_production",
    "Al",
    "Fe",
    "Bottom_Ash",
    "Unreacted_Ash",
    "Fly_Ash"
}


def get_flow_uuid(flow_name, flow_uuid=""):
    """
    Preserve supplied UUIDs.
    Generate deterministic UUIDs for product, waste, and unmapped flows.
    """

    if flow_uuid not in [None, ""]:
        return flow_uuid

    return make_uuid(flow_name)


def add_exchange(
    flow_name,
    amount,
    unit,
    flow_type,
    is_input,
    provider_name,
    provider_uuid,
    reference=False,
    flow_uuid="",
    context=PROCESS_CATEGORY,
):
    rows.append(
        {
            "ProcessID": "",
            "ProcessCategory": PROCESS_CATEGORY,
            "ProcessName": PROCESS_NAME,
            "FlowUUID": get_flow_uuid(flow_name, flow_uuid),
            "FlowName": flow_name,
            "Context": context,
            "IsInput": is_input,
            "default_provider": provider_name,
            "default_provider_name": provider_uuid,
            "FlowType": flow_type,
            "reference": reference,
            "amount": float(amount),
            "amountFormula": "",
            "unit": unit,
            "avoided_product": "",
            "exchange_dqi": "",
            "location": LOCATION,
        }
    )

# ------------------------------------------------------------------
# Reference flow
# ------------------------------------------------------------------

add_exchange(
    flow_name="Mixed Plastic",
    amount=1,
    unit="sh tn",
    flow_type="PRODUCT_FLOW",
    is_input=True,
    provider_name='',
    provider_uuid='',
    reference=True,
)

# %%

# ------------------------------------------------------------------
# Technosphere exchanges
# ------------------------------------------------------------------
# Zero values are intentionally retained.
# Only NaN values are skipped.
tech_map = pd.read_excel(
    PATH_PROJECT / "utils/flowmapping/SwolfPy_WTE_Tech_flow_map.xlsx"
)

swolfpy_to_uslci = {}

# Row 0 = USLCI Flow
# Row 1 = USLCI Flow UUID
# Row 2 = USLCI Flow Provider
# Row 3 = USLCI Flow Provider UUID
uslci_flows = tech_map.iloc[0]
uslci_uuids = tech_map.iloc[1]
provider_names = tech_map.iloc[2]
provider_uuids = tech_map.iloc[3]

for swolfpy_flow in tech_map.columns:

    if swolfpy_flow == "SwolfPy Flow":
        continue

    provider_name = provider_names[swolfpy_flow]
    provider_uuid = provider_uuids[swolfpy_flow]

    if provider_name == "Cutoff flow":
        provider_name = None

    if provider_uuid == "Cutoff flow":
        provider_uuid = None

    swolfpy_to_uslci[swolfpy_flow] = {
        "flow_name": uslci_flows[swolfpy_flow],
        "flow_uuid": uslci_uuids[swolfpy_flow],
        "provider_name": provider_name,
        "provider_uuid": provider_uuid,
    }


for flow, amount in wte.WTE["Technosphere"][material].items():

    if pd.isna(amount):
        continue

    flow_name = flow[1] if isinstance(flow, tuple) else str(flow)

    unit = "kg"

    if "Electricity" in flow_name:
        unit = "kWh"
    elif "Heat" in flow_name:
        unit = "MJ"
    elif "Truck" in flow_name:
        unit = "tkm"

    is_input = flow_name in technosphere_flows

    # Apply SwolfPy -> USLCI mapping if available
    mapped = swolfpy_to_uslci.get(flow_name)

    if mapped is not None:

        add_exchange(
            flow_name=mapped["flow_name"],
            amount=amount,
            unit=unit,
            flow_type="PRODUCT_FLOW",
            is_input=is_input,
            flow_uuid=mapped["flow_uuid"],
            provider_name=mapped["provider_name"],
            provider_uuid=mapped["provider_uuid"],
            context=PROCESS_CATEGORY,
        )

    else:

        print(f"No USLCI mapping found for {flow_name}; using generated UUID")

        add_exchange(
            flow_name=flow_name,
            amount=amount,
            unit=unit,
            flow_type="PRODUCT_FLOW",
            is_input=is_input,
            provider_name='',
            provider_uuid='',
            flow_uuid=make_uuid(flow_name),
            context=PROCESS_CATEGORY,
        )
        
# %%
        
# ------------------------------------------------------------------
# Waste outputs
# ------------------------------------------------------------------
# Zero values are intentionally retained.
# Only NaN values are skipped.

# for flow, amount in wte.WTE["Waste"][material].items():

#     if pd.isna(amount):
#         continue

#     flow_name = str(flow)

#     add_exchange(
#         flow_name=flow_name,
#         amount=amount,
#         unit="kg",
#         flow_type="PRODUCT_FLOW",
#         is_input=False,
#         provider_name='',
#         provider_uuid='',
#         flow_uuid=make_uuid(flow_name),
#         context=PROCESS_CATEGORY,
#     )

for flow, amount in wte.WTE["Waste"][material].items():

    if pd.isna(amount):
        continue

    flow_name = flow[1] if isinstance(flow, tuple) else str(flow)

    unit = "kg"

    is_input = flow_name in technosphere_flows

    # Apply SwolfPy -> USLCI mapping if available
    mapped = swolfpy_to_uslci.get(flow_name)

    if mapped is not None:

        add_exchange(
            flow_name=mapped["flow_name"],
            amount=amount,
            unit=unit,
            flow_type="PRODUCT_FLOW",
            is_input=False,
            flow_uuid=mapped["flow_uuid"],
            provider_name=mapped["provider_name"],
            provider_uuid=mapped["provider_uuid"],
            context=PROCESS_CATEGORY,
        )

    else:

        print(f"No USLCI mapping found for {flow_name}; using generated UUID")

        add_exchange(
            flow_name=flow_name,
            amount=amount,
            unit=unit,
            flow_type="PRODUCT_FLOW",
            is_input=False,
            provider_name='',
            provider_uuid='',
            flow_uuid=make_uuid(flow_name),
            context=PROCESS_CATEGORY,
        )
# ------------------------------------------------------------------
# Biosphere exchanges
# ------------------------------------------------------------------
# Zero values are intentionally retained.
# Only NaN values are skipped.
#
# Uses combustion_emission_unmapped so that:
#   FlowName = SWOLF internal name
#   FlowUUID = SWOLF biosphere UUID

for flow_name, flow_uuid in biosphere_uuid_by_name.items():

    # Skip flows that are not present in the source dataframe
    if flow_name not in combustion_emission_unmapped.columns:
        continue

    amount = combustion_emission_unmapped.loc[material, flow_name]

    # Skip missing values
    if pd.isna(amount):
        continue

    # Look up the corresponding FedElemFlowList flow
    mapped = bios_to_fedefl_map.get(flow_uuid)

    # Handle unmapped flows
    if mapped == "no uuid matches":
        print(f"Skipping {flow_name}: no FedElemFlowList mapping found")
        continue

    if mapped is None:
        print(f"Skipping {flow_name}: UUID {flow_uuid} not found in mapping dictionary")
        continue

    # Unpack (Flowable, Flow UUID)
    mapped_flow_name, mapped_uuid = mapped

    add_exchange(
        flow_name=mapped_flow_name,
        amount=amount,
        unit="kg",
        flow_type="ELEMENTARY_FLOW",
        is_input=False,
        provider_name='',
        provider_uuid='',
        flow_uuid=mapped_uuid,
        context="emission/air",
    )



# %%


# ------------------------------------------------------------------
# Build OLCA inventory dataframe
# ------------------------------------------------------------------

df_olca = pd.DataFrame(rows)

for col in schema:
    if col not in df_olca.columns:
        df_olca[col] = ""

df_olca = df_olca[schema]

# %% addtional unit conversion to kg/Mg basis

exceptions = ['Ammonia','Charcoal','lime','Transport','Electricity','Plastic']
pattern = '|'.join(exceptions)

mask = df_olca['FlowName'].str.contains(pattern, case=False, regex=True)

df_olca.loc[~mask, 'amount'] = df_olca.loc[~mask, 'amount'] * 1000


# %% convert all exchanges up to sh ton basis 

ref_mask = df_olca['reference']
#df_olca.loc[~ref_mask, 'amount'] = df_olca.loc[~ref_mask, 'amount'] * 1.10231
df_olca.loc[~ref_mask, 'amount'] = df_olca.loc[~ref_mask, 'amount'] * (1000 / config['model_defaults']['functional_unit']['amount'])

# %%


# ------------------------------------------------------------------
# Assign Process UUID
# ------------------------------------------------------------------

process_uuid = make_uuid(PROCESS_NAME)
df_olca["ProcessID"] = process_uuid


# ------------------------------------------------------------------
# Sanity checks
# ------------------------------------------------------------------

bad_tuple_names = df_olca[
    df_olca["FlowName"].astype(str).str.startswith("(")
]

if len(bad_tuple_names) > 0:
    print("WARNING: Some FlowNames still look like tuples:")
    print(bad_tuple_names[["FlowUUID", "FlowName"]])


missing_uuids = df_olca[
    df_olca["FlowUUID"].isna() | (df_olca["FlowUUID"] == "")
]

if len(missing_uuids) > 0:
    print("WARNING: Some flows are missing UUIDs:")
    print(missing_uuids[["FlowName", "FlowUUID"]])


# ------------------------------------------------------------------
# Optional review
# ------------------------------------------------------------------

print("\nInventory Summary")
print("-" * 50)
print(f"Process UUID: {process_uuid}")
print(f"Number of exchanges: {len(df_olca)}")
print()

print(
    df_olca[
        [
            "FlowName",
            "FlowUUID",
            "FlowType",
            "IsInput",
            "reference",
            "amount",
            "unit",
        ]
    ]
)

print(df_olca.groupby('IsInput')['amount'].sum())
# %%



# # ------------------------------------------------------------------
# # Optional export
# # ------------------------------------------------------------------

# df_olca.to_csv(
#     "mixed_plastic_wte_olca_inventory.csv",
#     index=False,
# )

# print("\nExported mixed_plastic_wte_olca_inventory.csv")

# %%

# validate_exchange_data(df_olca)
flows, new_flows = build_flow_dict(df_olca)

# %% build_process_dict

meta={}
location_objs = {}
source_objs={}
actor_objs={}
dq_objs={}
df_params={}

id_to_name = df_olca.set_index("ProcessName")["ProcessID"].to_dict()
processes = {}

for process_name in id_to_name.keys():

    # Filter rows where 'ProcessName' matches 'process_name'
    filtered_df = df_olca[df_olca['ProcessName'] == process_name]
    p_dict = build_process_dict(
        filtered_df,
        flows,
        meta={},
        location_objs = {},
        source_objs={},
        actor_objs={},
        dq_objs={}, df_params={}
    )
    processes.update(p_dict)

# %% write everything

write_objects(
    "SwolfPy_WTE_PW_JSON",
    flows,
    new_flows,
    processes,
    location_objs,
    dq_objs,
    source_objs,
    actor_objs,
    out_path=OUTPUT_PATH,
)

