####
#Comparing myac events packagenames
####

# Import Packages
import pandas as pd
import numpy as np
from datetime import date
import shutil
import xlsxwriter
import ast
# Import self-defined functions
from functions import *
import matplotlib.pyplot as plt

# auto-reload
%reload_ext autoreload
%autoreload 2

# show all columns
pd.set_option('display.max_columns', 999)

##########################################################################################
# INPUTS
##########################################################################################

# reduced filter 

criteria_specified="regular"

# define date
today = date.today()
today = today.strftime("%Y-%m-%d")

# select driver version
driver_version = "18"

# path to migration file
path_to_migration_file = "myplant_myac_migration_file.xlsx"

# path to folder containing already migrated files
migration_folder = "migrated"

##########################################################################################
# IMPORT DATA
##########################################################################################

# activate driver
conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")

# load oracle data
oracle_landscape_raw = import_oracle_data_from_azure(conn)

# load additional unit status info to further exclude expired units
additional_unit_status = import_additional_unit_status_info(conn)

# load myac package data - is_myac_last_event == 1 (only this entry is the current valid one) & billingtype == "PACKAGE"
unit_definition_billings_h = get_unit_definition_billings_h(conn)
unit_definition_billings_h_select = unit_definition_billings_h[lambda x: (x["is_myac_last_event"] == 1) & (x["billingtype"] == "PACKAGE")][["unitdefinition_id", "billingtype", "title", "rate", "uom", "packagename", "maturityintervals"]] # , "id" 

# load myac unit data (also with is_myac_last_event == 1). this is mainly to attach serial number (usn) and be able to attach the contract later
unit_definition_h = get_unit_definition_h(conn)
unit_definition_h_select = unit_definition_h[lambda x: (x["is_myac_last_event"] == 1)][["id", "contractid", "unitstartcounter", "unitendcounter", "serialnumber", "enginetype", "engineversion"]] # "contractid", 

# load myac contract data (opportunity id and contract number)
contract_definition_h = get_contract_definition_h(conn)
contract_definition_h_select = contract_definition_h[lambda x: (x["is_myac_last_event"] == 1)][["id", "opportunityid", "primarycontract", "oraclecontractsnumber"]].drop_duplicates()

# load myac opportunity data (otr-status and customername)
opportunity_definition_h = get_opportunity_definition_h(conn)
opportunity_definition_h_select = opportunity_definition_h[lambda x: (x["is_myac_last_event"] == 1)][["id", "opportunitynumber", "version", "customername"]].drop_duplicates()

#load ib report for engine oph counter 

engine_oph_counter=import_ib_extended_from_azure(conn)
#[element for element in engine_oph_counter.columns.tolist() if "serial_number" in element]
engine_oph_counter=engine_oph_counter[["unit_serial_no","unit_design_number",
                                       "engine_counter_ophs","eng_ophs_real_cntr_reading","eng_ophs_real_cntr_reading_dt"]].rename(columns={"engine_serial_number":"esn",
                                                                                                                                                "unit_serial_no":"usn",
                                                                                                                                                "unit_design_number":"design_number"})
engine_oph_counter=(format_usn_or_esn(engine_oph_counter, "usn")
                                             .rename(columns = {"usn": "usn_unformatted","usn formatted": "usn"
                                                                }) )
engine_oph_counter=engine_oph_counter.loc[lambda x: x["usn"].isna()==False,:]

# load myPlant data
# import from Azure
geo_loc_ib_metabase = import_geo_loc(conn)
dmp_events = import_dmp_events(conn)
sbom_nonsuperseded = import_sbom_nonsuperseded(conn)


##########################################################################################
# PREPARE ORACLE-DATA (BACKBONE)
##########################################################################################

# usage contracts 

usage_contracts=oracle_landscape_raw.loc[lambda x: (x["contract status"] == "ACTIVE")&(x["contract type oracle"].isin(["MSA PREVENTIVE AND CORRECTIVE","MSA USAGE BILLED"])==True
                                                                                       ),"unit serial number"].nunique()

# format columns
oracle_landscape_active_msa_formatted = transform_oracle_data(oracle_landscape_raw)[lambda x: (x["oracle_unit_status"] == "ACTIVE")&(x["oracle_contract_type"] == "MSA BILLABLE SHIPPING")][["contract_number", "usn",
                                                                                                                                     "usn_unformatted", "esn",
                                                                                                                                       "oracle_unit_status",
                                                                                                                                         "unit status ib",
                                                                                                                                           "contract start oph oracle", "contract end oph oracle", "eot_date",
                                                                                                                                             "customer name",
                                                                                                                                             "oracle_contract_type"]]

# add additional unit status
oracle_landscape_active_msa_formatted_ext = (oracle_landscape_active_msa_formatted
                                             .assign(oracle_unit_status = lambda x: 
                                                     np.where((x["unit status ib"].astype(str) == "None") | (x["usn_unformatted"].isin(additional_unit_status["usn_unformatted"].unique())), "EXPIRED_ACCORDING_TO_IB", x["oracle_unit_status"]))
                                             .drop(columns = ["usn_unformatted", "unit status ib"]) 
                                             )

# add flag regarding additional unit status
oracle_landscape_active_msa_formatted_ext = oracle_landscape_active_msa_formatted_ext.assign(flag_unit_inactive_in_ib = lambda x: np.where(x["oracle_unit_status"] != "ACTIVE", True, False))

# add flag regarding contracts / units ending in 2023
oracle_landscape_active_msa_formatted_ext = oracle_landscape_active_msa_formatted_ext.assign(flag_eot_date_reached_in_2023 = lambda x: np.where(x["eot_date"].astype(str).str[:4] == "2023", True, False)).drop(columns = "eot_date")

# new flag usage
#oracle_landscape_active_msa_formatted_ext = oracle_landscape_active_msa_formatted_ext.assign(flag_usage_contract = lambda x: np.where(x["oracle_contract_type"] != "MSA BILLABLE SHIPPING", True, False)).drop(columns = "oracle_contract_type")

# add flag when multiple contracts for one unit ==> safety guard against myplant "confusion"
oracle_multiple_contracts=oracle_landscape_active_msa_formatted_ext.groupby(["usn"]).aggregate({"contract_number":"nunique"}).reset_index()
oracle_multiple_contracts=oracle_multiple_contracts.rename(columns={"contract_number":"number_contracts"})
oracle_multiple_contracts=oracle_multiple_contracts.loc[lambda x: x["number_contracts"]>1,:]

oracle_landscape_active_msa_formatted_ext = oracle_landscape_active_msa_formatted_ext.assign(flag_multiple_contracts = lambda x: 
                                                                                             np.where(x["usn"].isin(oracle_multiple_contracts["usn"])==True,
                                                                                                       True, False))

##########################################################################################
# PREPARE MYA-C DATA
##########################################################################################

#### MERGE MYA-C DATA
# combine unit- and package data
df_unit_definition_and_billings = pd.merge(
    unit_definition_h_select.rename(columns = {"id": "unitdefinition_id"}),
    unit_definition_billings_h_select,
    on = "unitdefinition_id",
    how = "outer"
)

# add contract info
df_unit_definition_and_billings_and_contract_info = pd.merge(
    df_unit_definition_and_billings,
    contract_definition_h_select.rename(columns = {"id": "contractid"}),
    on = "contractid",
    how = "left"
)

# add otr-status info
df_unit_definition_and_billings_and_contract_info = pd.merge(
    df_unit_definition_and_billings_and_contract_info,
    opportunity_definition_h_select.rename(columns = {"id": "opportunityid"}),
    on = "opportunityid",
    how = "left"
)

    
# add to df using packagename or title 
use_title=False
if use_title: 
    df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]==""),"title"]=df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]==""),"packagename"]
    myac_translation = create_harmonization_table_for_myac_or_cpq_package_names(df=df_unit_definition_and_billings_and_contract_info.assign(title = lambda x: x["title"].astype(str)), unharmonized_name_column="title")
    df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]=="not defined"),"title"]=df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]=="not defined"),"packagename"]


    df_unit_definition_and_billings_and_contract_info_harmonized = df_unit_definition_and_billings_and_contract_info.merge(myac_translation.drop(columns = "number_of_occurences"), on = "title", how = "left")

else:
    df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]==""),"title"]=df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]==""),"packagename"]
    myac_translation = create_harmonization_table_for_myac_or_cpq_package_names(df=df_unit_definition_and_billings_and_contract_info.assign(packagename = lambda x: x["packagename"].astype(str)), unharmonized_name_column="packagename")
    df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]=="not defined"),"title"]=df_unit_definition_and_billings_and_contract_info.loc[lambda x: (x["title"]=="not defined"),"packagename"]
    
    df_unit_definition_and_billings_and_contract_info_harmonized = df_unit_definition_and_billings_and_contract_info.merge(myac_translation.drop(columns = "number_of_occurences"), on = "packagename", how = "left")

# take care of nans
df_unit_definition_and_billings_and_contract_info_harmonized = df_unit_definition_and_billings_and_contract_info_harmonized.assign(package_name_harmonized = lambda x: np.where(x["package_name_harmonized"].isna(), "not defined", x["package_name_harmonized"]))

# create overview datamodel
dm_myac_overview = (df_unit_definition_and_billings_and_contract_info_harmonized
                    [lambda x: (x["primarycontract"] == True) & (x["version"] == "OTR")]
                    .assign(flag_package_info_missing_myac = lambda x: np.where(x["billingtype"].isna(), True, False))
                    .rename(columns = {"serialnumber": "usn", "oraclecontractsnumber": "contract_number"})
                    [["opportunitynumber", "opportunityid", "contract_number", "contractid", "primarycontract", "usn", "unitdefinition_id", "version", "unitstartcounter", "unitendcounter", "customername", "flag_package_info_missing_myac"]]
                    .drop_duplicates()
)

# flag: units with multiple entries for the same package. THIS CAN BE VALID! In most cases, however, it seems to be a defect. Also included here: Packagename "not defined" --> Look into it, what it means
ids_with_duplications = df_unit_definition_and_billings_and_contract_info_harmonized[lambda x: x["billingtype"] == "PACKAGE"].groupby(["contractid", "serialnumber", "package_name_harmonized"]).agg(unitdefinition_id = ("unitdefinition_id", "unique"), number_entries = ("unitdefinition_id", "count")).reset_index()[lambda x: x["number_entries"] != 1].explode("unitdefinition_id")["unitdefinition_id"].unique()
dm_myac_overview = dm_myac_overview.assign(flag_duplications_on_packagelevel_valid_and_invalid = lambda x: np.where(x["unitdefinition_id"].isin(ids_with_duplications), True, False))


#### MERGE WITH ORACLE BACKBONE
dm_oracle_myac_prelim = (pd.merge(
    oracle_landscape_active_msa_formatted_ext,
    dm_myac_overview,
    on = ["contract_number", "usn"], 
    how = "outer")
    .fillna({"oracle_unit_status": "NOT ACTIVE", "version": "NOT OTR"}))
# here some duplications occur because of split unit start- and end-counters

# add flag
dm_oracle_myac_prelim = dm_oracle_myac_prelim.assign(flag_startcounter_mismatch_myac_oracle = lambda x: np.where(x["unitstartcounter"] == x["contract start oph oracle"], False, True))

##########################################################################################
# PREPARE MYPLANT-DATA
##########################################################################################

# get highlevel data for contract number and usn and esn
geo_loc_ib_metabase["oph_offset_engine_to_unit"]=geo_loc_ib_metabase["oph_offset_engine_to_unit"].fillna(0)
geo_loc_select = geo_loc_ib_metabase[["asset_id", "unit_serial_number", "serial_number","oph_offset_engine_to_unit"]].dropna().drop_duplicates()
# check if myplant data exists (comment = "None" also counts as myPlant data doesn't exist, but mainly the existence of the asset id in this list)
assets_with_sbom_data = sbom_nonsuperseded.groupby("asset_id")["comment"].unique().reset_index()[lambda x: (x["comment"].astype(str) != "[None]")]["asset_id"].unique()
# add flag
geo_loc_select_ext = geo_loc_select.assign(flag_package_info_missing_myplant = lambda x: np.where(x["asset_id"].isin(assets_with_sbom_data), False, True))

# create flag from datamodel: assets that have matching package-info in cpq and myplant AND WHERE CPQ IS NOT EMPTY
# last part is important because that bucket had been actively ignored in the myPlant validation (commit 325f330 in myPlantValidationModel (15.08.2023))
#Drop na for asset_id with NA (currently only 1 asset (18.12.23))
dm_packages_cpq_myplant_overview=dm_packages_cpq_myplant_overview.loc[lambda x: x["asset_id"].isna()==False,:]

assets_matching_cpq_myplant = dm_packages_cpq_myplant_overview[lambda x: (x["oracle_unit_status"] == "ACTIVE") & (x["event_defects_in_record"] == 0) & (x["package_info_present_cpq"] == True)]["asset_id"].astype(int).unique()

# create flag from datamodel: assets where cpq is empty (explanation see above)
assets_cpq_empty = dm_packages_cpq_myplant_overview[lambda x: (x["oracle_unit_status"] == "ACTIVE") & 
                                                    (x["package_info_present_cpq"] == False) 
                                                    ]["asset_id"].astype(int).unique()

# flag offset not zero
geo_loc_select_ext= geo_loc_select_ext.assign(flag_offset_not_zero = lambda x: np.where(x["oph_offset_engine_to_unit"]!=0, True, False))


# (x["contract_number"].isin(contract_number_migrated_sg)==False)
geo_loc_select_ext = geo_loc_select_ext.assign(
    flag_package_info_missing_cpq_ignored_before = lambda x: np.where(x["asset_id"].isin(assets_cpq_empty), True, False),
    flag_package_info_equal_in_myplant_and_cpq_ignored_before = lambda x: np.where(x["asset_id"].isin(assets_matching_cpq_myplant), True, False),
    
)

# create flag from validationlist: assets currently under review and assets validated/corrected
assets_validated_or_corrected = validationlist_packagecorrection[lambda x: ((x["status"].astype(str).str.lower().str.contains("validated")) | (x["status"].astype(str).str.lower().str.contains("corrected")))]["asset_id"].astype(int).unique()
assets_under_review = validationlist_packagecorrection[lambda x: ~((x["status"].astype(str).str.lower().str.contains("validated")) | (x["status"].astype(str).str.lower().str.contains("corrected")))]["asset_id"].astype(int).unique()
# add flags
geo_loc_select_ext = geo_loc_select_ext.assign(
    flag_unit_validated_or_corrected = lambda x: np.where(x["asset_id"].isin(assets_validated_or_corrected), True, False),
    flag_unit_under_review = lambda x: np.where(x["asset_id"].isin(assets_under_review), True, False)
)

#### MERGE WITH PRELIM DATAMODEL
dm_oracle_myac_myplant_prelim = pd.merge(
    dm_oracle_myac_prelim,
    geo_loc_select_ext,
    left_on = ["usn", "esn"],
    right_on = ["unit_serial_number", "serial_number"],
    how = "left"
)

#### MYPLANT SBOM EVENT MISMATCH
# which assets are not completely the same? - also includes inactive assets!
df_packages_events_sbom_mismatch = find_assets_with_mismatching_package_numbers_between_scheduled_events_and_partscope(dmp_events, sbom_nonsuperseded)
assets_with_mismatch_events_partscope = df_packages_events_sbom_mismatch["asset_id"].astype(int).unique()

#### ADD FLAG TO PRELIM DATAMODEL
dm_oracle_myac_myplant_prelim = (dm_oracle_myac_myplant_prelim
                                 .assign(flag_myplant_package_data_mismatch_present = lambda x: np.where(x["asset_id"].isin(assets_with_mismatch_events_partscope), True, False))
                                 )


##########################################################################################
# CREATE FLAGS AND OVERVIEW
##########################################################################################
## create flag for ab energy
ab_ids = dm_oracle_myac_myplant_prelim[lambda x: ((x["customername"].astype(str).str.upper().str.contains("AB ")==True) & ~(x["customername"].astype(str).str.upper().str.contains("UAB")) & ~(x["customername"].astype(str).str.upper().str.contains("FABRIK")) & ~(x["customername"].astype(str).str.lower().str.contains("abfall")) & ~(x["customername"].astype(str).str.lower().str.contains("abwasser")) & ~(x["customername"].astype(str).str.lower().str.contains("industriegebiet")) & ~(x["customername"].astype(str).str.lower().str.contains("recycling")) & ~(x["customername"].astype(str).str.lower().str.contains("vandselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("acatel")) & ~(x["customername"].astype(str).str.lower().str.contains("abelbaan")) & ~(x["customername"].astype(str).str.lower().str.contains("nocivelli")) & ~(x["customername"].astype(str).str.lower().str.contains("bernabeu")) & ~(x["customername"].astype(str).str.lower().str.contains("kraftvarmeselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("beltaine")) & ~(x["customername"].astype(str).str.lower().str.contains("energiselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("gabel"))& ~(x["customername"].astype(str).str.lower().str.contains("abelebaan")) & ~(x["customername"].astype(str).str.lower().str.contains("sabormex")) & ~(x["customername"].astype(str).str.lower().str.contains("fresenius")) & ~(x["customername"].astype(str).str.lower().str.contains("syvab"))) | ((x["customer name"].astype(str).str.upper().str.contains("AB")) & ~(x["customer name"].astype(str).str.upper().str.contains("UAB")) & ~(x["customer name"].astype(str).str.upper().str.contains("FABRIK")))]["unitdefinition_id"].unique()
ab_ids=ab_ids[np.isnan(ab_ids)==False]

ab_assets = dm_oracle_myac_myplant_prelim[lambda x: ((x["customername"].astype(str).str.upper().str.contains("AB ")==True) & ~(x["customername"].astype(str).str.upper().str.contains("UAB")) & ~(x["customername"].astype(str).str.upper().str.contains("FABRIK")) & ~(x["customername"].astype(str).str.lower().str.contains("abfall")) & ~(x["customername"].astype(str).str.lower().str.contains("abwasser")) & ~(x["customername"].astype(str).str.lower().str.contains("industriegebiet")) & ~(x["customername"].astype(str).str.lower().str.contains("recycling")) & ~(x["customername"].astype(str).str.lower().str.contains("vandselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("acatel")) & ~(x["customername"].astype(str).str.lower().str.contains("abelbaan")) & ~(x["customername"].astype(str).str.lower().str.contains("nocivelli")) & ~(x["customername"].astype(str).str.lower().str.contains("bernabeu")) & ~(x["customername"].astype(str).str.lower().str.contains("kraftvarmeselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("beltaine")) & ~(x["customername"].astype(str).str.lower().str.contains("energiselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("gabel")) & ~(x["customername"].astype(str).str.lower().str.contains("abelebaan")) & ~(x["customername"].astype(str).str.lower().str.contains("sabormex")) & ~(x["customername"].astype(str).str.lower().str.contains("fresenius"))& ~(x["customername"].astype(str).str.lower().str.contains("syvab"))) | ((x["customer name"].astype(str).str.upper().str.contains("AB")) & ~(x["customer name"].astype(str).str.upper().str.contains("UAB")) & ~(x["customer name"].astype(str).str.upper().str.contains("FABRIK")))]["asset_id"].unique()

ab_usns = dm_oracle_myac_myplant_prelim[lambda x: ((x["customername"].astype(str).str.upper().str.contains("AB ")==True) & ~(x["customername"].astype(str).str.upper().str.contains("UAB")) & ~(x["customername"].astype(str).str.upper().str.contains("FABRIK")) & ~(x["customername"].astype(str).str.lower().str.contains("abfall")) & ~(x["customername"].astype(str).str.lower().str.contains("abwasser")) & ~(x["customername"].astype(str).str.lower().str.contains("industriegebiet")) & ~(x["customername"].astype(str).str.lower().str.contains("recycling")) & ~(x["customername"].astype(str).str.lower().str.contains("vandselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("acatel")) & ~(x["customername"].astype(str).str.lower().str.contains("abelbaan")) & ~(x["customername"].astype(str).str.lower().str.contains("nocivelli")) & ~(x["customername"].astype(str).str.lower().str.contains("bernabeu")) & ~(x["customername"].astype(str).str.lower().str.contains("kraftvarmeselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("beltaine")) & ~(x["customername"].astype(str).str.lower().str.contains("energiselskab")) & ~(x["customername"].astype(str).str.lower().str.contains("gabel")) & ~(x["customername"].astype(str).str.lower().str.contains("abelebaan")) & ~(x["customername"].astype(str).str.lower().str.contains("sabormex")) & ~(x["customername"].astype(str).str.lower().str.contains("fresenius"))& ~(x["customername"].astype(str).str.lower().str.contains("syvab"))) | ((x["customer name"].astype(str).str.upper().str.contains("AB")) & ~(x["customer name"].astype(str).str.upper().str.contains("UAB")) & ~(x["customer name"].astype(str).str.upper().str.contains("FABRIK")))]["usn"].unique()

dm_oracle_myac_myplant_prelim = dm_oracle_myac_myplant_prelim.assign(flag_ab_energy_specific_format = lambda x: np.where((x["unitdefinition_id"].isin(ab_ids))| (x["usn"].isin(ab_usns)) | (x["asset_id"].isin(ab_assets)), True, False))


## create flag for units with multiple "other"-packages
otherpackages_myplant = sbom_nonsuperseded[["asset_id", "comment"]].drop_duplicates().dropna()[lambda x: x["comment"].str.lower().str.contains("other")].groupby("asset_id").agg(other_packages = ("comment", lambda x: set(x)), number_other_packages = ("comment", "nunique")).reset_index()[lambda x: x["number_other_packages"] != 1]
assetids_otherpackages = otherpackages_myplant["asset_id"].dropna().unique()
# add flag
dm_oracle_myac_myplant_prelim = dm_oracle_myac_myplant_prelim.assign(flag_multiple_other_packages_in_unit = lambda x: np.where(x["asset_id"].isin(assetids_otherpackages), True, False))


## create flag for units that have MORE individual packagenames in myPlant (--> no Price available)
# myac data
#packagenames_myac = df_unit_definition_and_billings_and_contract_info_harmonized.groupby(["contractid", "serialnumber"]).agg(unitdefinition_id = ("unitdefinition_id", "unique"), individual_packagenames_myac = ("package_name_harmonized", lambda x: set(x)), number_individual_packagenames_myac = ("package_name_harmonized", "nunique")).reset_index().explode("unitdefinition_id").merge(dm_oracle_myac_myplant_prelim[["unitdefinition_id", "asset_id"]].drop_duplicates(), on = "unitdefinition_id", how = "left")

packagenames_myac = df_unit_definition_and_billings_and_contract_info_harmonized.groupby(["contractid", "serialnumber","unitdefinition_id"]).agg(individual_packagenames_myac = ("package_name_harmonized", lambda x: set(x)), number_individual_packagenames_myac = ("package_name_harmonized", "nunique")).reset_index().explode("unitdefinition_id").merge(dm_oracle_myac_myplant_prelim[["unitdefinition_id", "asset_id"]].drop_duplicates(), on = "unitdefinition_id", how = "left")

# myplant data
packagenames_myplant = sbom_nonsuperseded[["asset_id", "comment"]].drop_duplicates().groupby("asset_id").agg(individual_packagenames_myplant = ("comment", lambda x: set(x)), number_individual_packagenames_myplant = ("comment", "nunique")).reset_index()#.merge(dm_oracle_myac_myplant_prelim[["unitdefinition_id", "asset_id"]].drop_duplicates(), on = "asset_id", how = "left")
# combine
packagenames_combined = pd.merge(packagenames_myac,packagenames_myplant,on = "asset_id",how = "inner")
# which units have more individual packagenames in myplant?
packagenames_combined = packagenames_combined.assign(packagenames_more_in_myplant = lambda x: x["individual_packagenames_myplant"] - x["individual_packagenames_myac"],
                                                     packagenames_more_in_myac = lambda x: x["individual_packagenames_myac"] - x["individual_packagenames_myplant"],
                                                     number_packagenames_more_in_myplant = lambda x: x["packagenames_more_in_myplant"].str.len())
unitdefinitionids_more_packages_in_myplant = packagenames_combined[lambda x: x["number_packagenames_more_in_myplant"] != 0]["unitdefinition_id"].dropna().unique()
# add flag
dm_oracle_myac_myplant_prelim = dm_oracle_myac_myplant_prelim.assign(flag_unmatched_packages_myplant_price_unknown = lambda x: 
                                                                     np.where(x["unitdefinition_id"].isin(unitdefinitionids_more_packages_in_myplant), True, False))

#add flag for singapure migrated contracts
dm_oracle_myac_myplant_prelim = dm_oracle_myac_myplant_prelim.assign(flag_migrated_to_SG = lambda x: 
                                                                     np.where(x["contract_number"].isin(contract_number_migrated_sg), True, False))

#add flag for intervals in myplant beyond unit end counter 

myp_intervals_wrong=myplant_myac_flag_intervals(geo_loc_ib_metabase, sbom_nonsuperseded, df_unit_definition_and_billings_and_contract_info,dm_oracle_myac_myplant_prelim)
dm_oracle_myac_myplant_prelim = dm_oracle_myac_myplant_prelim.assign(flag_interval_outside_unit_range = lambda x: 
                                                                     np.where(x["asset_id"].isin(myp_intervals_wrong), True, False))


#add flag for package type numbers 

unique_package_quantities=df_unit_definition_and_billings_and_contract_info_harmonized.groupby(["unitdefinition_id"]).aggregate({"package_name_harmonized":"count"}).reset_index()
unique_package_quantities["unitdefinition_id"]=unique_package_quantities["unitdefinition_id"].astype(float)
unique_package_quantities=unique_package_quantities.loc[lambda x: x["package_name_harmonized"]>1,:]

###
#High level statistics for reporting harmonization progress
###

level_0, level_1, level_2, level_3, level_4, level_5, level_overview=function_high_level_statistics(oracle_landscape_raw, ab_usns)


#################
###QTY_TYPE_MISMATCH by USN
#################

dm_package_quantity_and_type_by_usn_mismatch, dm_package_quantity_and_type_by_usn, df_qty_myac_unit_type, df_qty_myplant_unit_qty_type, dm_package_quantity_mismatch = qty_within_type_mismatch(df_unit_definition_and_billings_and_contract_info_harmonized, sbom_nonsuperseded, dm_oracle_myac_myplant_prelim)
dm_package_quantity_and_type_by_usn_mismatch=dm_package_quantity_and_type_by_usn_mismatch.loc[lambda x: x["usn"].isin(level_5)==True,:]

#################
###TYPE_MISMATCH by USN
#################

dm_package_type_by_usn=type_mismatch_export(dm_oracle_myac_myplant_prelim,df_qty_myac_unit_type,df_qty_myplant_unit_qty_type)


#Distinct types 

packagenames_combined['sym_diff_packagetypes'] = packagenames_combined.apply(lambda x: list(set(x['individual_packagenames_myplant']).symmetric_difference(set(x['individual_packagenames_myac']))), axis=1)

dm_package_type_mismatch=packagenames_combined.loc[lambda x: x["sym_diff_packagetypes"].str.len()!=0,:]

# add dataframe for latest type by system

## get entries that have already been migrated
# extract date and unitdefinition_ids for each migration file
migration_tracker = get_migrated_data_from_migration_folder(migration_folder=migration_folder)

# add flag for already migrated entries and migration date
dm_oracle_myac_myplant_prelim["migration_date"] = np.nan
dm_oracle_myac_myplant_prelim["flag_entry_already_migrated"] = False
for migration_date in migration_tracker.keys():
    current_ids = migration_tracker[migration_date]
    dm_oracle_myac_myplant_prelim = dm_oracle_myac_myplant_prelim.assign(flag_entry_already_migrated = lambda x: np.where(x["unitdefinition_id"].isin(current_ids), True, x["flag_entry_already_migrated"]),
                                                                         migration_date = lambda x: np.where(x["unitdefinition_id"].isin(current_ids), migration_date, x["migration_date"])
)
    

