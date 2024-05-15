





####
##MSA Execution KPIs
####

import datetime as dt
# Import Packages
import pandas as pd
import numpy as np
from datetime import date
import shutil
import xlsxwriter

# Import self-defined functions
from functions import *
from sklearn.inspection import PartialDependenceDisplay, partial_dependence
from sklearn.datasets import make_friedman1
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import GradientBoostingRegressor

from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from statsmodels.api import add_constant
import statsmodels.api as sm
import statsmodels.formula.api as smf 
from dateutil.relativedelta import relativedelta
from datetime import date

# show all columns
pd.set_option('display.max_columns', 999)
###
#Description: 
#This script generates KPIs for monitoring the execution of MSA contracts based on clusters
###1st: MSA fleet status
###2nd: MSA data quality status
###3rd: MSA ship and bill entitlement status
###4th: MSA ship and bill execution status

##########################################################################################
# IMPORT
##########################################################################################
conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")

oracle_landscape_raw = import_oracle_data_from_azure(conn)
#Load key reports 
#IB report 
ib_extended_report=import_ib_extended_from_azure(conn)
#myPlant - USN mapping 
geo_loc_ib_metabase = import_geo_loc(conn)
#myPlant events
dmp_events = import_dmp_events(conn)
#myPlant partscope 
sbom_nonsuperseded = import_sbom_nonsuperseded(conn)
#MYAC opportunities
opportunity_report_myac=get_opportunity_config(conn)

#######################
##1st: MSA fleet status
#######################

####
#Criteria: 
#Active MSAs: Contract Numbers with contract status active 
#Active Units: Oracle contract unit status active unit oks status ??? 
#Unit level execution: Manual list 
#Unit commissioned: Oracle unit IB status is "active" or "active-docu inoplete" or "temporarily inactive"
####

#######################
##EVALUATE TABLES
#######################

date_filter=date.today()
print(f"Date evaluated on: {date_filter}")   
msa_types_to_structure=["MSA BILLABLE SHIPPING","MSA USAGE BILLED","MSA PREVENTIVE AND CORRECTIVE"]
ib_status_selected=["Active","Standby","Active Docu incomplete", "Temporarily Inactive"]

df_export=pd.DataFrame()

for combination in itertools.product(msa_types_to_structure, ["unit serial - number only","contract number"]):
    df_0, df_1, df_2, df_3, df_4,overview = msa_fleet_status(oracle_landscape_raw, combination[1], [combination[0]],[], date_filter, ib_status_selected)
    overview
    df_export=pd.concat([df_export,overview], axis=0)     
    
    
#######################
##LOAD HISTORIC VALUES
#######################

historic_df_export=get_historic_values("overall_msa_execution_stats/stacked","appended_values")

#######################
##GENERATE OUTPUTS
#######################

df_export_appended=pd.concat([df_export,historic_df_export], axis=0)


writer = pd.ExcelWriter("msa_fleet_status/stacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export_appended, sheet_name="appended_values")

writer.close()

writer = pd.ExcelWriter("msa_fleet_status/unstacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export, sheet_name="appended_values")

writer.close()

writer = pd.ExcelWriter("msa_fleet_status/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export_appended, sheet_name="appended_values")

writer.close()


#######################
##2nd: MSA data quality status
#######################

####
#Criteria: 
# Units with outdated OPH counters (#), today minus month of last myPlant unit counter reading date > 6 //ib_extended_report
# Units beyond contract end date (#), today > MYAC unit end date // unit_definition_h
# Units outside contractual counter ranges (#), myPlant OPH counter > MYAC unit end date or myPlant OPH counter < MYAC unit start date // ib_extended & landscape_report
# Units with missing myPlant scopes (#), sum of myPlant packages = 0 // sbom_non_superseded & dmp_events
####

###Targetformat input_df_msa_data_quality
#### USN | ls: contract status | ls: country | ls: contract name | ls: customer name | 
#### ls: unit commissioning date | ls: engine commissioning date | ib: most_updated_unit_oph_counter_reading_date | 
#### ib: unit_contract_end_date | myac: unitstartcounter | myac: unitendcounter | ib: most_updated_unit_oph_counter_reading
#### myp_sbom: quantity | 



df_packages_events_sbom_myp=events_partscope_qty_myp(dmp_events, sbom_nonsuperseded)
df_packages_events_sbom_myp=df_packages_events_sbom_myp[["asset_id", "sum_zero_at_least_once","sum_zero_at_partscope"]].drop_duplicates()


msa_data_quality_backbone=gen_input_df_msa_data_quality(oracle_landscape_raw, ib_extended_report, geo_loc_ib_metabase, df_packages_events_sbom_myp, opportunity_report_myac)
active_unit_unit_level_usns_total,active_outdated_oph_counter,active_beyond_contract_end, active_outside_counter_range, active_missing_partscope, active_missing_partscope_or_event, df_steerco_overview_updated = msa_data_quality(msa_data_quality_backbone, "usn", ["MSA BILLABLE SHIPPING"],[], date.today(), ib_status_selected)



date_filter=str(date.today())

msa_types_to_structure=["MSA BILLABLE SHIPPING","MSA USAGE BILLED","MSA PREVENTIVE AND CORRECTIVE"]
df_export=pd.DataFrame()

for combination in itertools.product(msa_types_to_structure, ["usn","contract_number"]):
    df_0, df_1, df_2, df_3, df_4,df_5, overview = msa_data_quality(msa_data_quality_backbone, combination[1], [combination[0]],[], date.today())
    overview
    df_export=pd.concat([df_export,overview], axis=0)     
    
    
#######################
##LOAD HISTORIC VALUES
#######################

historic_df_export=get_historic_values("msa_data_quality_status/stacked","appended_values")

#######################
##GENERATE OUTPUTS
#######################

df_export_appended=pd.concat([df_export,historic_df_export], axis=0)


writer = pd.ExcelWriter("msa_data_quality_status/stacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export_appended, sheet_name="appended_values")

writer.close()

writer = pd.ExcelWriter("msa_data_quality_status/unstacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export, sheet_name="appended_values")

writer.close()

writer = pd.ExcelWriter("msa_data_quality_status/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export_appended, sheet_name="appended_values")

writer.close()



df_1,df_2,df_3, df_4, df_5, df_6, df_steerco_overview_updated = msa_data_quality(msa_data_quality_backbone, "usn", ["MSA BILLABLE SHIPPING"],[], date.today(), ib_status_selected)
df_steerco_overview_updated