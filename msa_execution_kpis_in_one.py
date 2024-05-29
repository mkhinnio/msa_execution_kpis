





####
##MSA Execution KPIs
####

###
#Author: Martin Kuehner 

import datetime as dt
# Import Packages
import pandas as pd
import numpy as np
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

oracle_landscape_raw = import_oracle_data_from_azure(conn)
oracle_landscape_select = oracle_landscape_raw.rename(columns={"unit serial - number only":"usn","contract number":"contract_number"})

#Parameter
msa_types_to_structure=["MSA BILLABLE SHIPPING","MSA USAGE BILLED","MSA PREVENTIVE AND CORRECTIVE"]
ib_status_selected=["Active","Standby","Active Docu incomplete", "Temporarily Inactive"]


##########################################################################################
# PREPARATION
##########################################################################################

#######################
##1st: MSA fleet status / and also 2nd MSA Data Quality status
#######################

####
#Criteria: 
####

# 1. Oracle contract status "active" + contract type "MSA Usage"
# 2. Oracle contract unit status "active"
# 3. Manually maintained list to filter out customers (e.g. SECCO)
# 4. Oracle unit IB status "active" or "active - docu incomplete" or "temporarily inactive"
####

#Only works with landscape_select

########################
#2nd: Data quality status
######################## 

#Check for sbom events mismatch myp - currently not used in dashboard logic
df_packages_events_sbom_myp=events_partscope_qty_myp(dmp_events, sbom_nonsuperseded)
#
msa_data_quality_backbone=gen_input_df_msa_data_quality(oracle_landscape_select, ib_extended_report, geo_loc_ib_metabase, df_packages_events_sbom_myp, opportunity_report_myac)

####
#Criteria:
####
 
# For all units under 4.: today minus month of last myPlant unit counter reading date > 6
# For all units under 4.: today > MYAC unit end date
# For all units under 4: myPlant OPH counter > MYAC unit end date or myPlant OPH counter < MYAC unit start date
# For all units under 4.: sum of myPlant packages = 0
####

# active_unit_unit_level_usns_total,active_outdated_oph_counter,active_beyond_contract_end, active_outside_counter_range, active_missing_partscope, active_missing_partscope_or_event, df_steerco_overview_updated = msa_data_quality(msa_data_quality_backbone, "usn", ["MSA BILLABLE SHIPPING"],[], date.today(), ib_status_selected)

#######################
# INITIALIZE TABLES
#######################

date_filter=date.today()

print(f"Date evaluated on: {date_filter}")   

main_path="overall_msa_execution_stats"

df_export_fleet_status=pd.DataFrame()
df_export_data_quality=pd.DataFrame()
df_export_fleet_status_details=pd.DataFrame()
df_export_data_quality_details=pd.DataFrame()

#######################
# GENERATE CURRENT STATUS SUMMARY
#######################

for combination in itertools.product(msa_types_to_structure, ["usn","contract_number"]):
    #Fleet Status
    df_0, df_1, df_2, df_3, df_4,overview, details_df_fleet_status = msa_fleet_status(oracle_landscape_select, combination[1], [combination[0]],[], date_filter, ib_status_selected, ib_extended_report)
    df_export_fleet_status=pd.concat([df_export_fleet_status,overview], axis=0)     
    df_export_fleet_status_details=pd.concat([df_export_fleet_status_details,details_df_fleet_status], axis=0)  
    #Data Quality
    df_0, df_1, df_2, df_3, df_4,df_5, overview, details_df_data_quality = msa_data_quality(msa_data_quality_backbone, combination[1], [combination[0]],[], date_filter, ib_status_selected, ib_extended_report)
    df_export_data_quality=pd.concat([df_export_data_quality,overview], axis=0)
    df_export_data_quality_details=pd.concat([df_export_data_quality_details,details_df_data_quality], axis=0) 

#######################
# LOAD HISTORIC VALUES
#######################

historic_fleet_status=get_historic_values("overall_msa_execution_stats/stacked","fleet_status")
historic_quality_status=get_historic_values("overall_msa_execution_stats/stacked","quality_status")

#######################
# GENERATE OUTPUTS
#######################

df_fleet_status_appended=pd.concat([df_export_fleet_status,historic_fleet_status], axis=0)
df_data_quality_appended=pd.concat([df_export_data_quality,historic_quality_status], axis=0)


writer = pd.ExcelWriter(main_path+ "/stacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_fleet_status_appended, sheet_name="fleet_status")
create_excel_table_for_data_table(writer=writer, df=df_data_quality_appended, sheet_name="quality_status")
create_excel_table_for_data_table(writer=writer, df=df_export_fleet_status_details, sheet_name="fleet_status - details")
create_excel_table_for_data_table(writer=writer, df=df_export_data_quality_details, sheet_name="quality_status - details")
writer.close()

writer = pd.ExcelWriter(main_path+ "/unstacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export_fleet_status, sheet_name="fleet_status")
create_excel_table_for_data_table(writer=writer, df=df_export_data_quality, sheet_name="quality_status")
create_excel_table_for_data_table(writer=writer, df=df_export_fleet_status_details, sheet_name="fleet_status - details")
create_excel_table_for_data_table(writer=writer, df=df_export_data_quality_details, sheet_name="quality_status - details")
writer.close()

writer = pd.ExcelWriter(main_path+ "/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_fleet_status_appended, sheet_name="fleet_status")
create_excel_table_for_data_table(writer=writer, df=df_data_quality_appended, sheet_name="quality_status")
create_excel_table_for_data_table(writer=writer, df=df_export_fleet_status_details, sheet_name="fleet_status - details")
create_excel_table_for_data_table(writer=writer, df=df_export_data_quality_details, sheet_name="quality_status - details")
writer.close()


#######################
##EXAMPLE TEST
#######################

######
# df_1,df_2,df_3, df_4, df_5, df_6, df_steerco_overview_updated = msa_data_quality(msa_data_quality_backbone, "usn", ["MSA BILLABLE SHIPPING"],[], date.today(), ib_status_selected)
# df_steerco_overview_updated



####
##Test
###


# today=date.today()
# today=str(today)
# iterations_months=[el for el in range(0,25)]

# for it in iterations_months:
#     df_export=pd.DataFrame()
#     date_filter=date.today()- relativedelta(months=(24-it))


#     print(f"Date evaluated on: {date_filter}")   
#     msa_types_to_structure=["MSA BILLABLE SHIPPING","MSA USAGE BILLED","MSA PREVENTIVE AND CORRECTIVE"]
#     ib_status_selected=["Active","Standby","Active Docu incomplete", "Temporarily Inactive"]

#     main_path="overall_msa_execution_stats"

#     df_export_fleet_status=pd.DataFrame()
#     df_export_data_quality=pd.DataFrame()

#     for combination in itertools.product(msa_types_to_structure, ["usn","contract_number"]):
#         df_0, df_1, df_2, df_3, df_4,overview = msa_fleet_status(oracle_landscape_select, combination[1], [combination[0]],[], date_filter, ib_status_selected)
#         df_export_fleet_status=pd.concat([df_export_fleet_status,overview], axis=0)     
#         df_0, df_1, df_2, df_3, df_4,df_5, overview = msa_data_quality(msa_data_quality_backbone, combination[1], [combination[0]],[], date_filter, ib_status_selected)
#         df_export_data_quality=pd.concat([df_export_data_quality,overview], axis=0)
        
#     #######################
#     ##LOAD HISTORIC VALUES
#     #######################

#     historic_fleet_status=get_historic_values("overall_msa_execution_stats/stacked","fleet_status")
#     historic_quality_status=get_historic_values("overall_msa_execution_stats/stacked","quality_status")

#     #######################
#     ##GENERATE OUTPUTS
#     #######################

#     df_fleet_status_appended=pd.concat([df_export_fleet_status,historic_fleet_status], axis=0)
#     df_data_quality_appended=pd.concat([df_export_data_quality,historic_quality_status], axis=0)


#     writer = pd.ExcelWriter(main_path+ "/stacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
#     create_excel_table_for_data_table(writer=writer, df=df_fleet_status_appended, sheet_name="fleet_status")
#     create_excel_table_for_data_table(writer=writer, df=df_data_quality_appended, sheet_name="quality_status")
#     writer.close()

#     writer = pd.ExcelWriter(main_path+ "/unstacked/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
#     create_excel_table_for_data_table(writer=writer, df=df_export_fleet_status, sheet_name="fleet_status")
#     create_excel_table_for_data_table(writer=writer, df=df_data_quality_appended, sheet_name="quality_status")
#     writer.close()

#     writer = pd.ExcelWriter(main_path+ "/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
#     create_excel_table_for_data_table(writer=writer, df=df_fleet_status_appended, sheet_name="fleet_status")
#     create_excel_table_for_data_table(writer=writer, df=df_data_quality_appended, sheet_name="quality_status")
#     writer.close()
