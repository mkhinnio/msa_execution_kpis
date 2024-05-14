





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


#######################
##1st: MSA fleet status
#######################

####
#Criteria: 
##Active MSAs: Contract Numbers with contract status active 
##Active Units: Oracle contract unit status active unit oks status ??? 
##Unit level execution: Manual list 
##Unit commissioned: Oracle unit IB status is "active" or "active-docu inoplete" or "temporarily inactive"
####

#######################
##1.1: All MSAs
#######################
today=date.today()
today=str(today)
iterations_months=[el for el in range(0,24)]
df_export=pd.DataFrame()
for it in iterations_months:
    date_filter=date.today()- relativedelta(months=(24-it))
    print(f"Date evaluated on: {date_filter}")   
    msa_types_to_structure=["MSA BILLABLE SHIPPING","MSA USAGE BILLED","MSA PREVENTIVE AND CORRECTIVE"]
    ib_types=["Active","Temporarily Inactive","Active Docu incomplete"]
    active_msas_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True),"contract number"].unique()
    active_unit_oks_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True),"unit serial - number only"].unique()
    active_unit_not_unit_level_usns_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True)&(x["unit serial - number only"].isin(not_unit_level_usns)==True),"unit serial - number only"].unique()

    active_unit_ib_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True)&(x["unit status ib"].isin(ib_types)==True),"unit serial - number only"].unique()

    active_msas_to_report=[]
    active_msas_units_oks_to_report=[]
    active_msas_units_not_executed_unit_level_to_report=[]
    active_msas_units_oks_and_ib_active_to_report=[]

    for combination in itertools.product(msa_types_to_structure, ["unit serial - number only","contract number"]):
        df_0, df_1, df_2, df_3, df_4,overview = harmonization_figures_total_waterfall(oracle_landscape_raw, combination[1], [combination[0]],[], date_filter)
        overview
        df_export=pd.concat([df_export,overview], axis=0)     
        
        
    #######################
    ##LOAD HISTORIC VALUES
    #######################

    historic_df_export=get_historic_values("msa_fleet_status/archive","appended_values")
    print(len(historic_df_export))
    #######################
    ##APPEND HISTORIC VALUES
    #######################

    df_export_appended=pd.concat([df_export,historic_df_export], axis=0)
    writer = pd.ExcelWriter("msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
    create_excel_table_for_data_table(writer=writer, df=df_export, sheet_name="current_values")
    
    writer.close()

    writer = pd.ExcelWriter("msa_fleet_status/archive/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
    create_excel_table_for_data_table(writer=writer, df=df_export_appended, sheet_name="appended_values")
    
    writer.close()
    
    
    
    
    
    
    
    
    
#Exemptions not_unit_level_execution
not_unit_level_executed_customers=["INDUSTRIAS JUAN F SECCO SA","GREENERGY","BREITENER"]
not_unit_level_executed_contract_name=["infinis"]
not_unit_level_executed_installed_at_country=["bangladesh"]
not_unit_level_usns=oracle_landscape_raw.loc[lambda x: (x["customer name"].str.upper().str.contains("|".join(not_unit_level_executed_customers))==True)|(x["contract name"].str.lower().str.contains("|".join(not_unit_level_executed_contract_name))==True)|(x["installed at country"].str.lower().str.contains("|".join(not_unit_level_executed_installed_at_country))==True),"unit serial - number only"].unique()

msa_types_to_structure=["MSA BILLABLE SHIPPING","MSA USAGE BILLED","MSA PREVENTIVE AND CORRECTIVE"]
ib_types=["Active","Temporarily Inactive","Active Docu incomplete"]
active_msas_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True),"contract number"].unique()
active_unit_oks_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True),"unit serial - number only"].unique()
active_unit_not_unit_level_usns_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True)&(x["unit serial - number only"].isin(not_unit_level_usns)==True),"unit serial - number only"].unique()

active_unit_ib_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"].isin(msa_types_to_structure)==True)&(x["unit status ib"].isin(ib_types)==True),"unit serial - number only"].unique()

active_msas_to_report=[]
active_msas_units_oks_to_report=[]
active_msas_units_not_executed_unit_level_to_report=[]
active_msas_units_oks_and_ib_active_to_report=[]

for el in msa_types_to_structure:
    active_msas_to_report.append(oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["contract type oracle"]==el),"contract number"].nunique())
    active_msas_units_oks_to_report.append(oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"]==el),"unit serial - number only"].nunique())
    active_msas_units_not_executed_unit_level_to_report.append(oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"]==el)&(x["unit serial - number only"].isin(not_unit_level_usns)==True),"unit serial - number only"].nunique())
    active_msas_units_oks_and_ib_active_to_report.append(oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["contract type oracle"]==el)&(x["unit status ib"].isin(ib_types)==True),"unit serial - number only"].nunique())

#Add totals
active_msas_to_report.append(len(active_msas_total))
active_msas_units_oks_to_report.append(len(active_unit_oks_total))
active_msas_units_not_executed_unit_level_to_report.append(len(active_unit_not_unit_level_usns_total))
active_msas_units_oks_and_ib_active_to_report.append(len(active_unit_ib_total))
#Add KPIs
active_msas_to_report.append("Active MSAs")
active_msas_units_oks_to_report.append("Active Units under MSAs")
active_msas_units_not_executed_unit_level_to_report.append("Active Units under MSAs w/o unit-level execution")
active_msas_units_oks_and_ib_active_to_report.append("Active Units commissioned")

#Combine metadata
active_msas_to_report_meta=["Active MSAs"]
active_msas_units_oks_to_report_meta=["Active Units under MSAs"]
active_msas_units_not_executed_unit_level_to_report_meta=["Active Units under MSAs w/o unit-level execution"]
active_msas_units_oks_and_ib_active_to_report_meta=["Active Units commissioned"]

active_msas_to_report_meta.append(["Contract status:"]+ ["ACTIVE"])
active_msas_units_oks_to_report_meta.append(["Contract status:, unit oks status:"] + ["ACTIVE","ACTIVE"])
active_msas_units_not_executed_unit_level_to_report_meta.append(["Contract status:, unit oks status:, unit_in_flag_bucket: "]+["ACTIVE","ACTIVE",not_unit_level_executed_customers,not_unit_level_executed_contract_name,not_unit_level_executed_installed_at_country])
active_msas_units_oks_and_ib_active_to_report_meta.append(["Contract status:, unit oks status:, unit ib status: "]+["ACTIVE","ACTIVE",ib_types])

df_meta_data_export=pd.DataFrame([active_msas_to_report_meta,active_msas_units_oks_to_report_meta,
                                active_msas_units_not_executed_unit_level_to_report_meta,
                                active_msas_units_oks_and_ib_active_to_report_meta])
df_meta_data_export.columns=["KPI"]+["OPERATIONALIZATION"]
df_meta_data_export["TIMESTAMP"]=date_filter

#####
#COMBINE TO DATAFRAME
#####

#Current export 
df_export=pd.DataFrame([active_msas_to_report,active_msas_units_oks_to_report,
                    active_msas_units_not_executed_unit_level_to_report,
                    active_msas_units_oks_and_ib_active_to_report])
df_export.columns=msa_types_to_structure+["TOTAL"]+["KPI"]
df_export["TIMESTAMP"]=date_filter

#######################
##LOAD HISTORIC VALUES
#######################

historic_df_export=get_historic_values("msa_fleet_status/archive","appended_values")
historic_df_meta_export=get_historic_values("msa_fleet_status/archive","meta_values")
print(len(historic_df_export))
#######################
##APPEND HISTORIC VALUES
#######################

df_export_appended=pd.concat([df_export,historic_df_export], axis=0)
df_meta_export_appended=pd.concat([df_meta_data_export,historic_df_meta_export], axis=0)

writer = pd.ExcelWriter("msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export, sheet_name="current_values")
create_excel_table_for_data_table(writer=writer, df=df_meta_data_export, sheet_name="meta_values")
writer.close()

writer = pd.ExcelWriter("msa_fleet_status/archive/msa_execution_kpis_" + str(date_filter) + ".xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_export_appended, sheet_name="appended_values")
create_excel_table_for_data_table(writer=writer, df=df_meta_export_appended, sheet_name="meta_values")
writer.close()


###
#Calculation example
###

ab_usns = oracle_landscape_raw[lambda x: ((x["customer name"].astype(str).str.upper().str.contains("AB ")==True) & ~(x["customer name"].astype(str).str.upper().str.contains("UAB")) & ~(x["customer name"].astype(str).str.upper().str.contains("FABRIK")) & ~(x["customer name"].astype(str).str.lower().str.contains("abfall")) & ~(x["customer name"].astype(str).str.lower().str.contains("abwasser")) & ~(x["customer name"].astype(str).str.lower().str.contains("industriegebiet")) & ~(x["customer name"].astype(str).str.lower().str.contains("recycling")) & ~(x["customer name"].astype(str).str.lower().str.contains("vandselskab")) & ~(x["customer name"].astype(str).str.lower().str.contains("acatel")) & ~(x["customer name"].astype(str).str.lower().str.contains("abelbaan")) & ~(x["customer name"].astype(str).str.lower().str.contains("nocivelli")) & ~(x["customer name"].astype(str).str.lower().str.contains("bernabeu")) & ~(x["customer name"].astype(str).str.lower().str.contains("kraftvarmeselskab")) & ~(x["customer name"].astype(str).str.lower().str.contains("beltaine")) & ~(x["customer name"].astype(str).str.lower().str.contains("energiselskab")) & ~(x["customer name"].astype(str).str.lower().str.contains("gabel")) & ~(x["customer name"].astype(str).str.lower().str.contains("abelebaan")) & ~(x["customer name"].astype(str).str.lower().str.contains("sabormex")) & ~(x["customer name"].astype(str).str.lower().str.contains("fresenius"))& ~(x["customer name"].astype(str).str.lower().str.contains("syvab"))) | ((x["customer name"].astype(str).str.upper().str.contains("AB")) & ~(x["customer name"].astype(str).str.upper().str.contains("UAB")) & ~(x["customer name"].astype(str).str.upper().str.contains("FABRIK")))]["unit serial - number only"].unique()


df_0, df_1, df_2, df_3, df_4,df_5, overview = harmonization_figures_total_waterfall(oracle_landscape_raw, ab_usns, "unit serial - number only", ['MSA BILLABLE SHIPPING'],["MSA_PREVENTIVE","MSA_PREVENTIVE_AND_CORRECTIVE"])
overview

df_0, df_1, df_2, df_3 ,df_4, overview = harmonization_figures_total_waterfall(oracle_landscape_raw, "contract number", ['MSA BILLABLE SHIPPING'],["MSA_PREVENTIVE","MSA_PREVENTIVE_AND_CORRECTIVE"])
overview


df_0, df_1, df_2, df_3 ,df_4, overview = harmonization_figures_total_waterfall(oracle_landscape_raw, "unit serial - number only", ['MSA BILLABLE SHIPPING'],["MSA_PREVENTIVE","MSA_PREVENTIVE_AND_CORRECTIVE"], date_filter)
overview

df_0, df_1, df_2, df_3, df_4,overview = harmonization_figures_total_waterfall(oracle_landscape_raw, "contract number", ['MSA BILLABLE SHIPPING'],[], date_filter)
overview

df_0, df_1, df_2, df_3, df_4,df_5, overview = harmonization_figures_total_waterfall(oracle_landscape_raw, ab_usns, "contract number", ["PREVENTIVE","PREVENTIVE AND CORRECTIVE"],["CSA_PREVENTIVE","CSA_PREVENTIVE_AND_CORRECTIVE"])
overview


