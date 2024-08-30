





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

##########################################################################################
# IMPORT
##########################################################################################

conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")

oracle_landscape_raw = import_oracle_data_from_azure(conn)


df_mapping_sr=pd.read_excel("Costs_mapping_Myac_SR_Plan.xlsx")

#Parameters
catalogue="920 Activity Catalog JUN-2024"  #"920 Activity Catalog JUN-2024"  "920 Activity Catalog JUNE-2023"
period_actualization="2023-12-31"
year_actualization_onward=2023
sel_active_opp_version=True
sel_last_version=True
ib_status_selected=["Active","Standby","Active Docu incomplete", "Temporarily Inactive"]

#
#FOR CMR Review run this on friday and weekend (+retrieve Cottbus)
# 


oracle_landscape_raw_select=oracle_landscape_raw.copy()

#Filter only the latest contract end date oracle (also take into account old ORACLE contracts, relevant for past actuals)

grouped_max_start_date_oracle=oracle_landscape_raw_select.groupby(["unit serial - number only","contract status"]).aggregate({"contract end date oracle":"max"}).reset_index()
grouped_max_start_date_oracle=grouped_max_start_date_oracle.rename(columns={"contract end date oracle":"max_contract_end"})

oracle_landscape_raw_select=grouped_max_start_date_oracle.merge(oracle_landscape_raw_select,how="left",left_on=["unit serial - number only","contract status","max_contract_end"],
                                                                right_on=["unit serial - number only","contract status","contract end date oracle"])

grouped_max_header=oracle_landscape_raw_select.groupby(["unit serial - number only","contract status"]).aggregate({"latest header update":"max"}).reset_index()
grouped_max_header=grouped_max_header.rename(columns={"latest header update":"max_latest_date"})

oracle_landscape_raw_select=grouped_max_header.merge(oracle_landscape_raw_select,how="left",left_on=["unit serial - number only","contract status","max_latest_date"],
                                                                right_on=["unit serial - number only","contract status","latest header update"])



oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["contract name"]!="SCHEDULER",:]
oracle_landscape_raw_select_active_active=oracle_landscape_raw_select.loc[lambda x: (x["unit oks status"]=="ACTIVE")&(x["contract status"]=="ACTIVE"),:]
oracle_landscape_raw_select_active_terminated=oracle_landscape_raw_select.loc[lambda x: (x["unit oks status"]=="ACTIVE")&(x["contract status"]!="ACTIVE"),:]

oracle_landscape_raw_select=pd.concat([oracle_landscape_raw_select_active_active,oracle_landscape_raw_select_active_terminated],axis=0)

oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["contract type myac"].str.contains("CSA")==True,:]


#oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["unit serial number"].isin(filter_kevin_units)==True,:]
# oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["contract status"]=="ACTIVE",:]
oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["unit oks status"]=="ACTIVE",:]
#oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["unit oks status"]=="ACTIVE",:]



oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["unit status ib"].isin(ib_status_selected)==True,:]

grouped_expiration=oracle_landscape_raw_select.groupby(["unit serial - number only","contract status","contract number"]).aggregate({"contract name":"nunique"}).reset_index()
grouped_expiration_not_active=grouped_expiration.loc[lambda x: x["contract status"]!="ACTIVE","unit serial - number only"].unique()
grouped_expiration_active=grouped_expiration.loc[lambda x: x["contract status"]=="ACTIVE","unit serial - number only"].unique()

grouped_expiration=grouped_expiration.loc[lambda x: ((x["contract status"]=="ACTIVE")&(x["unit serial - number only"].isin(grouped_expiration_active)==True))|(((x["unit serial - number only"].isin(grouped_expiration_active)==False))&(x["unit serial - number only"].isin(grouped_expiration_not_active)==True)),:]


#
oracle_req_actuals=oracle_landscape_raw_select.loc[lambda x: (x["contract number"].isin(grouped_expiration["contract number"])==True),:]


date_today=str(date.today())

#FC Billings 2024
# df_power_query_allfinancials_billings=power_query_billings(conn)
# #df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]==catalogue)&(x["opportunity_version"]=="OTR")&
# #                                        (x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_last_version"]==sel_last_version)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>2023),:].groupby(["contract_number","opportunity_name_conf"]).aggregate({"billing_amount":"sum"}).to_excel("billing_2024" + date_today + ".xlsx")

# #df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]==catalogue)&(x["opportunity_version"]=="OTR")&
# #                                          (x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_last_version"]==sel_last_version)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>2023),:].groupby(["contract_number","opportunity_name_conf","unit_billing_type","unit_billing_date"]).aggregate({"billing_amount":"sum"}).reset_index().to_excel("fc_billing_2024_raw" + date_today + ".xlsx")


###
#Sequential access
###


#FC Cost Topsum 2024
df_cost_all_financials=power_query_allfinancials_usns_select(conn)
df_cost_all_financials["unit_period"]=df_cost_all_financials["unit_period"].fillna(pd.to_datetime(period_actualization))
#df_cost_all_financials.loc[lambda x: (x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==sel_last_version)&(x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index().to_excel("fc_cost_2024_granular" + date_today + ".xlsx")


#FC Cost by type 2024 

df_cost_fc_granular=get_financials_myac_cost_granular_by_opportunity_usns_select(conn)
#Filter for relevant version of cost catalogue 
df_cost_fc_granular_subset=df_cost_fc_granular.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_last_version"]==sel_last_version)&
                                                   (x["active_opportunity_version"]==sel_active_opp_version)&(x["primary_contract"]==True) ,:]


# df_cost_fc_granular_920=get_financials_myac_cost_granular_by_opportunity(conn)
# #Filter for reelevant entries 
# df_cost_fc_granular_subset_920=df_cost_fc_granular_920.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_last_version"]==sel_last_version)&
#                                                    (x["active_opportunity_version"]==sel_active_opp_version)&(x["primary_contract"]==True) ,:]


# df_cost_all_financials_920=power_query_allfinancials(conn)
# df_cost_all_financials_920["unit_period"]=df_cost_all_financials_920["unit_period"].fillna(pd.to_datetime(period_actualization))


df_cost_fc_granular_subset=df_cost_fc_granular_subset.loc[lambda x: x["schedule_date"].dt.year>=year_actualization_onward,:]
df_cost_fc_granular_subset["scope"]=df_cost_fc_granular_subset["scope"].fillna("None")
df_cost_fc_granular_subset["unit_type"]=df_cost_fc_granular_subset["unit_type"].fillna("None")
df_cost_fc_granular_subset["service"]=df_cost_fc_granular_subset["service"].fillna("None")
df_cost_fc_granular_subset["unit"]=df_cost_fc_granular_subset["unit"].fillna("None")

# #Returns granular unit cost 
# df_cost_fc_granular_subset.groupby(["opportunity_number","contract_number","unit_catalog_version",'scope', 'service', 'unit_type','schedule_date']).aggregate({"value":"sum","cost":"sum","ic_cost":"sum"}).reset_index().to_excel("fc_unit_level_cost_2024_granular" + date_today + ".xlsx")

#Confirm that only site level is missing! 

#FC 2024 Site Level
#df_cost_all_financials.loc[lambda x: (x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==sel_last_version)&(x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf"]).aggregate({"cost":"mean","billings_consid_ldb":"mean"}).reset_index().to_excel("fc_site_level_cost_2024_granular" + date_today + ".xlsx")


#FC_24_harmonized

df_cost_24_site_harmonized=df_cost_all_financials.loc[lambda x: (x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==sel_last_version)&(x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number"]).aggregate({"cost":"mean","unit_period":"nunique"}).reset_index()
df_cost_24_site_harmonized["scope"]="SITE"
df_cost_24_site_harmonized["service"]="None"
df_cost_24_site_harmonized["unit"]="None"
df_cost_24_site_harmonized["billing_type"]="None"
df_cost_24_site_harmonized["frequency"]="None"
df_cost_24_site_harmonized["unit_serial_number"]="None"
df_cost_24_site_harmonized=df_cost_24_site_harmonized.rename(columns={"unit_period":"occurrence"})


df_cost_24_unit_harmonized=df_cost_fc_granular_subset.groupby(["opportunity_number","unit_serial_number",'scope', 'service', 'unit_type','unit',"frequency"]).aggregate({"cost":"mean","schedule_date":"nunique"}).reset_index()

df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"unit_type":"billing_type"})
df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"schedule_date":"occurrence"})

#

#Billigsharmoized

# df_billings_fc_24_harmonized=df_power_query_allfinancials_billings.loc[lambda x: (x["opportunity_version"]=="OTR")&
#                                           (x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_last_version"]==sel_last_version)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>=year_actualization_onward),:].groupby(["opportunity_number","contract_number","unit_serial_number","opportunity_name_conf","unit_billing_type"]).aggregate({"billing_amount":"mean"}).reset_index()
# #Filter out old MERHEIM 
# df_billings_fc_24_harmonized=df_billings_fc_24_harmonized.loc[lambda x: x["opportunity_name_conf"]!="MYA HKW Merheim RheinEnergie 3xJ920  Y647 ",:]

# df_billings_fc_24_harmonized=df_billings_fc_24_harmonized.rename(columns={"unit_billing_type":"billing_type","billing_amount":"billings"})


# ###
# #Output of haronized files 
# ###

# df_fc_harmonized_billings_to_use=pd.concat([df_billings_fc_24_harmonized], axis=0)
df_fc_harmonized_costs_to_use=pd.concat([df_cost_24_unit_harmonized,df_cost_24_site_harmonized], axis=0)

#Harmonize Opportunitynumbers 
# df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="934650","opportunity_number"]="0934650"
# df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="967118","opportunity_number"]="0967118"
# df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="992496","opportunity_number"]="0992496"

df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="934650","opportunity_number"]="0934650"
df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="967118","opportunity_number"]="0967118"
df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="992496","opportunity_number"]="0992496"


##
#CALCULATION OF WEIGHTED AVERAGES 
##

#Steps: 
#-Filter out unplanned 
#-Calculated total contract value of plan costs
#-Group

df_fc_harmonized_costs_to_use_export=df_fc_harmonized_costs_to_use.loc[lambda x: x["service"].str.contains("Unplanned")==False,:]
df_fc_harmonized_costs_to_use_export["cost_occurrence_product"]=df_fc_harmonized_costs_to_use_export["cost"]*df_fc_harmonized_costs_to_use_export["occurrence"]

df_fc_harmonized_costs_to_use_export_grouped=df_fc_harmonized_costs_to_use_export.groupby(["opportunity_number","unit_serial_number","billing_type","frequency"]).aggregate({"cost_occurrence_product":"sum","occurrence":"sum"}).reset_index()
df_fc_harmonized_costs_to_use_export_grouped["cost_weighted"]=df_fc_harmonized_costs_to_use_export_grouped["cost_occurrence_product"]/df_fc_harmonized_costs_to_use_export_grouped["occurrence"]


# filter_kevin_units=["4749102"
# ,"GEJ-1030121"
# ,"GEJ-1115967"
# ,"GEJ-1075596"
# ,"JEN-1441297"
# ,"JEN-1457743"
# ,"GEJ-1195711"
# ,"4467761"
# ,"3441541"
# ,"GEJ-1084564"]

#Include contract and usn in format 

df_fc_harmonized_costs_to_use_export_grouped=df_fc_harmonized_costs_to_use_export_grouped.merge(oracle_landscape_raw_select[["contract number","unit serial number","unit serial - number only"]],how="left",
                                                                                                left_on=["unit_serial_number"],
                                                                                                right_on=["unit serial - number only"])

#Filter Kevins units 


df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped.loc[lambda x: (x["contract number"].isin(grouped_expiration["contract number"])==True),:]   ####or: &(x["unit serial number"].isin(filter_kevin_units)==True)

df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped_output[["opportunity_number","unit serial number","contract number","billing_type","frequency","cost_weighted","occurrence"]]


df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped_output.merge(df_mapping_sr, how="left",on="frequency")

df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped_output[["opportunity_number","unit serial number","contract number","billing_type","frequency","sr_type_mapped","cost_weighted","occurrence"]]


df_fc_harmonized_costs_to_use_export_grouped_output["cost_tc"]=df_fc_harmonized_costs_to_use_export_grouped_output["cost_weighted"]*df_fc_harmonized_costs_to_use_export_grouped_output["occurrence"]
df_fc_outputs_grouped=df_fc_harmonized_costs_to_use_export_grouped_output.groupby(["opportunity_number","unit serial number","contract number",
                                                                                   "billing_type","sr_type_mapped"]).aggregate({"cost_tc":"sum","occurrence":"sum"})

df_fc_outputs_grouped=df_fc_outputs_grouped.reset_index()
df_fc_outputs_grouped["cost_weighted"]=df_fc_outputs_grouped["cost_tc"]/df_fc_outputs_grouped["occurrence"]




df_fc_harmonized_costs_to_use_export_grouped_output.to_excel("output_costs_export_sample_all_yearss_" + date_today + ".xlsx")

df_fc_outputs_grouped.to_excel("grouped_sample_sr_mapping_" + date_today + ".xlsx")


# ##
# #Comparing for selective contracts positions in FC
# ##
# sel_opportunity="1335920"
# #Check service or check scope billing_type
# df_cost_24_unit_harmonized.loc[lambda x: (x["schedule_year"]>2023)&(x["opportunity_number"]==sel_opportunity)&(x["scope"]=="Gen Set_Engine")&(x["billing_type"]=="INNIO_PARTS"),:].groupby(["opportunity_number","service"]).aggregate({"cost":"sum"}).sort_values(by="cost",ascending=False)

df_fc_harmonized_costs_to_use.groupby(["opportunity_number"]).aggregate({"cost":"sum"})


df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="0992496",:].groupby(["opportunity_number","scope","service","billing_type"]).aggregate({"cost":"mean"})

df_fc_harmonized_costs_to_use.loc[lambda x: (x["scope"]=="Exhaust gas heat exchanger")&(x["opportunity_number"]=="0992496"),:].head()