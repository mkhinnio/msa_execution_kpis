





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

##########################################################################################
# IMPORT
##########################################################################################
conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")


###
#FC 23 - Old MYA-C 
###

#
#FOR CMR Review run this on friday and weekend (+retrieve Cottbus)
# 

date_today=str(date.today())

#FC Billings 2024

##
#Parameters
##

catalogue=["920 Activity Catalog JUNE-2023"]
period_actualization="2022-12-31"
year_actualization_onward=2022
sel_active_opp_version=False
sel_last_version=False
#FC Cost Topsum 2024
df_all_costs_for_site=power_query_allfinancials(conn)
df_all_costs_for_site["unit_period"]=df_all_costs_for_site["unit_period"].fillna(pd.to_datetime(period_actualization))
df_all_costs_for_site["unit_serial_number"]=df_all_costs_for_site["unit_serial_number"].fillna("None")


#FC Cost by type 2024 

df_cost_fc_granular=get_financials_myac_cost_granular_by_opportunity(conn) #get_financials_myac_cost_granular_by_opportunity_csa
#Filter for reelevant entries 
df_cost_fc_granular["etl_hash_pk"]="None"
df_cost_fc_granular["contract_modification_date"]="None"

df_cost_fc_granular=df_cost_fc_granular.drop_duplicates()
#"CSA Activity Catalog MAY-2023","920 Activity Catalog JUNE-2023"
df_cost_fc_granular_subset=df_cost_fc_granular.loc[lambda x: (x["primary_contract"]==True)&(x["opportunity_version"]=="OTR")&(x["unit_catalog_version"].isin(catalogue)==True)&(x["active_opportunity_version"]==sel_active_opp_version),:].drop(["etl_hash_pk"],axis=1).drop_duplicates()


df_cost_fc_granular_subset=df_cost_fc_granular_subset.loc[lambda x: x["schedule_date"].dt.year>=year_actualization_onward,:]
df_cost_fc_granular_subset["scope"]=df_cost_fc_granular_subset["scope"].fillna("None")
df_cost_fc_granular_subset["unit_type"]=df_cost_fc_granular_subset["unit_type"].fillna("None")
df_cost_fc_granular_subset["service"]=df_cost_fc_granular_subset["service"].fillna("None")
df_cost_fc_granular_subset["unit"]=df_cost_fc_granular_subset["unit"].fillna("None")
df_cost_fc_granular_subset["schedule_date"]=df_cost_fc_granular_subset["schedule_date"].fillna("None")

#Returns granular unit cost 
#df_cost_fc_granular_subset.groupby(["opportunity_number","contract_number","unit_serial_number","unit_catalog_version",'scope', 'service', 'unit_type',"frequency','schedule_date']).aggregate({"value":"sum","cost":"sum","ic_cost":"sum"}).reset_index().to_excel("fc_unit_level_cost_2024_granular" + date_today + ".xlsx")

#Confirm that only site level is missing! 

#FC 2024 Site Level
# df_all_costs_for_site.loc[lambda x: (x["primary_contract"]==True)&(x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index().to_excel("fc_site_level_cost_2024_granular" + date_today + ".xlsx")



###
#FC 2023 Numbers

#FC_24_harmonized

df_all_costs_for_site_earliest=df_all_costs_for_site.groupby(["opportunity_number_conf","contract_number"]).aggregate({"contract_modification_date":"min"}).reset_index().rename(columns={"contract_modification_date":"max_cont_mod_date"})
df_all_costs_for_site=df_all_costs_for_site.merge(df_all_costs_for_site_earliest,how="left",on=["opportunity_number_conf","contract_number"])

df_all_costs_for_site=df_all_costs_for_site.loc[lambda x: x["contract_modification_date"]==x["max_cont_mod_date"],:]

df_cost_24_site_harmonized=df_all_costs_for_site.loc[lambda x: (x["primary_contract"]==True)&(x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==sel_last_version)&(x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_version"]=="OTR"),:].drop_duplicates().groupby(["opportunity_number","contract_number","unit_serial_number","contract_type","unit_period"]).aggregate({"cost":"sum"}).reset_index()
df_cost_24_site_harmonized["scope"]="SITE"
df_cost_24_site_harmonized["service"]="None"
df_cost_24_site_harmonized["unit"]="None"
df_cost_24_site_harmonized["billing_type"]="None"
df_cost_24_site_harmonized["frequency"]="None"

df_cost_24_site_harmonized=df_cost_24_site_harmonized.rename(columns={"unit_period":"occurrence_date"})
df_cost_24_site_harmonized["schedule_year"]=pd.to_datetime(df_cost_24_site_harmonized["occurrence_date"]).dt.year
df_cost_24_site_harmonized["schedule_month"]=pd.to_datetime(df_cost_24_site_harmonized["occurrence_date"]).dt.month


df_cost_24_unit_harmonized=df_cost_fc_granular_subset.groupby(["opportunity_number","contract_number","unit_serial_number",'scope', 'service', 'unit_type',"frequency",'unit','schedule_date']).aggregate({"cost":"sum"}).reset_index()
df_cost_24_unit_harmonized["schedule_year"]=pd.to_datetime(df_cost_24_unit_harmonized["schedule_date"]).dt.year
df_cost_24_unit_harmonized["schedule_month"]=pd.to_datetime(df_cost_24_unit_harmonized["schedule_date"]).dt.month
df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"schedule_date":"occurrence_date"})
df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"unit_type":"billing_type"})



df_fc_harmonized_costs_to_use=pd.concat([df_cost_24_unit_harmonized,df_cost_24_site_harmonized], axis=0)

df_fc_harmonized_costs_to_use.to_excel("df_fc_harmonized_costs_to_use_past_" + date_today + ".xlsx")

##
#Check with MYA-C Front-End (J920 with Kevin)
##


df_fc_harmonized_costs_to_use.loc[lambda x: (x["schedule_year"]>=year_actualization_onward),:].groupby(["contract_number"]).aggregate({"cost":"sum"})

df_fc_harmonized_costs_to_use.loc[lambda x: (x["contract_number"]=="SER_DE_00693")&(x["schedule_year"]>2023),:].groupby(["contract_number","scope"]).aggregate({"cost":"sum"})


df_fc_harmonized_costs_to_use.loc[lambda x: (x["contract_number"]=="SER_DE_00552")&(x["schedule_year"]>2023),:].groupby(["contract_number","scope"]).aggregate({"cost":"sum"})