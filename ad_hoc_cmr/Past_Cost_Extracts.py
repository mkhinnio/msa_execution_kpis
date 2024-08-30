





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



#Parameters
catalogue="920 Activity Catalog JUN-2024"  #"920 Activity Catalog JUN-2024"  "920 Activity Catalog JUNE-2023"
period_actualization="2023-12-31"
year_actualization_onward=2023
sel_active_opp_version=True
sel_last_version=True

#
#FOR CMR Review run this on friday and weekend (+retrieve Cottbus)
# 

date_today=str(date.today())

#FC Billings 2024
df_power_query_allfinancials_billings=power_query_billings(conn)
df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]==catalogue)&(x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_last_version"]==sel_last_version)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>2023),:].groupby(["contract_number","opportunity_name_conf"]).aggregate({"billing_amount":"sum"}).to_excel("billing_2024" + date_today + ".xlsx")

df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]==catalogue)&(x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_last_version"]==sel_last_version)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>2023),:].groupby(["contract_number","opportunity_name_conf","unit_billing_type","unit_billing_date"]).aggregate({"billing_amount":"sum"}).reset_index().to_excel("fc_billing_2024_raw" + date_today + ".xlsx")


#FC Cost Topsum 2024
df_cost_all_financials=power_query_allfinancials(conn)
df_cost_all_financials["unit_period"]=df_cost_all_financials["unit_period"].fillna(pd.to_datetime(period_actualization))
df_cost_all_financials.loc[lambda x: (x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==sel_last_version)&(x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index().to_excel("fc_cost_2024_granular" + date_today + ".xlsx")


#FC Cost by type 2024 

df_cost_fc_granular=get_financials_myac_cost_granular_by_opportunity(conn)
#Filter for reelevant entries 
df_cost_fc_granular_subset=df_cost_fc_granular.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_last_version"]==sel_last_version)&
                                                   (x["active_opportunity_version"]==sel_active_opp_version)&(x["primary_contract"]==True) ,:]

#Check Cottbus


df_cost_fc_granular_subset=df_cost_fc_granular_subset.loc[lambda x: x["schedule_date"].dt.year>=year_actualization_onward,:]
df_cost_fc_granular_subset["scope"]=df_cost_fc_granular_subset["scope"].fillna("None")
df_cost_fc_granular_subset["unit_type"]=df_cost_fc_granular_subset["unit_type"].fillna("None")
df_cost_fc_granular_subset["service"]=df_cost_fc_granular_subset["service"].fillna("None")
df_cost_fc_granular_subset["unit"]=df_cost_fc_granular_subset["unit"].fillna("None")
df_cost_fc_granular_subset["schedule_date"]=df_cost_fc_granular_subset["schedule_date"].fillna("None")

# #Returns granular unit cost 
# df_cost_fc_granular_subset.groupby(["opportunity_number","contract_number","unit_catalog_version",'scope', 'service', 'unit_type','schedule_date']).aggregate({"value":"sum","cost":"sum","ic_cost":"sum"}).reset_index().to_excel("fc_unit_level_cost_2024_granular" + date_today + ".xlsx")

#Confirm that only site level is missing! 

#FC 2024 Site Level
df_cost_all_financials.loc[lambda x: (x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==sel_last_version)&(x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index().to_excel("fc_site_level_cost_2024_granular" + date_today + ".xlsx")





#FC_24_harmonized

df_cost_24_site_harmonized=df_cost_all_financials.loc[lambda x: (x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>=period_actualization)&(x["opportunity_last_version"]==sel_last_version)&(x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_type","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index()
df_cost_24_site_harmonized["scope"]="SITE"
df_cost_24_site_harmonized["service"]="None"
df_cost_24_site_harmonized["unit"]="None"
df_cost_24_site_harmonized["billing_type"]="None"

df_cost_24_site_harmonized=df_cost_24_site_harmonized.rename(columns={"unit_period":"occurrence_date"})
df_cost_24_site_harmonized["schedule_year"]=pd.to_datetime(df_cost_24_site_harmonized["occurrence_date"]).dt.year
df_cost_24_site_harmonized["schedule_month"]=pd.to_datetime(df_cost_24_site_harmonized["occurrence_date"]).dt.month


df_cost_24_unit_harmonized=df_cost_fc_granular_subset.groupby(["opportunity_number",'scope', 'service', 'unit_type','unit','schedule_date']).aggregate({"cost":"sum"}).reset_index()
df_cost_24_unit_harmonized["schedule_year"]=pd.to_datetime(df_cost_24_unit_harmonized["schedule_date"]).dt.year
df_cost_24_unit_harmonized["schedule_month"]=pd.to_datetime(df_cost_24_unit_harmonized["schedule_date"]).dt.month
df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"schedule_date":"occurrence_date"})
df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"unit_type":"billing_type"})

#

#Billigsharmoized

df_billings_fc_24_harmonized=df_power_query_allfinancials_billings.loc[lambda x: (x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==sel_active_opp_version)&(x["opportunity_last_version"]==sel_last_version)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>=year_actualization_onward),:].groupby(["opportunity_number","contract_number","opportunity_name_conf","unit_billing_type","unit_billing_date"]).aggregate({"billing_amount":"sum"}).reset_index()
#Filter out old MERHEIM 
df_billings_fc_24_harmonized=df_billings_fc_24_harmonized.loc[lambda x: x["opportunity_name_conf"]!="MYA HKW Merheim RheinEnergie 3xJ920  Y647 ",:]

df_billings_fc_24_harmonized=df_billings_fc_24_harmonized.rename(columns={"unit_billing_type":"billing_type","billing_amount":"billings","unit_billing_date":"billing_date"})

df_billings_fc_24_harmonized["schedule_year"]=pd.to_datetime(df_billings_fc_24_harmonized["billing_date"]).dt.year
df_billings_fc_24_harmonized["schedule_month"]=pd.to_datetime(df_billings_fc_24_harmonized["billing_date"]).dt.month

###
#Output of haronized files 
###

df_fc_harmonized_billings_to_use=pd.concat([df_billings_fc_24_harmonized], axis=0)
df_fc_harmonized_costs_to_use=pd.concat([df_cost_24_unit_harmonized,df_cost_24_site_harmonized], axis=0)

#Harmonize Opportunitnumbers 
df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="934650","opportunity_number"]="0934650"
df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="967118","opportunity_number"]="0967118"
df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="992496","opportunity_number"]="0992496"

df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="934650","opportunity_number"]="0934650"
df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="967118","opportunity_number"]="0967118"
df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="992496","opportunity_number"]="0992496"



# df_fc_harmonized_billings_to_use.to_excel("df_fc_harmonized_billings_to_use" + date_today + ".xlsx")
# df_fc_harmonized_costs_to_use.to_excel("df_fc_harmonized_costs_to_use" + date_today + ".xlsx")


# df_fc_harmonized_costs_to_use.groupby(["opportunity_number","cmr_year"]).aggregate({"cost":"sum"}).reset_index()


# ##
# #Comparing for selective contracts positions in FC
# ##
# sel_opportunity="1335920"
# #Check service or check scope billing_type
# df_cost_24_unit_harmonized.loc[lambda x: (x["schedule_year"]>2023)&(x["opportunity_number"]==sel_opportunity)&(x["scope"]=="Gen Set_Engine")&(x["billing_type"]=="INNIO_PARTS"),:].groupby(["opportunity_number","service"]).aggregate({"cost":"sum"}).sort_values(by="cost",ascending=False)

df_fc_harmonized_costs_to_use.groupby(["opportunity_number"]).aggregate({"cost":"sum"})


df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="0992496",:].groupby(["opportunity_number","scope","service","billing_type"]).aggregate({"cost":"mean"})

df_fc_harmonized_costs_to_use.loc[lambda x: (x["scope"]=="Exhaust gas heat exchanger")&(x["opportunity_number"]=="0992496"),:].head()


#adhoc 
