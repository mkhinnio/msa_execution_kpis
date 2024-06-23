





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

df_billing_fc_23=pd.read_csv("ad_hoc_cmr/dwh_myac_rs_billing_forecast_entry_202406191458 (1).csv")
df_cost_fc_23=pd.read_csv("ad_hoc_cmr/dwh_myac_rs_cost_forecast_entry_202406191456.csv")




#
#FOR CMR Review run this on friday and weekend (+retrieve Cottbus)
# 

date_today=str(date.today())

#FC Billings 2024
df_power_query_allfinancials_billings=power_query_billings(conn)
df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>2023),:].groupby(["contract_number","opportunity_name_conf"]).aggregate({"billing_amount":"sum"}).to_excel("billing_2024" + date_today + ".xlsx")

df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>2023),:].groupby(["contract_number","opportunity_name_conf","unit_billing_type","unit_billing_date"]).aggregate({"billing_amount":"sum"}).reset_index().to_excel("fc_billing_2024_raw" + date_today + ".xlsx")


#FC Cost Topsum 2024
df_kiel_all_financials=power_query_allfinancials(conn)
df_kiel_all_financials["unit_period"]=df_kiel_all_financials["unit_period"].fillna(pd.to_datetime("2024-12-31"))
df_kiel_all_financials.loc[lambda x: (x["unit_period"]>="2023-12-31")&(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index().to_excel("fc_cost_2024_granular" + date_today + ".xlsx")


#FC Cost by type 2024 

df_cost_fc_granular=get_financials_myac_cost_granular_by_opportunity(conn)
#Filter for reelevant entries 
df_cost_fc_granular_subset=df_cost_fc_granular.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_last_version"]==True)&
                                                   (x["active_opportunity_version"]==True)&(x["primary_contract"]==True) ,:]

#Check Cottbus

df_cost_fc_granular.loc[lambda x: (x["opportunity_number"]==1168281) ,:].groupby(["opportunity_version","opportunity_last_version","active_opportunity_version","primary_contract"]).aggregate({"cost":"sum"})

df_cost_fc_granular_subset=df_cost_fc_granular_subset.loc[lambda x: x["schedule_date"].dt.year>=2023,:]
df_cost_fc_granular_subset["scope"]=df_cost_fc_granular_subset["scope"].fillna("None")
df_cost_fc_granular_subset["unit_type"]=df_cost_fc_granular_subset["unit_type"].fillna("None")
df_cost_fc_granular_subset["service"]=df_cost_fc_granular_subset["service"].fillna("None")
df_cost_fc_granular_subset["unit"]=df_cost_fc_granular_subset["unit"].fillna("None")
df_cost_fc_granular_subset["schedule_date"]=df_cost_fc_granular_subset["schedule_date"].fillna("None")

#Returns granular unit cost 
df_cost_fc_granular_subset.groupby(["opportunity_number","contract_number","unit_catalog_version",'scope', 'service', 'unit_type','schedule_date']).aggregate({"value":"sum","cost":"sum","ic_cost":"sum"}).reset_index().to_excel("fc_unit_level_cost_2024_granular" + date_today + ".xlsx")

#Confirm that only site level is missing! 

#FC 2024 Site Level
df_kiel_all_financials.loc[lambda x: (x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>="2023-12-31")&(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index().to_excel("fc_site_level_cost_2024_granular" + date_today + ".xlsx")



###
#FC 2023 Numbers


df_billing_fc_23.loc[lambda x: pd.to_datetime(x["billing_date"]).dt.year>2024,:].groupby(["opportunity","billing_date","type"]).aggregate({"value":"sum"}).reset_index().to_excel("fc_billing_2023_raw" + date_today + ".xlsx")



df_cost_fc_23.loc[lambda x: pd.to_datetime(x["occurrence_date"]).dt.year>2024,:].groupby(["opportunity","occurrence_date","type","scope","service","unit"]).aggregate({"cost":"sum","ic_cost":"sum","ic_value":"sum"}).reset_index().to_excel("fc_cost_2023_raw" + date_today + ".xlsx")



#
#Harmonize format and names values

#FC_23_harmonized

df_billings_fc_23_harmonized=df_billing_fc_23.loc[lambda x: pd.to_datetime(x["billing_date"]).dt.year>=2023,:].groupby(["opportunity","billing_date","type"]).aggregate({"value":"sum"}).reset_index()
df_billings_fc_23_harmonized=df_billings_fc_23_harmonized.rename(columns={"type":"billing_type","value":"billings","opportunity":"opportunity_number"})
df_billings_fc_23_harmonized["schedule_year"]=pd.to_datetime(df_billings_fc_23_harmonized["billing_date"]).dt.year
df_billings_fc_23_harmonized["schedule_month"]=pd.to_datetime(df_billings_fc_23_harmonized["billing_date"]).dt.month
df_billings_fc_23_harmonized["cmr_year"]=2023


df_cost_fc_23_harmonized=df_cost_fc_23.loc[lambda x: pd.to_datetime(x["occurrence_date"]).dt.year>=2023,:].groupby(["opportunity","occurrence_date","type","scope","service","unit"]).aggregate({"cost":"sum"}).reset_index()
df_cost_fc_23_harmonized=df_cost_fc_23_harmonized.rename(columns={"type":"billing_type","opportunity":"opportunity_number"})
df_cost_fc_23_harmonized["schedule_year"]=pd.to_datetime(df_cost_fc_23_harmonized["occurrence_date"]).dt.year
df_cost_fc_23_harmonized["schedule_month"]=pd.to_datetime(df_cost_fc_23_harmonized["occurrence_date"]).dt.month
df_cost_fc_23_harmonized["cmr_year"]=2023

#FC_24_harmonized

df_cost_24_site_harmonized=df_kiel_all_financials.loc[lambda x: (x["unit_activity_catalog"].isna()==True)&(x["unit_period"]>="2023-12-31")&(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_type","unit_period"]).aggregate({"cost":"sum","billings_consid_ldb":"sum"}).reset_index()
df_cost_24_site_harmonized["scope"]="SITE"
df_cost_24_site_harmonized["service"]="None"
df_cost_24_site_harmonized["unit"]="None"
df_cost_24_site_harmonized["billing_type"]="None"

df_cost_24_site_harmonized=df_cost_24_site_harmonized.rename(columns={"unit_period":"occurrence_date"})
df_cost_24_site_harmonized["schedule_year"]=pd.to_datetime(df_cost_24_site_harmonized["occurrence_date"]).dt.year
df_cost_24_site_harmonized["schedule_month"]=pd.to_datetime(df_cost_24_site_harmonized["occurrence_date"]).dt.month
df_cost_24_site_harmonized["cmr_year"]=2024


df_cost_24_unit_harmonized=df_cost_fc_granular_subset.groupby(["opportunity_number",'scope', 'service', 'unit_type','unit','schedule_date']).aggregate({"cost":"sum"}).reset_index()
df_cost_24_unit_harmonized["schedule_year"]=pd.to_datetime(df_cost_24_unit_harmonized["schedule_date"]).dt.year
df_cost_24_unit_harmonized["schedule_month"]=pd.to_datetime(df_cost_24_unit_harmonized["schedule_date"]).dt.month
df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"schedule_date":"occurrence_date"})
df_cost_24_unit_harmonized=df_cost_24_unit_harmonized.rename(columns={"unit_type":"billing_type"})
df_cost_24_unit_harmonized["cmr_year"]=2024

#

#Billigsharmoized

df_billings_fc_24_harmonized=df_power_query_allfinancials_billings.loc[lambda x: (x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True)&(x["primary_contract"]==True)&(x["unit_billing_date"].dt.year>=2023),:].groupby(["opportunity_number","contract_number","opportunity_name_conf","unit_billing_type","unit_billing_date"]).aggregate({"billing_amount":"sum"}).reset_index()
#Filter out old MERHEIM 
df_billings_fc_24_harmonized=df_billings_fc_24_harmonized.loc[lambda x: x["opportunity_name_conf"]!="MYA HKW Merheim RheinEnergie 3xJ920  Y647 ",:]

df_billings_fc_24_harmonized=df_billings_fc_24_harmonized.rename(columns={"unit_billing_type":"billing_type","billing_amount":"billings","unit_billing_date":"billing_date"})

df_billings_fc_24_harmonized["schedule_year"]=pd.to_datetime(df_billings_fc_24_harmonized["billing_date"]).dt.year
df_billings_fc_24_harmonized["schedule_month"]=pd.to_datetime(df_billings_fc_24_harmonized["billing_date"]).dt.month
df_billings_fc_24_harmonized["cmr_year"]=2024

###
#Output of haronized files 
###
df_cost_fc_23_harmonized["opportunity_number"]=df_cost_fc_23_harmonized["opportunity_number"].astype(str)
df_cost_fc_23_harmonized.loc[lambda x: x["billing_type"]=="Other Provider","billing_type"]="OTHER_PROVIDER"
df_cost_fc_23_harmonized.loc[lambda x: x["billing_type"]=="INNIO Parts","billing_type"]="INNIO_PARTS"
df_cost_fc_23_harmonized.loc[lambda x: x["billing_type"]=="INNIO Labor","billing_type"]="INNIO_LABOR"
df_cost_fc_23_harmonized.loc[lambda x: x["billing_type"]=="Freight","billing_type"]="FREIGHT"

df_billings_fc_23_harmonized["opportunity_number"]=df_billings_fc_23_harmonized["opportunity_number"].astype(str)


df_fc_harmonized_billings_to_use=pd.concat([df_billings_fc_24_harmonized,df_billings_fc_23_harmonized], axis=0)
df_fc_harmonized_costs_to_use=pd.concat([df_cost_24_unit_harmonized,df_cost_24_site_harmonized,df_cost_fc_23_harmonized], axis=0)

#Harmonize Opportunitnumbers 
df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="934650","opportunity_number"]="0934650"
df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="967118","opportunity_number"]="0967118"
df_fc_harmonized_billings_to_use.loc[lambda x: x["opportunity_number"]=="992496","opportunity_number"]="0992496"

df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="934650","opportunity_number"]="0934650"
df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="967118","opportunity_number"]="0967118"
df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="992496","opportunity_number"]="0992496"



df_fc_harmonized_billings_to_use.to_excel("df_fc_harmonized_billings_to_use" + date_today + ".xlsx")
df_fc_harmonized_costs_to_use.to_excel("df_fc_harmonized_costs_to_use" + date_today + ".xlsx")


df_fc_harmonized_costs_to_use.groupby(["opportunity_number","cmr_year"]).aggregate({"cost":"sum"}).reset_index()

###
#Harmonize actuals (tried to harmonized)
##

#
#Load ACTUALS CMR 23

df_actuals_23=pd.read_csv("ad_hoc_cmr/actuals_2023_gaurav.csv", on_bad_lines='skip', delimiter=";", decimal=",", encoding='latin-1')
df_actuals_23[ ' EUR Amount ']=df_actuals_23[ ' EUR Amount '].str.replace(",",".")
df_actuals_23[ ' EUR Amount ']=df_actuals_23[ ' EUR Amount '].str.replace(" ","")
df_actuals_23.loc[lambda x: x[ ' EUR Amount ']=="-",' EUR Amount ']=0
df_actuals_23[' EUR Amount ']=df_actuals_23[' EUR Amount '].astype(float).astype(int)

df_actuals_23_revenue_eur=df_actuals_23.loc[lambda x: (x["GL_Gross_Margin"]=="Revenue")&(x["Source"]=="Receivables")&(x["Revenue Inclusion"]=="Include"),:].groupby(["OKS Contract Number","Quarter"]).aggregate({" EUR Amount ":"sum"}).reset_index()

df_actuals_23_revenue_eur=df_actuals_23_revenue_eur.rename(columns={"EUR Amount":"billings","OKS Contract Number":"contract_number"})
df_actuals_23_revenue_eur["currency"]="EUR"


#EUR cost

#Preventive

df_actuals_23_cost_preventive_eur=df_actuals_23.loc[lambda x: ((x["Cost Category"].isna()==True)|(x["Cost Category"].isin(["Planned"])==True))&(x["Expense Inclusion"]=="include"),:].groupby(["OKS Contract Number","Quarter"]).aggregate({" EUR Amount ":"sum"}).reset_index()

df_actuals_23_cost_preventive_eur=df_actuals_23_cost_preventive_eur.rename(columns={"EUR Amount":"billings","OKS Contract Number":"contract_number"})
df_actuals_23_cost_preventive_eur["currency"]="EUR"
df_actuals_23_cost_preventive_eur

#Corrective

df_actuals_23_cost_corrective_eur=df_actuals_23.loc[lambda x: ((x["service_cost_category"].isna()==True)|(x["service_cost_category"].isin(["Corrective"])==True)|(x["service_cost_category"]=="0"))&((x["Cost Category"].isin(["Unplanned"])==True))&(x["Expense Inclusion"]=="include"),:].groupby(["OKS Contract Number","Quarter"]).aggregate({" EUR Amount ":"sum"}).reset_index()

df_actuals_23_cost_corrective_eur=df_actuals_23_cost_corrective_eur.rename(columns={"EUR Amount":"billings","OKS Contract Number":"contract_number"})
df_actuals_23_cost_corrective_eur["currency"]="EUR"
df_actuals_23_cost_corrective_eur


#COQ Fleet Program 
df_actuals_23_cost_preventive_eur=df_actuals_23.loc[lambda x: ((x["Cost Category"].isna()==True)|(x["Cost Category"].isin(["Planned"])==True))&(x["Expense Inclusion"]=="include"),:].groupby(["OKS Contract Number","Quarter"]).aggregate({" EUR Amount ":"sum"}).reset_index()

df_actuals_23_cost_preventive_eur=df_actuals_23_cost_preventive_eur.rename(columns={"EUR Amount":"billings","OKS Contract Number":"contract_number"})
df_actuals_23_cost_preventive_eur["currency"]="EUR"
df_actuals_23_cost_preventive_eur

#
#Focus on the price effect for usage billing
# 
#Load myac_opportunity_configuration
#

##
#Load opportunity configuration report to assess price quantity results
##
#Draw on Contribution Margin 

conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")
df_financials_dwh_vw=get_financials_myac(conn)
df_opp_conf=get_opportunity_config(conn)

#Compare against
df_opp_conf.loc[lambda x: (x["Last Actualized Date"]=="2023-12-31")&(x["Last Version"]==True),["Opportunity Name","Oracle Contract Number", 'Last Actualized Date',"Total Opportunity CM% Cons Bonus/LDs"]].drop_duplicates().sort_values(by=["Last Actualized Date"], ascending=False)

df_opp_conf.loc[lambda x: (x["Oracle Contract Number"]=="SER_DE_01209")&(x["Last Actualized Date"]=="2023-12-31"),:]


df_opp_conf.loc[lambda x: (x["Oracle Contract Number"]=="SER_DE_01209")&(x["Last Actualized Date"]=="2023-12-31")&(x["Unit Serial Number"]=="1402506"),].drop_duplicates()


#Compare forecast sum 


df_financials_dwh_vw.groupby(["Oracle Contract Number"]).aggregate({"Billings cons. Bonus/LD":"sum","Cost":"sum"})

df_financials_dwh_vw.loc[lambda x: (x["Oracle Contract Number"]=="SER_DE_01209")&(x["Last Actualized Date"]=="2023-12-31"),:].groupby(["Oracle Contract Number","Contract Name","Primary Contract","Active Opportunity Version","Last Version"]).aggregate({"Billings cons. Bonus/LD":"sum","Cost":"sum"})
