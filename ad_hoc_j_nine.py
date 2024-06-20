




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
oracle_landscape_select = oracle_landscape_raw.rename(columns={"unit serial - number only":"usn","contract number":"contract_number"})



financials_myac = get_financials_myac(conn)
financials_myac["year"]=pd.to_datetime(financials_myac["Unit Period"]).dt.year
financials_myac_year=financials_myac.loc[lambda x: x["Last Version"]==True,:]

financials_myac_year=financials_myac_year.groupby(["Unit Serial Number","year"]).agg(cost=("Cost","sum"),
                                                                                revenue=("Billings cons. Bonus/LD","sum")).reset_index().rename(columns={"Unit Serial Number":"unit_serial_number"})


#How many J9 fleet? ==> financials_myac
#pgsdwh.sot_gps_dp.dwh_vw_myac_financial_forecast_report_csa

usn_j9_select=oracle_landscape_raw.loc[lambda x: x["product family"]=="Type 9","unit serial - number only"].unique()
financials_myac_j9=financials_myac.loc[lambda x: (x["Unit Serial Number"].isin(usn_j9_select)==True),:]

financials_myac_j9=financials_myac.loc[lambda x: (x["Last Version"]==True)&(x["Unit Serial Number"].isin(usn_j9_select)==True),:]
financials_myac_j9=financials_myac_j9.loc[lambda x: (x["Opportunity Version"]=="OTR"),:]
financials_myac_j9=financials_myac_j9.loc[lambda x: (x["Primary Contract"]==True),:]

financials_myac_j9.loc[lambda x: x["Oracle Contract Number"]=="SER_DE_01147",:].groupby(["year"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"}).sum()



#Works
financials_myac_j9.loc[lambda x: x["Oracle Contract Number"]=="SER_DE_01399",:].groupby(["year"]).aggregate({'Billings cons. Bonus/LD':"sum"}).sum()
#Billings correct in total
#Cost is wrong
financials_myac_j9.loc[lambda x: x["Oracle Contract Number"]=="SER_DE_01589",:].groupby(["year"]).aggregate({'Billings cons. Bonus/LD':"sum"}).sum()


#Works and contracts
financials_myac_j9.loc[lambda x: (x["Oracle Contract Number"]=="SER_DE_01589"),:].groupby(["year"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"}).sum()


financials_myac_j9.loc[lambda x: x["Unit Serial Number"]=="1405726",:].groupby(["year"]).aggregate({'Billings cons. Bonus/LD':"sum"})


#Covered additional scope is not included - differences is due to covered additional scope
financials_myac_j9.loc[lambda x: (x["Oracle Contract Number"]=="SER_IT_00640"),:].groupby(["year"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"}).sum()



#Covered additional scope is not included - differences is due to covered additional scope
financials_myac_j9.loc[lambda x: (x["Oracle Contract Number"]=="SER_DE_00552"),:].groupby(["year"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"}).sum()


#View J9 

financials_myac_j9.loc[lambda x: (x["Oracle Contract Number"]=="SER_IT_00640")&(x["year"]>2023)&(x["Unit Period"]==dt.date(2026, 10, 1)),:].groupby(["Last Actualized Date"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"})


conn_ala = activate_database_driver(driver_version="18", credentials_file="credentials_ALa.yml")

tables_site=get_financials_myac_cost_site(conn)

tables_site.loc[lambda x: (x["contract_number"]=="SER_IT_00640")&(x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True),:]

tables_site.loc[lambda x: (x["contract_number"]=="SER_IT_00640")&
                (x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True),:].groupby(["unit_read_date"]).aggregate({"cost":"sum"})


tables_site.loc[lambda x: (x["contract_number"]=="SER_IT_00640"),:].groupby(["unit_read_date"]).aggregate({"cost":"sum"})

tables_site.loc[lambda x: (x["contract_number"]=="SER_IT_00640")&(x["unit_read_date"].isna()==True),:]

tables_site.loc[lambda x: (x["contract_number"]=="SER_IT_00640")&(x["unit_read_date"].isna()==True)
                &(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True),"cost"].sum()

#Overview actuals myac
###


financials_myac_j9=financials_myac.loc[lambda x: (x["Last Version"]==True)&(x["Opportunity Version"]=="OTR")&
                                       (x["Primary Contract"]==True),:]

financials_myac_j9=financials_myac.loc[lambda x: (x["Primary Contract"]==True),:]
financials_myac_j9.loc[lambda x: (x["Oracle Contract Number"]=="SER_IT_00640")&(x["year"]>2023),:].groupby(["Last Actualized Date"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"})


#
#Granular 

conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")

tables_granular=get_financials_myac_cost_granular(conn)



tables_granular.loc[lambda x: (x["contract_number"]=="SER_IT_00640")
                &(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True)&(x["unit_serial_number"]=="1221617")
                &(x["scope"]=="Gen Set_Engine")&(x["service"]=="Unplanned Maintenance TS")&(x["schedule_date"]=="2024-09-11"),:]




tables_granular.loc[lambda x: (x["contract_number"]=="SER_IT_00640")
                &(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True),:].groupby(["unit_catalog_version","unit_type"]).aggregate({"value":"sum"})



###
df_costs=get_cost_information_actuals(conn)
list_mat=[]
for col in df_costs.columns:
    try:
        mask=df_costs.loc[:,col].astype(str).str.contains("JEN_",case=False,na=False)
        if mask.any():
            print(f"Found matches in {col}:")
            list_mat.append(col)
        else: 
            print(print(f"Not found matches in {col}:"))
        
    except:
        print("error")

dmp_events=import_dmp_events(conn)
dmp_events=dmp_events.loc[lambda x: x["type"]=="unplanned",:]

for i in range(5,len(dmp_events)):
    try:
        dmp_events["execution_timestamp"].iloc[i]=dt.datetime.fromtimestamp((round(dmp_events["execution_timestamp"].iloc[i]/1000,0)))
    except:
        dmp_events["execution_timestamp"].iloc[i]="None"

dmp_events=dmp_events.loc[lambda x: x["execution_timestamp"]!="None",:]
dmp_events=dmp_events.loc[lambda x: x["execution_timestamp"]!="None",:]

srs_costs_frame=dmp_events.loc[lambda x: x["asset_id"]==112491,"sr_number"].unique()





###
#Powerquery COst
###
conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")

df_power_query_cost=power_query_cost(conn)
df_power_query_billings=power_query_billings(conn)
df_power_query_allfinancials=power_query_allfinancials(conn)
df_power_query_opportunityname=power_query_opportunityname(conn)

#Check pivot financials 

df_power_query_allfinancials.loc[lambda x: x["opportunity_name_conf"]=="ACEA 3 - 1xJ920 ",:].groupby(["contract_modification_date"]).aggregate({"cost":"sum"})

df_power_query_allfinancials.loc[lambda x: x["opportunity_name_conf"]=="ACEA 3 - 1xJ920 ",:].groupby(["contract_modification_date"]).aggregate({"cost":"sum"})


#Meta

#SER_IT_00640

df_power_query_allfinancials.loc[lambda x: x["contract_number"]=="SER_IT_00640",:].groupby(["contract_modification_date"]).aggregate({"cost":"sum"})



df_power_query_allfinancials.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_number"]=="1516209")&(x["contract_modification_date"]=="2024-02-16 11:00:00	"),:].groupby(["contract_modification_date"]).aggregate({"cost":"sum"})


df_power_query_allfinancials.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_number"]=="1516209")&(x["contract_modification_date"]=="2024-02-16 11:00:00	"),:]


#
#Site level granularity 
#
#Granularty

df_power_query_cost.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_number"]=="1516209"),:]

tables_granular.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_number"]=="1516209"),"cost"].sum()
tables_granular=get_financials_myac_cost_granular(conn)


df_power_query_allfinancials.loc[lambda x: x["contract_number"]=="SER_IT_00640",:].groupby(["contract_modification_date"]).aggregate({"cost":"sum"})

tables_granular.loc[lambda x: x["contract_number"]=="SER_IT_00640",:].groupby(["unit_catalog_version"]).aggregate({"cost":"sum"})

#
#"Cylinder Heads - CoQ"


#COmpare 165 K difference in MYA-C financials and Cost tables

meta_deviation_highlevel=df_power_query_allfinancials.loc[lambda x: (x["unit_activity_catalog"]=="920 Activity Catalog JUNE-2023")&(x["contract_number"]=="SER_IT_00640"),:] #.groupby(["contract_number"]).aggregate({"cost":"sum"})
#Highlevel is to high 2.5 vs. 2.4 
#Lowlevel matches MYA-C frontend
meta_deviation_lowlevel=tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog JUNE-2023")&(x["contract_number"]=="SER_IT_00640"),:] #.groupby(["contract_number"]).aggregate({"cost":"sum"})

tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog APR-2024")&(x["contract_number"]=="SER_IT_00640"),:].groupby(["contract_number"]).aggregate({"cost":"sum"})

#Acea 1 and 2 1516209 SER_IT_00782

#
tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog APR-2024")&(x["contract_number"]=="SER_IT_00782"),:].groupby(["contract_number"]).aggregate({"cost":"sum"})
