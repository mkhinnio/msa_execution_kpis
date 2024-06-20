




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
#Powerquery COst
###
conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")

tables_granular=get_financials_myac_cost_granular(conn)
df_power_query_allfinancials=power_query_allfinancials(conn)
df_power_query_allfinancials["contract_modification_date"]=df_power_query_allfinancials["contract_modification_date"].fillna("0")

tables_granular.loc[lambda x: x["contract_number"]=="SER_IT_00640",:].groupby(["unit_catalog_version"]).aggregate({"cost":"sum"})
tables_granular["unit_catalog_version"]=tables_granular["unit_catalog_version"].fillna("0")
tables_granular["unit_catalog_version"]=tables_granular["unit_catalog_version"].fillna("0")



###
#2024 numbers
###

#Billings 
df_power_query_allfinancials_billings=power_query_billings(conn)
df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True)&(x["primary_contract"]==True),:].groupby(["contract_number","opportunity_name_conf"]).aggregate({"billing_amount":"sum"}).to_excel("billing_2024.xlsx")


#Cost

#Granular one only 
#SER_DE_00952    kiel
df_kiel_contract=get_financials_myac_cost_granular_by_contract(conn)


df_kiel_contract_by_opportunity=get_financials_myac_cost_granular_by_opportunity(conn)


df_kiel_all_financials=power_query_allfinancials(conn)
df_kiel_all_financials.loc[lambda x: (x["unit_period"]>"2023-12-31"),:].groupby(["contract_number","opportunity_number","opportunity_last_version"]).aggregate({"cost":"sum","billings_consid_ldb":"mean"})


df_kiel_all_financials.loc[lambda x: (x["unit_period"]>="2023-12-31")&(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf"]).aggregate({"cost":"sum","billings_consid_ldb":"mean"}).to_excel("cost_2024_granular.xlsx")



df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True)&(x["primary_contract"]==True)&(x["contract_number"]=="SER_DE_01147"),:]


#
#FOR CMR Review run this on friday and weekend (retrieve Cottbus)
# 


df_power_query_allfinancials_billings=power_query_billings(conn)
df_power_query_allfinancials_billings.loc[lambda x: (x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["unit_activity_catalog"]=="920 Activity Catalog APR-2024")&(x["opportunity_version"]=="OTR")&
                                          (x["active_opportunity_version"]==True)&(x["opportunity_last_version"]==True)&(x["primary_contract"]==True),:].groupby(["contract_number","opportunity_name_conf"]).aggregate({"billing_amount":"sum"}).to_excel("billing_2024.xlsx")


df_kiel_all_financials=power_query_allfinancials(conn)
df_kiel_all_financials.loc[lambda x: (x["unit_period"]>="2023-12-31")&(x["opportunity_last_version"]==True)&(x["active_opportunity_version"]==True)&(x["opportunity_version"]=="OTR"),:].groupby(["opportunity_number","contract_number","opportunity_number_conf","contract_type","opportunity_name_conf"]).aggregate({"cost":"sum","billings_consid_ldb":"mean"}).to_excel("cost_2024_granular.xlsx")


# #
# #
# #"Cylinder Heads - CoQ"


# #COmpare 165 K difference in MYA-C financials and Cost tables

# meta_deviation_highlevel=df_power_query_allfinancials.loc[lambda x: (x["unit_activity_catalog"]=="920 Activity Catalog JUNE-2023")&(x["contract_number"]=="SER_IT_00640"),:] #.groupby(["contract_number"]).aggregate({"cost":"sum"})
# #Highlevel is to high 2.5 vs. 2.4 
# #Lowlevel matches MYA-C frontend
# meta_deviation_lowlevel=tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog JUNE-2023")&(x["contract_number"]=="SER_IT_00640"),:] #.groupby(["contract_number"]).aggregate({"cost":"sum"})

# tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog APR-2024")&(x["contract_number"]=="SER_IT_00640"),:].groupby(["contract_number"]).aggregate({"cost":"sum"})

# #Acea 1 and 2 1516209 SER_IT_00782

# #

# tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog JUNE-2023"),:].groupby(["opportunity_number","contract_number","contract_name"]).aggregate({"cost":"sum"})


# tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog JUNE-2023")&(x["contract_number"]=="SER_IT_00640"),:].groupby(["contract_number"]).aggregate({"cost":"sum"})

# #Acea 1 and 2 1516209 SER_IT_00782
# cn_select="SER_DE_00952"
# #SER_DE_00693 eon 
# # SER_DE_00952    kiel
# df_power_query_allfinancials.loc[lambda x: (x["unit_period"]>"2022-12-31")&(x["contract_number"]==cn_select),:].groupby(["unit_activity_catalog"]).aggregate({"cost":"sum"})
# #Highlevel is to high 2.5 vs. 2.4 
# #Lowlevel matches MYA-C frontend
# tables_granular.loc[lambda x: (x["contract_number"]==cn_select),:].groupby(["unit_catalog_version"]).aggregate({"cost":"sum","value":"sum"})



# ###

# tables_granular.loc[lambda x: (x["unit_catalog_version"]=="920 Activity Catalog APR-2024"),:].groupby(["opportunity_number","opportunity_last_version"]).aggregate({"cost":"sum","value":"sum"}).to_excel("output_tables_granular.xlsx")

# df_power_query_allfinancials.loc[lambda x: (x["unit_period"]>"2022-12-31"),:].groupby(["opportunity_number","opportunity_last_version","contract_modification_date"]).aggregate({"cost":"sum","billings_consid_ldb":"mean"}).reset_index().to_excel("output_tables_granular_higher_level.xlsx")

# tables_granular.loc[lambda x: (x["contract_number"]==cn_select),:].groupby(["unit_catalog_version"]).aggregate({"value":"sum"})


# ##Power querys joined 

# tables_joined_granular=power_query_cost(conn)

# tables_granular_single_contract=get_financials_myac_cost_granular_by_contract(conn)

