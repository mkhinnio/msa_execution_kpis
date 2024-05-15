





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
ib_status_selected=["Active","Standby","Active Docu incomplete", "Temporarily Inactive"]

oracle_landscape_raw = import_oracle_data_from_azure(conn)

geo_loc_ib_metabase = import_geo_loc(conn)


df_corner_point = import_corner_point(conn)

df_aggregated_oph_year, df_corner_point = aggregated_oph_year(df_corner_point)

df_aggregated_oph_year=df_aggregated_oph_year.merge(geo_loc_ib_metabase[["asset_id","unit_serial_number"]], how="left",on="asset_id")
###
#Total number of hours by fleet type
###

msa_df_1,msa_df_2,msa_df_3, msa_df_4, msa_df_5, df_steerco_overview_updated_msa = msa_fleet_status(oracle_landscape_raw, "unit serial - number only", ["PREVENTIVE AND CORRECTIVE","MSA USAGE BILLED", "MSA BILLABLE SHIPPING"],[
    "MSA_PREVENTIVE_AND_CORRECTIVE","MSA_PREVENTIVE"], date.today(), ib_status_selected)
df_steerco_overview_updated_msa

csa_df_1,csa_df_2,csa_df_3, csa_df_4, csa_df_5, df_steerco_overview_updated_csa = msa_fleet_status(oracle_landscape_raw, "unit serial - number only", ["PREVENTIVE AND CORRECTIVE","PREVENTIVE MAINTENANCE"],[
    "CSA_PREVENTIVE_AND_CORRECTIVE","CSA_PREVENTIVE"], date.today(), ib_status_selected)
df_steerco_overview_updated_csa



df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(msa_df_4)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})

df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(csa_df_4)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})


df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(msa_df_3)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})

df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(csa_df_3)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})
