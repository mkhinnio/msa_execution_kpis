





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
oracle_landscape_select = oracle_landscape_raw.rename(columns={"unit serial - number only":"usn","contract number":"contract_number"})

geo_loc_ib_metabase = import_geo_loc(conn)

df_corner_point = import_corner_point(conn)
ib_extended_report=import_ib_extended_from_azure(conn)

df_aggregated_oph_year, df_corner_point = aggregated_oph_year(df_corner_point)

df_aggregated_oph_year=df_aggregated_oph_year.merge(geo_loc_ib_metabase[["asset_id","unit_serial_number"]], how="left",on="asset_id")

##########################################################################################
#Total number of hours by fleet type
##########################################################################################

msa_df_1,msa_df_2,msa_df_3, msa_df_4, msa_df_5, df_steerco_overview_updated_msa, details_df_output1 = msa_fleet_status(oracle_landscape_select, "usn", ["PREVENTIVE AND CORRECTIVE","MSA USAGE BILLED", "MSA BILLABLE SHIPPING"],[
    "MSA_PREVENTIVE_AND_CORRECTIVE","MSA_PREVENTIVE"], date.today(), ib_status_selected, ib_extended_report)
df_steerco_overview_updated_msa

csa_df_1,csa_df_2,csa_df_3, csa_df_4, csa_df_5, df_steerco_overview_updated_csa, details_df_output2 = msa_fleet_status(oracle_landscape_select, "usn", ["PREVENTIVE AND CORRECTIVE","PREVENTIVE MAINTENANCE"],[
    "CSA_PREVENTIVE_AND_CORRECTIVE","CSA_PREVENTIVE"], date.today(), ib_status_selected, ib_extended_report)
df_steerco_overview_updated_csa


df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(msa_df_4)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})

df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(csa_df_4)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})


df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(msa_df_3)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})

df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(csa_df_3)==True,:].groupby(["year"]).aggregate({"actual_oph":"sum"})


##########################################################################################
#SAVINGS minOPH
##########################################################################################

financials_myac = get_financials_myac(conn)
financials_myac["year"]=pd.to_datetime(financials_myac["Unit Period"]).dt.year
financials_myac_year=financials_myac.loc[lambda x: x["Last Version"]==True,:]

financials_myac_year=financials_myac_year.groupby(["Unit Serial Number","year"]).agg(cost=("Cost","sum"),
                                                                                revenue=("Billings cons. Bonus/LD","sum")).reset_index().rename(columns={"Unit Serial Number":"unit_serial_number"})

###
#CSA fleet
###

df_aggregated_oph_year_csa=df_aggregated_oph_year.loc[lambda x: x["unit_serial_number"].isin(csa_df_4)==True,:]
df_aggregated_oph_year_csa=df_aggregated_oph_year_csa.merge(financials_myac_year, how="left",
                                                                             left_on=["unit_serial_number","year"], 
                                                                             right_on=["unit_serial_number","year"])
##
#MYA-C data
##



# load myac package data - is_myac_last_event == 1 (only this entry is the current valid one) & billingtype == "PACKAGE"
unit_definition_billings_h = get_unit_definition_billings_h(conn)
unit_definition_billings_h_select = unit_definition_billings_h[lambda x: (x["is_myac_last_event"] == 1) & (x["billingtype"] == "PACKAGE")][["unitdefinition_id", "billingtype", "title", "rate", "uom", "packagename", "maturityintervals"]] # , "id" 

# load myac unit data (also with is_myac_last_event == 1). this is mainly to attach serial number (usn) and be able to attach the contract later
unit_definition_h = get_unit_definition_h(conn)
unit_definition_h_select = unit_definition_h[lambda x: (x["is_myac_last_event"] == 1)][["id", "contractid", "unitstartcounter", "unitendcounter", "serialnumber", "enginetype", "engineversion","minimumoperatinghours","expectedoperatinghoursperyear"]] # "contractid", 

# load myac contract data (opportunity id and contract number)
contract_definition_h = get_contract_definition_h(conn)
contract_definition_h_select = contract_definition_h[lambda x: (x["is_myac_last_event"] == 1)][["id", "opportunityid", "primarycontract", "oraclecontractsnumber","contractcategory","effectivecontractstartdate"]].drop_duplicates()

# load myac opportunity data (otr-status and customername)
opportunity_definition_h = get_opportunity_definition_h(conn)
opportunity_definition_h_select = opportunity_definition_h[lambda x: (x["is_myac_last_event"] == 1)][["id", "opportunitynumber", "version", "customername"]].drop_duplicates()



#### MERGE MYA-C DATA
# combine unit- and package data
df_unit_definition_and_billings = pd.merge(
    unit_definition_h_select.rename(columns = {"id": "unitdefinition_id"}),
    unit_definition_billings_h_select,
    on = "unitdefinition_id",
    how = "outer"
)

# add contract info
df_unit_definition_and_billings_and_contract_info = pd.merge(
    df_unit_definition_and_billings,
    contract_definition_h_select.rename(columns = {"id": "contractid"}),
    on = "contractid",
    how = "left"
)

# add otr-status info
df_unit_definition_and_billings_and_contract_info = pd.merge(
    df_unit_definition_and_billings_and_contract_info,
    opportunity_definition_h_select.rename(columns = {"id": "opportunityid"}),
    on = "opportunityid",
    how = "left"
)



# create overview datamodel
dm_myac_overview = (df_unit_definition_and_billings_and_contract_info
                    [lambda x: (x["primarycontract"] == True)]
                    .rename(columns = {"serialnumber": "usn", "oraclecontractsnumber": "contract_number"})
                    [["opportunitynumber", "opportunityid", "contract_number","contractcategory", "contractid", "primarycontract","effectivecontractstartdate", "usn", "unitdefinition_id", "version", "unitstartcounter", "unitendcounter", "customername","minimumoperatinghours","expectedoperatinghoursperyear"]]
                    .drop_duplicates()
)

###
#CSA J9 fleet
###

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



#Opportunity

opp_myac=get_opportunity_config(conn)


#View J9 

financials_myac_j9.loc[lambda x: (x["Oracle Contract Number"]=="SER_IT_00640")&(x["year"]>2023)&(x["Unit Period"]==dt.date(2026, 10, 1)),:].groupby(["Last Actualized Date"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"})


conn_ala = activate_database_driver(driver_version="18", credentials_file="credentials_ALa.yml")

tables_site=get_financials_myac_cost_site(conn_ala)

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

financials_myac_j9.loc[lambda x: (x["Oracle Contract Number"]=="SER_IT_00640")&(x["year"]>2023),:].groupby(["Last Actualized Date"]).aggregate({'Billings cons. Bonus/LD':"sum",'Cost':"sum", "IC Cost":"sum"})


#
#Granular 


tables_granular=get_financials_myac_cost_granular(conn_ala)



df_aggregated_oph_year_csa=df_aggregated_oph_year_csa.merge(dm_myac_overview[["usn","minimumoperatinghours","expectedoperatinghoursperyear"]], 
                                                            how="left",
                                                            left_on="unit_serial_number",
                                                            right_on="usn")


##
#Revenue potential
##

df_aggregated_oph_year_csa_revenue=df_aggregated_oph_year_csa.drop_duplicates().dropna()
df_aggregated_oph_year_csa_revenue["minimumoperatinghours_2024"]=0.65*df_aggregated_oph_year_csa_revenue["expectedoperatinghoursperyear"]
df_aggregated_oph_year_csa_revenue["billable_oph"]=df_aggregated_oph_year_csa_revenue[["actual_oph","minimumoperatinghours"]].max(axis=1)
df_aggregated_oph_year_csa_revenue["billable_oph_24"]=df_aggregated_oph_year_csa_revenue[["actual_oph","minimumoperatinghours_2024"]].max(axis=1)

df_aggregated_oph_year_csa_revenue["rate_effective"]=df_aggregated_oph_year_csa_revenue["revenue"]/df_aggregated_oph_year_csa_revenue["billable_oph"]

df_aggregated_oph_year_csa_revenue["revenue_2024"]=df_aggregated_oph_year_csa_revenue["rate_effective"]*df_aggregated_oph_year_csa_revenue["billable_oph_24"]
df_aggregated_oph_year_csa_revenue["revenue_diff"]=df_aggregated_oph_year_csa_revenue["revenue_2024"]-df_aggregated_oph_year_csa_revenue["revenue"]

df_aggregated_oph_year_csa_revenue = df_aggregated_oph_year_csa_revenue.replace([np.inf, -np.inf], 0).fillna(0)

df_aggregated_oph_year_csa_revenue_select=df_aggregated_oph_year_csa_revenue.loc[lambda x: x["rate_effective"]<30,:]
df_aggregated_oph_year_csa_revenue_select=df_aggregated_oph_year_csa_revenue.loc[lambda x: x["actual_oph"]<x["minimumoperatinghours"],:]

df_aggregated_oph_year_csa_revenue_select.loc[lambda x: x["year"]<2024,:].groupby(["year"]).aggregate({"revenue_diff":"sum"}).sum()







##
#Event Scope mismatch myp


df_packages_events_sbom_myp=events_partscope_qty_myp(dmp_events, sbom_nonsuperseded)
df_packages_events_sbom_myp=df_packages_events_sbom_myp.merge(geo_loc_ib_metabase[["asset_id","serial_number","customer_name","unit_serial_number",
                                                                                   "service_contract_type"]],how="left",on="asset_id")

df_packages_events_sbom_myp=df_packages_events_sbom_myp.merge(oracle_landscape_select,how="left",left_on="unit_serial_number", right_on="usn")


df_packages_events_sbom_myp=df_packages_events_sbom_myp[["asset_id", "sum_zero_at_least_once","sum_zero_at_partscope"]].drop_duplicates()

#events_partscope_qty_myp_package


df_packages_events_sbom_myp=events_partscope_qty_myp_package(dmp_events, sbom_nonsuperseded)
df_packages_events_sbom_myp=df_packages_events_sbom_myp.merge(geo_loc_ib_metabase[["asset_id","serial_number","customer_name","unit_serial_number",
                                                                                   "service_contract_type"]],how="left",on="asset_id")


writer = pd.ExcelWriter("sbom_events_comparison.xlsx", engine='xlsxwriter')
create_excel_table_for_data_table(writer=writer, df=df_packages_events_sbom_myp, sheet_name="comparison")
writer.close()

df_packages_events_sbom_myp=df_packages_events_sbom_myp.merge(oracle_landscape_select,how="left",left_on="unit_serial_number", right_on="usn")


df_packages_events_sbom_myp=df_packages_events_sbom_myp[["asset_id", "sum_zero_at_least_once","sum_zero_at_partscope"]].drop_duplicates()
