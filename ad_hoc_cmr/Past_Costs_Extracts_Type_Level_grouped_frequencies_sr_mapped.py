





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
date_today=str(date.today())


####################
#Preprocess ORACLE DATA 
####################

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


#Filter out specific values 
oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["contract name"]!="SCHEDULER",:]

#Select active and active oks
oracle_landscape_raw_select_active_active=oracle_landscape_raw_select.loc[lambda x: (x["unit oks status"]=="ACTIVE")&(x["contract status"]=="ACTIVE"),:]
#Select terminated if nothing else is there 
oracle_landscape_raw_select_active_terminated=oracle_landscape_raw_select.loc[lambda x: (x["unit oks status"]=="ACTIVE")&(x["contract status"]!="ACTIVE"),:]

oracle_landscape_raw_select=pd.concat([oracle_landscape_raw_select_active_active,oracle_landscape_raw_select_active_terminated],axis=0)

#oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["contract type myac"].str.contains("CSA")==True,:]


oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["unit oks status"]=="ACTIVE",:]

#Select unit status IB
oracle_landscape_raw_select=oracle_landscape_raw_select.loc[lambda x: x["unit status ib"].isin(ib_status_selected)==True,:]

relevant_contracts=oracle_landscape_raw_select.groupby(["unit serial - number only","contract status","contract number"]).aggregate({"contract name":"nunique"}).reset_index()

#List of usns for active and inactive contracts
relevant_contracts_not_active=relevant_contracts.loc[lambda x: x["contract status"]!="ACTIVE","unit serial - number only"].unique()
relevant_contracts_active=relevant_contracts.loc[lambda x: x["contract status"]=="ACTIVE","unit serial - number only"].unique()

#Combined list of relevant contracts
relevant_contracts=relevant_contracts.loc[lambda x: ((x["contract status"]=="ACTIVE")&(x["unit serial - number only"].isin(relevant_contracts_active)==True))|(((x["unit serial - number only"].isin(relevant_contracts_active)==False))&(x["unit serial - number only"].isin(relevant_contracts_not_active)==True)),:]




##########################################################################################
# LOAD MYA-C data sequentially
##########################################################################################

flat_list_usns=oracle_landscape_raw_select["unit serial - number only"].unique().tolist()
partitioned_list = [flat_list_usns[i:i + 100] for i in range(0, len(flat_list_usns), 100)] 

partioned_outputs=[]
partioned_outputs_grouped=[]

for i in range(88,len(partitioned_list)-1):  #len(partitioned_list)

    #FC Cost Topsum 2024
    conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")
    try:
        df_cost_all_financials=power_query_allfinancials_usns_sequentially(conn,partitioned_list[i])
        df_cost_all_financials["unit_period"]=df_cost_all_financials["unit_period"].fillna(pd.to_datetime(period_actualization))
    except:
        conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")
        df_cost_all_financials=power_query_allfinancials_usns_sequentially(conn,partitioned_list[i])
        df_cost_all_financials["unit_period"]=df_cost_all_financials["unit_period"].fillna(pd.to_datetime(period_actualization))

    #FC Cost by type 2024 
    conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")

    try:
        df_cost_fc_granular=get_financials_myac_cost_granular_by_opportunity_usns_sequentially(conn,partitioned_list[i])
    except:
        conn = activate_database_driver(driver_version="18", credentials_file="credentials.yml")
        df_cost_fc_granular=get_financials_myac_cost_granular_by_opportunity_usns_sequentially(conn,partitioned_list[i])
    #Filter for relevant version of cost catalogue 
    df_cost_fc_granular_subset_otr=df_cost_fc_granular.loc[lambda x: (x["opportunity_version"]=="OTR")&(x["opportunity_last_version"]==sel_last_version)&
                                                    (x["active_opportunity_version"]==sel_active_opp_version)&(x["primary_contract"]==True) ,:]

    df_cost_fc_granular_subset_handover=df_cost_fc_granular.loc[lambda x: (x["opportunity_version"]=="HANDOVER")&(x["opportunity_last_version"]==sel_last_version)&
                                                    (x["active_opportunity_version"]==sel_active_opp_version)&(x["primary_contract"]==True) ,:]

    df_cost_fc_granular_subset_ended=df_cost_fc_granular.loc[lambda x: (x["opportunity_version"]=="ENDED")&(x["opportunity_last_version"]==sel_last_version)&
                                                    (x["active_opportunity_version"]==sel_active_opp_version)&(x["primary_contract"]==True) ,:]

    df_cost_fc_granular_subset_combined=pd.concat([df_cost_fc_granular_subset_otr,df_cost_fc_granular_subset_handover,df_cost_fc_granular_subset_ended],axis=0)
    df_cost_fc_granular_subset=pd.concat([df_cost_fc_granular_subset_otr,df_cost_fc_granular_subset_handover,df_cost_fc_granular_subset_ended],axis=0)

    otr_given=df_cost_fc_granular_subset_combined.groupby(["unit_serial_number"]).aggregate({"opportunity_version":"unique"}).reset_index().loc[lambda x: x["opportunity_version"].astype(str).str.contains("OTR"),"unit_serial_number"].unique().tolist()
    hand_over_given=df_cost_fc_granular_subset_combined.groupby(["unit_serial_number"]).aggregate({"opportunity_version":"unique"}).reset_index().loc[lambda x: x["opportunity_version"].astype(str).str.contains("HANDOVER"),"unit_serial_number"].unique().tolist()
    ended_given=df_cost_fc_granular_subset_combined.groupby(["unit_serial_number"]).aggregate({"opportunity_version":"unique"}).reset_index().loc[lambda x: x["opportunity_version"].astype(str).str.contains("ENDED"),"unit_serial_number"].unique().tolist()
    
    #Filters
    OTR_filter=df_cost_fc_granular_subset_otr["unit_serial_number"].isin(otr_given)==True
    ENDED_filter=(df_cost_fc_granular_subset_ended["unit_serial_number"].isin(otr_given)==False)&(df_cost_fc_granular_subset_ended["unit_serial_number"].isin(ended_given)==True)
    HANDOVER_filter=(df_cost_fc_granular_subset_handover["unit_serial_number"].isin(otr_given)==False)&(df_cost_fc_granular_subset_handover["unit_serial_number"].isin(ended_given)==False)&(df_cost_fc_granular_subset_handover["unit_serial_number"].isin(hand_over_given)==False)

    df_cost_fc_granular_subset=pd.concat([df_cost_fc_granular_subset_otr[OTR_filter],df_cost_fc_granular_subset_handover[HANDOVER_filter]
                                          ,df_cost_fc_granular_subset_ended[ENDED_filter]],axis=0)

    #Filters for actualization
    most_recent_actualization="2023"
    most_recent_actualization_min_1="2022"
    
    df_cost_fc_granular_subset_recent=df_cost_fc_granular_subset.loc[lambda x: (x["contract_actualization_period"].isna()==False)&(x["contract_actualization_period"].str.contains(most_recent_actualization)==True),:]
    df_cost_fc_granular_subset_recent_min_1=df_cost_fc_granular_subset.loc[lambda x: (x["contract_actualization_period"].isna()==False)&(x["contract_actualization_period"].str.contains(most_recent_actualization_min_1)==True),:]
    df_cost_fc_granular_subset_recent_na=df_cost_fc_granular_subset.loc[lambda x: (x["contract_actualization_period"].isna()==True),:]

    recent_usn=df_cost_fc_granular_subset_recent["unit_serial_number"].unique()
    recent_usn_min_1=df_cost_fc_granular_subset_recent_min_1["unit_serial_number"].unique()
    recent_usn_na=df_cost_fc_granular_subset_recent_na["unit_serial_number"].unique()

    #Filters
    recent_filter=(df_cost_fc_granular_subset_recent["unit_serial_number"].isin(recent_usn)==True)&(df_cost_fc_granular_subset_recent["contract_actualization_period"].str.contains(most_recent_actualization)==True)
    recent_min_1filter=(df_cost_fc_granular_subset_recent_min_1["unit_serial_number"].isin(recent_usn_min_1)==True)&(df_cost_fc_granular_subset_recent_min_1["unit_serial_number"].isin(recent_usn)==False)&(df_cost_fc_granular_subset_recent_min_1["contract_actualization_period"].str.contains(most_recent_actualization_min_1)==True)
    na_fitler=(df_cost_fc_granular_subset_recent_na["unit_serial_number"].isin(recent_usn_min_1)==False)&(df_cost_fc_granular_subset_recent_na["unit_serial_number"].isin(recent_usn)==False)&(df_cost_fc_granular_subset_recent_na["unit_serial_number"].isin(recent_usn_na)==True)

    df_cost_fc_granular_subset=pd.concat([df_cost_fc_granular_subset_recent[recent_filter],df_cost_fc_granular_subset_recent_min_1[recent_min_1filter]
                                          ,df_cost_fc_granular_subset_recent_na[na_fitler]],axis=0)

    df_cost_fc_granular_subset=df_cost_fc_granular_subset.loc[lambda x: x["schedule_date"].dt.year>=year_actualization_onward,:]
    df_cost_fc_granular_subset["scope"]=df_cost_fc_granular_subset["scope"].fillna("None")
    df_cost_fc_granular_subset["unit_type"]=df_cost_fc_granular_subset["unit_type"].fillna("None")
    df_cost_fc_granular_subset["service"]=df_cost_fc_granular_subset["service"].fillna("None")
    df_cost_fc_granular_subset["unit"]=df_cost_fc_granular_subset["unit"].fillna("None")

    # #Returns granular unit cost 
    # df_cost_fc_granular_subset.groupby(["opportunity_number","contract_number","unit_catalog_version",'scope', 'service', 'unit_type','schedule_date']).aggregate({"value":"sum","cost":"sum","ic_cost":"sum"}).reset_index().to_excel("fc_unit_level_cost_2024_granular" + date_today + ".xlsx")

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

    # ###
    # #Output of haronized files 
    # ###

    # df_fc_harmonized_billings_to_use=pd.concat([df_billings_fc_24_harmonized], axis=0)
    df_fc_harmonized_costs_to_use=pd.concat([df_cost_24_unit_harmonized,df_cost_24_site_harmonized], axis=0)

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

    df_fc_harmonized_costs_to_use_export_grouped=df_fc_harmonized_costs_to_use_export.groupby(["opportunity_number","unit_serial_number","frequency"]).aggregate({"cost_occurrence_product":"sum","occurrence":"mean"}).reset_index()
    df_fc_harmonized_costs_to_use_export_grouped["cost_weighted"]=df_fc_harmonized_costs_to_use_export_grouped["cost_occurrence_product"]/df_fc_harmonized_costs_to_use_export_grouped["occurrence"] 

    #Include contract and usn in format 

    df_fc_harmonized_costs_to_use_export_grouped=df_fc_harmonized_costs_to_use_export_grouped.merge(oracle_landscape_raw_select[["contract number","unit serial number","unit serial - number only"]],how="left",
                                                                                                    left_on=["unit_serial_number"],
                                                                                                    right_on=["unit serial - number only"])

    #Filter for relevant contracts

    df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped.loc[lambda x: (x["contract number"].isin(relevant_contracts["contract number"])==True),:]   ####or: &(x["unit serial number"].isin(filter_kevin_units)==True)

    df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped_output[["opportunity_number","unit serial number","contract number","frequency","cost_weighted","occurrence"]]


    df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped_output.merge(df_mapping_sr, how="left",on="frequency")

    df_fc_harmonized_costs_to_use_export_grouped_output=df_fc_harmonized_costs_to_use_export_grouped_output[["opportunity_number","unit serial number","contract number","frequency","sr_type_mapped","cost_weighted","occurrence"]]


    df_fc_harmonized_costs_to_use_export_grouped_output["cost_tc"]=df_fc_harmonized_costs_to_use_export_grouped_output["cost_weighted"]*df_fc_harmonized_costs_to_use_export_grouped_output["occurrence"]
    df_fc_outputs_grouped=df_fc_harmonized_costs_to_use_export_grouped_output.groupby(["opportunity_number","unit serial number","contract number",
                                                                                    "frequency"]).aggregate({"cost_tc":"sum","occurrence":"sum"})

    df_fc_outputs_grouped=df_fc_outputs_grouped.reset_index()
    df_fc_outputs_grouped["cost_weighted"]=df_fc_outputs_grouped["cost_tc"]/df_fc_outputs_grouped["occurrence"]

    partioned_outputs.append(df_fc_harmonized_costs_to_use_export_grouped_output)
    partioned_outputs_grouped.append(df_fc_outputs_grouped)


df_fc_harmonized_costs_to_use_export_grouped_output=pd.concat(partioned_outputs)
df_fc_outputs_grouped=pd.concat(partioned_outputs_grouped)

df_fc_harmonized_costs_to_use_export_grouped_output.to_excel("output_costs_export_sample_all_years_frequency_mapping_without_billingtype" + date_today + ".xlsx")

df_fc_outputs_grouped.drop_duplicates().to_excel("grouped_sample_sr_mapping_frequency_mapping_without_billingtype_" + date_today + ".xlsx")


# ##
# #Comparing for selective contracts positions in FC
# ##
# sel_opportunity="1335920"
# #Check service or check scope billing_type
# df_cost_24_unit_harmonized.loc[lambda x: (x["schedule_year"]>2023)&(x["opportunity_number"]==sel_opportunity)&(x["scope"]=="Gen Set_Engine")&(x["billing_type"]=="INNIO_PARTS"),:].groupby(["opportunity_number","service"]).aggregate({"cost":"sum"}).sort_values(by="cost",ascending=False)

# df_fc_harmonized_costs_to_use.groupby(["opportunity_number"]).aggregate({"cost":"sum"})


# df_fc_harmonized_costs_to_use.loc[lambda x: x["opportunity_number"]=="0992496",:].groupby(["opportunity_number","scope","service","billing_type"]).aggregate({"cost":"mean"})

# df_fc_harmonized_costs_to_use.loc[lambda x: (x["scope"]=="Exhaust gas heat exchanger")&(x["opportunity_number"]=="0992496"),:].head()