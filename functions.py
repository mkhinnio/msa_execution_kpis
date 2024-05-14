import pandas as pd
import numpy as np
import pyodbc
import yaml
import openpyxl
import shutil
import itertools
import datetime as dt


from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from statsmodels.api import add_constant
import statsmodels.api as sm
from datetime import date


from os import listdir

def activate_database_driver(driver_version="17", credentials_file="credentials.yml"):
    '''
    uses credentials from local file
    version needs to be specified (likely either 17 or 18)
    '''
    # Load credentials from the YAML file
    with open(credentials_file, 'r') as yaml_file:
        credentials = yaml.safe_load(yaml_file)
    # Access specific credentials
    db_database = credentials['sbomdatabase']['database']
    db_server = credentials['sbomdatabase']['server']
    #db_username = credentials['sbomdatabase']['username']
    #db_password = credentials['sbomdatabase']['password']
    # pyodbc.drivers()
    conn = pyodbc.connect("DRIVER={ODBC Driver " + driver_version + " for SQL Server};"
                        "Server="+db_server+";"
                        "Port=1433;"
                        "database="+db_database+";"
                        "Encrypt=no;"
                        "TrustServerCertificate=no;"
                        #"Uid="+db_username+";"
                        #"Pwd={"+db_password+"};"
                        "Trusted_Connection=no;"
                        "Authentication=ActiveDirectoryIntegrated"
                        )
    return conn


def create_excel_table_for_data_table(writer, df, sheet_name):
    # Write DataFrame to Excel
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    # Get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    # Set up the table
    (max_row, max_col) = df.shape
    column_settings = [{"header": column} for column in df.columns]
    worksheet.add_table(0, 0, max_row, max_col-1, {"columns": column_settings})
    worksheet.set_column(0, max_col-1, 12)


def find_rel_cols_containing_string(substring, df):

    #substring="COGS-Direct Labor"
    cols=[]
    for column in df.columns:
        # Check if the substring is in any of the entries in the column
        try:
            mask = df[column].astype(str).str.contains(substring, case=False, na=False)
            if mask.any():
                print(f"Found matches in {column}:")
                print(df[mask])
                cols.append(column)
            else:
                print(f"No matches found in {column}.")
        except:
            print("No match")
    return cols

def activate_database_driver(driver_version="17", credentials_file="credentials.yml"):
    '''
    uses credentials from local file
    version needs to be specified (likely either 17 or 18)
    '''
    # Load credentials from the YAML file
    with open(credentials_file, 'r') as yaml_file:
        credentials = yaml.safe_load(yaml_file)
    # Access specific credentials
    db_database = credentials['database']['database']
    db_server = credentials['database']['server']
    db_username = credentials['database']['username']
    db_password = credentials['database']['password']
    # pyodbc.drivers()
    conn = pyodbc.connect("DRIVER={ODBC Driver " + driver_version + " for SQL Server};"
                        "Server="+db_server+";"
                        "Port=1433;"
                        "database="+db_database+";"
                        "Encrypt=no;"
                        "TrustServerCertificate=no;"
                        "Uid="+db_username+";"
                        "Pwd={"+db_password+"};"
                        "Trusted_Connection=no;"
                        )
    return conn

def create_comparison_matrix(data, target_column, y_axis_category_column, x_axis_category_column, operation):
    # aggregate tabke
    table_grouped = (data
                        .groupby([y_axis_category_column, x_axis_category_column])
                        .agg({target_column: operation})
                        .reset_index()
                        )
    # restructure table
    table_grouped = table_grouped.pivot(index=y_axis_category_column, columns=x_axis_category_column, values=target_column).reset_index()
    table_grouped.columns.name = None
    table_grouped.columns = [y_axis_category_column] + [x_axis_category_column + "_" + str(header) for header in table_grouped.columns if header != y_axis_category_column]
    return table_grouped

def create_multiple_comparison_matrices(data, target_column, y_axis_category_column, x_axis_category_columns, operation):
    first = True
    for feature in x_axis_category_columns:
        if first == True:
            combined_table = create_comparison_matrix(data=data, target_column=target_column, y_axis_category_column=y_axis_category_column, x_axis_category_column=feature, operation=operation)
            first = False
        else:
            next_table = create_comparison_matrix(data=data, target_column=target_column, y_axis_category_column=y_axis_category_column, x_axis_category_column=feature, operation=operation)
            combined_table = pd.merge(
                combined_table,
                next_table,
                on = y_axis_category_column,
                how = "outer")
    return combined_table

def format_usn_or_esn(data, column_name):
    # strip from characters
    kwargs = {column_name + " formatted" : lambda x: np.where(x[column_name].str.contains("_"), 
                                       x[column_name],
                                       x[column_name].fillna("").apply(lambda y: ''.join(c for c in y if c.isdigit())))}
    data = data.assign(**kwargs)
    
    # replace empty entries with None
    kwargs = {column_name + " formatted" : lambda x: np.where(x[column_name + " formatted"] == "", 
                                       None,
                                       x[column_name + " formatted"])}
    data = data.assign(**kwargs)
    return data

def load_myac_financials(path, today, data_raw_all):
    df_input_financials=pd.read_csv(path)
    df_input_financials["billing_year"]=pd.to_datetime(df_input_financials["Unit Period"]).dt.year


    df_input_financials_grouped=df_input_financials.loc[lambda x: x["Billings cons. Bonus/LD"]!=0,:].groupby(["Unit Serial Number", "Oracle Contract Number","billing_year"]).aggregate({"Cost":"sum","Billings cons. Bonus/LD":"sum","IC Cost":"sum"}).reset_index()
    df_input_financials_grouped["CM"]=(df_input_financials_grouped["Billings cons. Bonus/LD"]-df_input_financials_grouped["Cost"])/df_input_financials_grouped["Billings cons. Bonus/LD"]
    # CEE analysis 

    regional_analysis=data_raw_all.copy()

    regional_analysis=regional_analysis.merge(df_input_financials_grouped,how="left",left_on=["usn","contract_number","year"], right_on=["Unit Serial Number","Oracle Contract Number","billing_year"])
    view=regional_analysis.groupby(["service_region","customer_industry","gas_type_formatted"]).aggregate({"CM":"mean"}).reset_index()

    return df_input_financials_grouped, regional_analysis

def get_contract_definition_h(conn):
    '''
    dwh_dm_myac_unit_definition_h
    '''
    query = """
            SELECT * 
            FROM pgsdwh.sot_gps_dp.dwh_dm_myac_contract_definition_h
            """
    contract_definition_h = pd.read_sql(query, conn) 
    return contract_definition_h
def explode_combinations(df):
    combination_rows = [] 
    for index in df.iterrows(): 
        for combination in product(index): 
            combination_rows.append(combination) 
    return  pd.DataFrame(combination_rows, columns=df.columns) #
 


def predict_values_for_cluster(model, X, variable_list, variable_list_values):
    # prepare industry
    var_1 = variable_list_values[0]
    column_1 = variable_list[0] + "_" + var_1

    # select service region
    var_2 = variable_list_values[1]
    column_2 = variable_list[1] + "_" + var_2

    # select gas type
    var_3 = variable_list_values[2]
    column_3 = variable_list[2] + "_" + var_3

    # bring variable into the correct format
    X_new = pd.DataFrame([False] * len(X.columns), X.columns).T
    X_new[[column_1, column_2, column_3]] = True

    # predict value
    prediction = model.predict([(X_new.iloc[0].values)])[0]

    # create df
    df_prediction = pd.DataFrame(variable_list_values + [prediction], variable_list + ["prediction"]).T

    # export 
    return df_prediction

def get_interaction_terms(df_input,list_dummy_for_interaction):
    names_list=[]
    df_output=df_input.copy()
    for element in list_dummy_for_interaction:
        names_list.append(df_input.columns[df_input.columns.str.startswith(element)].tolist())
    pos_list=list(range(len(names_list)))
    combinations_list=list(itertools.combinations(pos_list,2))

    for tup in combinations_list:
        for col1 in names_list[tup[0]]:
            for col2 in names_list[tup[1]]:
                df_output['dummies_const'+col2 + '_' + col1] = df_output[col1].mul(df_output[col2])
    return df_output, df_input



def create_output_visualizations(df_pred, names_list, variables_displayed):
    d={}
    for name, var_disp in zip(names_list,variables_displayed):
        d[name]=(df_pred[lambda x: x["customer_industry"].isin(["Agriculture", "Infrastructure", "Industrial", "Commercial"])&
                         x["service_region"].isin(["Netherlands", "Germany", "Central Eastern Europe", "Italy", "Spain","Denmark"])]
                               .assign(x_header = lambda x: x["service_region"] + x["gas_type_formatted"])
                               .rename(columns = {"customer_industry": "y_header"})
                               [["x_header", "y_header", var_disp]]
                               .pivot(index = "y_header", columns = "x_header"))
    return list(d.values())

def create_output_visualizations_updated(df_pred, names_list, variables_displayed):
    d={}
    for name, var_disp in zip(names_list,variables_displayed):
        d[name]=(df_pred
                               .assign(x_header = lambda x: x["customer_industry"] + x["gas_type_formatted"])
                               .rename(columns = {"service subregion": "y_header"})
                               [["x_header", "y_header", var_disp]]
                               .pivot(index = "y_header", columns = "x_header"))
    return list(d.values())

def assign_coefficients_segments(base_frame,input_frame):
        input_frame["dummies_const_count"]=input_frame["index"].str.count("dummies_const")
        input_frame_sel_list=input_frame.loc[lambda x: x["dummies_const_count"]==3,"index"].unique()
        input_frame_selected_pairs=input_frame.loc[lambda x: x["index"].isin(input_frame_sel_list)==True,:]
        group_val=pd.concat([base_frame, X[input_frame_sel_list]], axis=1)    

        mapping=dict(zip(input_frame_selected_pairs["index"], input_frame_selected_pairs[0]))

        for column, replacement_value in mapping.items():
            group_val[column]=group_val[column].replace(1,replacement_value)
            a = np.array(group_val[column].values.tolist())
            group_val[column]=np.where(a !=0, replacement_value, a).tolist()
        #pd.wide_to_long(group_coef, i=["service subregion", "gas_type_formatted", "customer_industry"], j="variable")
        group_val_melted=pd.melt(group_val, id_vars=["service subregion", "gas_type_formatted", "customer_industry"], var_name="variable")

        group_val_coef=group_val_melted.drop_duplicates().groupby(["service subregion","gas_type_formatted","customer_industry"]).aggregate({"value":"sum"}).reset_index()
        return group_val_coef


def find_matches(dataframe, column_name, match_dict):
    # This function takes a dictionary 'match_dict' where each key is the name of the new column to be added
    # to the DataFrame and each value is the list of strings to match against the specified column.

    # Define a helper function to find the first match for a given list of keywords
    def first_match(text, keywords):
        for match in keywords:
            if match in text:
                return match
        return None  # Return None if no match is found

    # Loop through each item in the dictionary to apply the helper function
    for col_name, keywords in match_dict.items():
        dataframe[col_name] = dataframe[column_name].apply(first_match, args=(keywords,))

    return dataframe

# Example usage:
# Assuming df is your DataFrame, and you want to check the 'Description' column against different lists of keywords.
match_keywords = {
    'Fruit_Match': ['apple', 'banana', 'cherry'],
    'Dessert_Match': ['pie', 'smoothie', 'cake'] }


def create_excel_table_for_data_table(writer, df, sheet_name):
    # Write DataFrame to Excel
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    # Get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    # Set up the table
    (max_row, max_col) = df.shape
    column_settings = [{"header": column} for column in df.columns]
    worksheet.add_table(0, 0, max_row, max_col-1, {"columns": column_settings})
    worksheet.set_column(0, max_col-1, 12)

def linear_model(X,Y,target_col):
        X = sm.add_constant(X, prepend=False)
        X=X.astype(float)
        model = sm.OLS(Y,X)
        model_fit = model.fit(fit_intercept=False)    
        
        print(f'R Squared of the model for {target_col} is {model_fit.rsquared}')
        intercept_value = model_fit.params[0]
        print(f'Estimated standardized average factor is {intercept_value}')
        coeff_df = model_fit.params.reset_index()
        summary=model_fit.summary()
        tvalues=pd.DataFrame(model_fit.tvalues).reset_index()
        return model_fit, coeff_df, intercept_value, summary , tvalues


def probit(X,Y,target_col):
        X = sm.add_constant(X, prepend=False)
        X=X.astype(float)
        model = sm.Probit(Y,X)
        model_fit = model.fit()    
        
        intercept_value = model_fit.params[0]
        print(f'Estimated standardized average factor is {intercept_value}')
        coeff_df = model_fit.params.reset_index()
        summary=model_fit.summary()
        return model_fit, coeff_df, intercept_value, summary 

def ridge_model(X, Y, target_col):
        model = Ridge() #hadd to switch to Ridge instead of LinearRegression due to abnormally high values for coefficients, as no penalization 
        model.fit(X,Y)
        model.score(X,Y)
        print(f'R Squared of the model is {model.score(X,Y)}')
        intercept_value = model.intercept_

        coeff_df = pd.DataFrame(model.coef_,X.columns,columns=['Coefficient'])
        return model, coeff_df, intercept_value


def modify_corner_point_monthly(df_corner_point):

    for i in range(0,len(df_corner_point)):
        df_corner_point["timestamp"][i]=dt.datetime.fromtimestamp((df_corner_point["timestamp"][i]/1000))

    df_corner_point["year"]=pd.to_datetime(df_corner_point["timestamp"],errors="coerce").dt.year
    df_corner_point["month"]=pd.to_datetime(df_corner_point["timestamp"],errors="coerce").dt.month
    df_corner_point["timestamp"]=pd.to_datetime(df_corner_point["timestamp"],errors="coerce")

    df_corner_point_group_year=df_corner_point.groupby(["asset_id","year","month"]).agg(max_timestamp=("timestamp","max"),min_timestamp=("timestamp","min") ).reset_index()


    df_corner_point_group_year=df_corner_point_group_year.merge(df_corner_point[["asset_id","timestamp","year","month","counter_value"]],how="left",
                                                                left_on=["min_timestamp","asset_id","year","month"], 
                                                                right_on=["timestamp","asset_id","year","month"]).reset_index().rename(columns={"counter_value":"counter_value_min"}).drop("timestamp",axis=1)


    df_corner_point_group_year=df_corner_point_group_year.merge(df_corner_point[["asset_id","timestamp","year","month","counter_value"]],how="left",
                                                                left_on=["max_timestamp","asset_id","year","month"], 
                                                                right_on=["timestamp","asset_id","year","month"]).reset_index().rename(columns={"counter_value":"counter_value_max"}).drop("timestamp",axis=1)
    
    return df_corner_point_group_year


def import_sf_dmpevents(conn):

    query= """
            SELECT sf.in_sitename__c, sf.in_sitecustomername__c, casenumber, sub.in_caseid__c subcase_number, dmp.event_id, dmp.main_event_id, dmp.case_number,
            sf.createddate sf_created_on,sub.createddate sub_created_on,sf.in_dateincidentoccurred__c , sf.closeddate sf_closed_on,
            Dateadd(s, CONVERT(Bigint, dmp.approval_timestamp) / 1000, CONVERT(Datetime, '1-1-1970 00:00:00'))
            AS approval_date,
            Dateadd(s, CONVERT(Bigint, dmp.engine_ready_timestamp) / 1000, CONVERT(Datetime, '1-1-1970 00:00:00'))
            AS engine_ready, in_linktomyplantevent__c
            FROM pgsdwh.sot_gps_dp.dwh_dm_salesforce_case sf
            LEFT JOIN pgsdwh.sot_gps_dp.dwh_dm_salesforce_subcase sub
            ON sf.caseid__c = sub.in_maincasenumber__c
            LEFT JOIN pgsdwh.myplant.maintenance_events_dmp_events dmp
            ON dmp.case_number = sf.caseid__c
            WHERE sf.createddate  >= '2021-01-01' and  sf.createddate <= '2023-12-31' and
            sf.in_business_segment__c like '%Jenbacher%' and sf.in_sitecustomername__c IS NOT NULL AND sf.in_sitename__c <> '412 Training Center'
            ORDER BY sf.createddate desc
            """
    table=pd.read_sql(query, conn)
    return table

def import_dmp_events(conn):
    '''
    event data
    '''
    query = """
            SELECT * 
            FROM myplant.maintenance_events_dmp_events
            """
    dmp_events = pd.read_sql(query, conn) 
    return dmp_events
def get_cost_information_actuals(conn):
    query= """
            select 
trx.service_request_number as sr_trx, 
trx.business_sub_division_dv_gl,
coa.innio_hierarchy_rep_1_dd,
coa.innio_hierarchy_rep_2_dd,
coa.innio_hierarchy_rep_3_dd, 
trx.cost_category_hierarchy_rep_1,
trx.project_code_dv_gl, 
trx.fiscal_period_dv,
trx.ledger_dv, 
trx.account_dd_gl,
coa.ge_hierarchy_dv
,sum(trx.accounted_amt_eur) as amt_eur_trx
from pgsdwh.sot_gps_dp.dwh_dm_idl_trx_actual_margin_se_v as trx 
left join pgsdwh.sot_gps_dp.dwh_dm_idl_coa_account_v as coa on trx.account_dv = coa.account_dv
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_coa_geography_v as coa_geography on trx.geography_dv_gl = coa_geography.geography_dv
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_map_purchase_order_v as map_purchase on trx.purchase_order_rf = map_purchase.purchase_order_rf 
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_map_service_contract_lines_v as map_scl on trx.service_contract_line_rf = map_scl.service_contract_line_rf 
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_map_service_request_v as map_sr on trx.service_request_rf = map_sr.service_request_rf 
where ((trx.fiscal_period_dv like '%23'))
and trx.project_code_dv_gl like 'PWCSA00002'
and trx.service_request_rf is not null
and trx.business_segment_dv_gl = 'Services'

--and (trx.account_dv_gl not like '5020101277' or trx.account_dv_gl not like '5020101278')
and trx.service_request_number not like ''
and coa.account_type_av like 'Expense'
--and trx.service_request_number like '1501031' --> SR filter for testing
group by trx.service_request_number, 
trx.business_sub_division_dv_gl,
coa.innio_hierarchy_rep_1_dd,
coa.innio_hierarchy_rep_2_dd,
coa.innio_hierarchy_rep_3_dd, 
trx.cost_category_hierarchy_rep_1,
trx.project_code_dv_gl, 
trx.fiscal_period_dv,
trx.ledger_dv, 
trx.account_dd_gl, 
coa.ge_hierarchy_dv
    """
    cost_overview = pd.read_sql(query, conn) 
    return cost_overview
#and (trx.innio_hierarchy_rep_3_dd like 'Direct costs of sales - direct costs'
# 'or trx.innio_hierarchy_rep_3_dd like 'Revenues from Equipment third party (gross)'
# or trx.innio_hierarchy_rep_3_dd like 'Revenues from Services intercompany (IC) (gross)'
# or trx.innio_hierarchy_rep_3_dd like 'Revenues from Services third party (gross)')

# and (trx.business_sub_division_dv_gl	like 'Energas'
# or trx.business_sub_division_dv_gl	like 'Jenbacher'
# or trx.business_sub_division_dv_gl	like 'Jenbacher NAM'
# or trx.business_sub_division_dv_gl	like 'NWS')

#  trx.fiscal_period_dv not

def raw_gl_head(conn):
    query= """
            select top 100 * 
from pgsdwh.sot_gps_dp.dwh_dm_idl_trx_actual_margin_se_v 
        """
    cost_overview = pd.read_sql(query, conn) 
    return cost_overview

def get_cost_information_actuals_high_level(conn):
    query= """
            select top 100 * 
from pgsdwh.sot_gps_dp.dwh_dm_idl_trx_actual_margin_se_v as trx 
left join pgsdwh.sot_gps_dp.dwh_dm_idl_coa_account_v as coa on trx.account_dv = coa.account_dv
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_coa_geography_v as coa_geography on trx.geography_dv_gl = coa_geography.geography_dv
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_map_purchase_order_v as map_purchase on trx.purchase_order_rf = map_purchase.purchase_order_rf 
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_map_service_contract_lines_v as map_scl on trx.service_contract_line_rf = map_scl.service_contract_line_rf 
--left join pgsdwh.sot_gps_dp.dwh_dm_idl_map_service_request_v as map_sr on trx.service_request_rf = map_sr.service_request_rf 
where ((trx.fiscal_period_dv like '%23'))
and trx.project_code_dv_gl like 'PWCSA00002'


--and (trx.account_dv_gl not like '5020101277' or trx.account_dv_gl not like '5020101278')
and trx.service_request_number not like ''
and coa.account_type_av like 'Expense'
--and trx.service_request_number like '1501031' --> SR filter for testing
    """
    cost_overview = pd.read_sql(query, conn) 
    return cost_overview

def get_unit_definition_h(conn):
    '''
    Unit - Definition
    dwh_dm_myac_unit_definition_h
    '''
    query = """
            SELECT * 
            FROM pgsdwh.sot_gps_dp.dwh_dm_myac_unit_definition_h
            """
    unit_definition_h = pd.read_sql(query, conn) 
    return unit_definition_h

def get_unit_definition_billings_h(conn):
    '''
    MYAC Data source	Unit - Definition - Billings
    Description	Array of billings defined for the unit
    Azure DWH table	dwh_dm_myac_unit_definition_billings_h
    '''
    query = """
            SELECT * 
            FROM pgsdwh.sot_gps_dp.dwh_dm_myac_unit_definition_billings_h
            """
    unit_definition_billings_h = pd.read_sql(query, conn) 
    return unit_definition_billings_h

def get_opportunity_definition_h(conn):
    '''
    dwh_dm_myac_unit_definition_h
    '''
    query = """
            SELECT * 
            FROM pgsdwh.sot_gps_dp.dwh_dm_myac_opportunity_definition_h
            """
    opportunity_definition_h = pd.read_sql(query, conn) 
    return opportunity_definition_h

def import_oracle_data_from_azure(conn):
    '''
    Landscape-Report is used, as opposed to IB-Report
    '''
    query = """
            SELECT * 
            FROM sot_gps_dp.dwh_rep_mya_agr_contract_landscape
            """
    oracle_landscape_raw = pd.read_sql(query, conn) 
    return oracle_landscape_raw


def import_costs(conn):
    '''
    highlevel data
    '''
    query = """
            SELECT * 
            FROM myplant.engine_forecast_engine_counter_corner_point where counter_type = 'OPH' AND source = 'HISTORY'
            """
    engine_forecast_engine_counter_corner_point = pd.read_sql(query, conn) 
    return engine_forecast_engine_counter_corner_point


def import_corner_point(conn):
    '''
    highlevel data
    '''
    query = """
            SELECT * 
            FROM myplant.engine_forecast_engine_counter_corner_point where counter_type = 'OPH' AND source = 'HISTORY'
            """
    engine_forecast_engine_counter_corner_point = pd.read_sql(query, conn) 
    return engine_forecast_engine_counter_corner_point

def get_contract_definition_h(conn):
    '''
    dwh_dm_myac_unit_definition_h
    '''
    query = """
            SELECT * 
            FROM pgsdwh.sot_gps_dp.dwh_dm_myac_contract_definition_h
            """
    contract_definition_h = pd.read_sql(query, conn) 
    return contract_definition_h

def import_geo_loc(conn):
    '''
    highlevel data
    '''
    query = """
            SELECT * 
            FROM myplant.geo_loc_ib_metadata
            """
    geo_loc_ib_metabase = pd.read_sql(query, conn) 
    return geo_loc_ib_metabase

        
def transform_oracle_data_csa(oracle_landscape_raw):
    # select active MSA-contracts
    oracle_landscape_active_csa_formatted = (oracle_landscape_raw
                                [lambda x: (x["contract status"] == "ACTIVE") & (~x["unit serial number"].str.contains("Dummy"))]
                                .rename(columns = {"unit oks status": "unit status oracle"}) # has been renamed
                                #[["contract number", "contract type oracle", "customer name", "unit status oracle", "unit status ib", "unit serial number", "engine serial number", "eot date"]] 
                                )
    # format usn and esn
    oracle_landscape_active_csa_formatted = (format_usn_or_esn(oracle_landscape_active_csa_formatted, "unit serial number")
                                             .rename(columns = {"unit serial number formatted": "usn",
                                                                "unit serial number": "usn_unformatted"}) )
    oracle_landscape_active_csa_formatted = (format_usn_or_esn(oracle_landscape_active_csa_formatted, "engine serial number")
                                             .rename(columns = {"engine serial number formatted": "esn",
                                                                "engine serial number": "esn_unformatted"}) )
    oracle_landscape_active_csa_formatted = oracle_landscape_active_csa_formatted.drop_duplicates()
    # rename columns
    oracle_landscape_active_csa_formatted = oracle_landscape_active_csa_formatted.rename(columns = {
        "contract number": "contract_number",
        "contract type oracle": "oracle_contract_type",
        "unit status oracle": "oracle_unit_status",
        "eot date": "eot_date" })
    # export
    return oracle_landscape_active_csa_formatted


def cost_up_driver_cost(corner_point_input,cost_input):



    df_corner_point_for_modelling_csa_selection=corner_point_input.copy()   #loc[lambda x: x["unit end oph myac"]==59999,:]
    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit start oph myac"]==0,:]

    #analysis 

    df_corner_point_for_modelling_csa_selection.groupby(["effective_start_year"]).agg(min_actual_oph=("actual_oph","min"), max_actual_oph=("actual_oph","max"))

    df_corner_point_for_modelling_csa_selection.loc[lambda x: x["effective_start_year"]==2014,:].groupby(["engine type"]).agg(min_actual_oph=("actual_oph","min"), max_actual_oph=("actual_oph","max"))


    #costs and serialnumber
    costs_serial_number=cost_input.loc[lambda x: (x["Revenue/Cost Category"]=="Unplanned")&(x["GL_Gross_Margin"]=="Expense"),:].groupby(["IB Serial Number"]).aggregate({" AMT EUR ":"sum"}).reset_index()

    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.merge(costs_serial_number, how="left",left_on=["usn"], 
                                                                                                right_on=["IB Serial Number"]).reset_index()
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["IB Serial Number"].isna()==False,:]
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit start oph myac"].isna()==False,:]
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit end oph myac"].isna()==False,:]


    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["year"]==2023,:]
    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["IB Serial Number"].isna()==False,:]

    #use 

    df_corner_point_for_modelling_csa_selection["run_time"]=pd.to_datetime(df_corner_point_for_modelling_csa_selection["effectivecontractstartdate"]).rsub(pd.to_datetime(date.today())).dt.days


    ##########################################################################################
    ##OLS model for unplanned cost expenses 
    ##########################################################################################


    data_raw=df_corner_point_for_modelling_csa_selection.copy()
    data_raw=data_raw.loc[lambda x: x["year"]==2023,:]
    #data_raw=data_raw.loc[lambda x: x["unit start oph myac"]==0,:]

    data_raw["unit end oph myac"]=data_raw["unit end oph myac"].astype(str)
    data_raw["unit start oph myac"]=data_raw["unit start oph myac"].astype(str)
    data_raw["category"]=data_raw["unit start oph myac"]+data_raw["unit end oph myac"]
    data_raw["year"]=data_raw["year"].astype(str)
    data_raw["effective_start_year"]=data_raw["effective_start_year"].astype(str)

    binary_variable_list=["engine type","gas type", "effective_start_year","category", "year"]
    regular_variable_list=["run_time","actual_oph", "counter_value_max"]
    target_col= " AMT EUR "

    # prepare data for linear regression
    X = data_raw[binary_variable_list] 
    X = pd.get_dummies(data=X, drop_first=False)
    X_reg= data_raw[regular_variable_list]
    X = pd.concat([X, X_reg], axis=1)


    Y = data_raw[target_col]

    # Generate model 

    results, coeff_parameter_act_oph, intercept_act_oph, summary, val_sto_ttest = linear_model(X,Y,target_col)

    
    return results, coeff_parameter_act_oph, intercept_act_oph, summary, val_sto_ttest


def min_oph_sensitivity(coeff_parameter_act_oph, group_coef):
        coeff_parameter_minimum_oph_effect=coeff_parameter_act_oph.copy()
        
        match_keywords={"service subregion": group_coef.drop_duplicates()["service subregion"].unique() ,
                        "gas_type_formatted": "_" + group_coef.drop_duplicates()["gas_type_formatted"].unique(),
                        "customer_industry": group_coef.drop_duplicates()["customer_industry"].unique()}

        coeff_parameter_minimum_oph_effect=find_matches(coeff_parameter_minimum_oph_effect,"index",match_keywords)
        coeff_parameter_minimum_oph_effect=coeff_parameter_minimum_oph_effect.assign(flag_minimumoperatinghours=lambda x: np.where(x["index"].str.contains("minimumoperatinghours")==True, True, False))
        coeff_parameter_minimum_oph_effect=coeff_parameter_minimum_oph_effect.loc[lambda x: ((x["flag_minimumoperatinghours"]==True)&((x["service subregion"].isna()==False)|(x["customer_industry"].isna()==False)|(x["gas_type_formatted"].isna()==False)))|(x["index"]=="minimumoperatinghours"),:]
        coeff_parameter_minimum_oph_effect["gas_type_formatted"]=coeff_parameter_minimum_oph_effect["gas_type_formatted"].str.replace("_","")
        #Combine
        base_coefs=group_coef.drop_duplicates()
        list_columns=base_coefs.columns.tolist()
        permutations=[list(combo) for i in range(1, len(list_columns) + 1) for combo in itertools.combinations(list_columns, i) ]

        def seq_merge(base_coefs,params_to_merge,permutations, grouping):
            output=[]
            for i in permutations:            
                input_frame=base_coefs[i].merge(params_to_merge.loc[lambda x: x[grouping]==len(i),i + [0]], how="left",on=i)
                #input_frame=input_frame.dropna()
                output.append(input_frame)
                output_df=base_coefs.copy()
            for i in range(0,len(permutations)):                
                try:
                    output_df=output_df.merge(output[i],how="left",on=[el for el in output[i].columns.tolist() if el!=0]).rename(columns={0:"value_"+str(i)})
                except:
                    print("no merge")
            
            return output_df, output

        rfdf, list_outputs=seq_merge(base_coefs, coeff_parameter_minimum_oph_effect, permutations, "dummies_const_count")
        rfdf=rfdf.drop_duplicates()
        rfdf["effect"]=rfdf.loc[:,[el for el in rfdf.columns.tolist() if "value" in el]].sum(axis=1)
        rfdf["effect"]=rfdf["effect"]+coeff_parameter_minimum_oph_effect.loc[lambda x: x["index"]=="minimumoperatinghours",0].iloc[0]
        return rfdf[["service subregion","gas_type_formatted","customer_industry","effect"]]



def cost_up_from_azure(corner_point_input,cost_input):



    df_corner_point_for_modelling_csa_selection=corner_point_input.copy()   #loc[lambda x: x["unit end oph myac"]==59999,:]
    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit start oph myac"]==0,:]
    unique_oph_end=df_corner_point_for_modelling_csa_selection["unit end oph myac"].value_counts().reset_index().iloc[0:25,:]["unit end oph myac"].unique()
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit end oph myac"].isin(unique_oph_end)==True,:]
    #analysis 

    df_corner_point_for_modelling_csa_selection.groupby(["effective_start_year"]).agg(min_actual_oph=("actual_oph","min"), max_actual_oph=("actual_oph","max"))

    df_corner_point_for_modelling_csa_selection.loc[lambda x: x["effective_start_year"]==2014,:].groupby(["engine type"]).agg(min_actual_oph=("actual_oph","min"), max_actual_oph=("actual_oph","max"))


    #costs and serialnumber
    #costs_serial_number=cost_input.loc[lambda x: (x["Revenue/Cost Category"]=="Unplanned")&(x["GL_Gross_Margin"]=="Expense"),:].groupby(["IB Serial Number"]).aggregate({" AMT EUR ":"sum"}).reset_index()

    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.merge(cost_input, how="left",left_on=["asset_id","year"], 
                                                                                                right_on=["asset_id","year"]).reset_index()
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit start oph myac"].isna()==False,:]
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit end oph myac"].isna()==False,:]


    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["year"]==2023,:]
    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["IB Serial Number"].isna()==False,:]

    #use 

    df_corner_point_for_modelling_csa_selection["run_time"]=pd.to_datetime(df_corner_point_for_modelling_csa_selection["effectivecontractstartdate"]).rsub(pd.to_datetime(df_corner_point_for_modelling_csa_selection["year"].astype(str)+"-"+"12"+"-"+"31")).dt.days


    ##########################################################################################
    ##OLS model for unplanned cost expenses 
    ##########################################################################################


    data_raw=df_corner_point_for_modelling_csa_selection.copy()
    data_raw=data_raw.loc[lambda x: x["run_time"]>0,:]
    #data_raw=data_raw.loc[lambda x: x["unit start oph myac"]==0,:]

    data_raw["unit end oph myac"]=data_raw["unit end oph myac"].astype(str)
    data_raw["unit start oph myac"]=data_raw["unit start oph myac"].astype(str)
    data_raw["category"]=data_raw["unit start oph myac"]+data_raw["unit end oph myac"]
    data_raw["year"]=data_raw["year"].astype(str)
    data_raw["effective_start_year"]=data_raw["effective_start_year"].astype(str)
    data_raw["average_actual_oph"]=data_raw["counter_value_max"]/data_raw["run_time"]*365

    binary_variable_list=["engine type","engine version", "gas_type_formatted","customer_industry", "service subregion", "year","effective_start_year","category"]
    regular_variable_list=["run_time","actual_oph", "counter_value_max"]
    regular_variable_list=["average_actual_oph","actual_oph"]
    target_col= "amt_eur_trx"
    #data_raw[target_col]=data_raw[target_col].fillna(0)
    data_raw=data_raw[regular_variable_list+binary_variable_list+[target_col]].dropna()
    # prepare data for linear regression
    X = data_raw[binary_variable_list] 
    X = pd.get_dummies(data=X, drop_first=False)
    X_reg= data_raw[regular_variable_list]
    X = pd.concat([X, X_reg], axis=1)


    Y = data_raw[target_col]

    # Generate model 

    results, coeff_parameter_act_oph, intercept_act_oph, summary, val_sto_ttest = linear_model(X,Y,target_col)

    
    return results, coeff_parameter_act_oph, intercept_act_oph, summary, val_sto_ttest


def cost_up_from_azure_monthly(corner_point_input,cost_input):



    df_corner_point_for_modelling_csa_selection=corner_point_input.copy()   #loc[lambda x: x["unit end oph myac"]==59999,:]
    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit start oph myac"]==0,:]
    unique_oph_end=df_corner_point_for_modelling_csa_selection["unit end oph myac"].value_counts().reset_index().iloc[0:25,:]["unit end oph myac"].unique()
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit end oph myac"].isin(unique_oph_end)==True,:]
    #analysis 

    df_corner_point_for_modelling_csa_selection.groupby(["effective_start_year"]).agg(min_actual_oph=("actual_oph","min"), max_actual_oph=("actual_oph","max"))

    df_corner_point_for_modelling_csa_selection.loc[lambda x: x["effective_start_year"]==2014,:].groupby(["engine type"]).agg(min_actual_oph=("actual_oph","min"), max_actual_oph=("actual_oph","max"))


    #costs and serialnumber
    #costs_serial_number=cost_input.loc[lambda x: (x["Revenue/Cost Category"]=="Unplanned")&(x["GL_Gross_Margin"]=="Expense"),:].groupby(["IB Serial Number"]).aggregate({" AMT EUR ":"sum"}).reset_index()

    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.merge(cost_input, how="left",left_on=["asset_id","year","month"], 
                                                                                                right_on=["asset_id","year","month"]).reset_index()
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit start oph myac"].isna()==False,:]
    df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["unit end oph myac"].isna()==False,:]


    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["year"]==2023,:]
    #df_corner_point_for_modelling_csa_selection=df_corner_point_for_modelling_csa_selection.loc[lambda x: x["IB Serial Number"].isna()==False,:]

    #use 

    df_corner_point_for_modelling_csa_selection["run_time"]=pd.to_datetime(df_corner_point_for_modelling_csa_selection["effectivecontractstartdate"]).rsub(pd.to_datetime(df_corner_point_for_modelling_csa_selection["year"].astype(str)+"-"+"12"+"-"+"31")).dt.days


    ##########################################################################################
    ##OLS model for unplanned cost expenses 
    ##########################################################################################


    data_raw=df_corner_point_for_modelling_csa_selection.copy()
    data_raw=data_raw.loc[lambda x: x["run_time"]>0,:]
    #data_raw=data_raw.loc[lambda x: x["unit start oph myac"]==0,:]

    data_raw["unit end oph myac"]=data_raw["unit end oph myac"].astype(str)
    data_raw["unit start oph myac"]=data_raw["unit start oph myac"].astype(str)
    data_raw["category"]=data_raw["unit start oph myac"]+data_raw["unit end oph myac"]
    data_raw["year"]=data_raw["year"].astype(str)
    data_raw["month"]=data_raw["month"].astype(str)
    data_raw["effective_start_year"]=data_raw["effective_start_year"].astype(str)
    data_raw["average_actual_oph"]=data_raw["counter_value_max"]/data_raw["run_time"]*365

    binary_variable_list=["engine type","engine version", "gas_type_formatted","customer_industry", "service subregion", "year","month","effective_start_year","category"]
    regular_variable_list=["run_time","actual_oph", "counter_value_max"]
    regular_variable_list=["average_actual_oph","actual_oph"]
    target_col= "amt_eur_trx"
    #data_raw[target_col]=data_raw[target_col].fillna(0)
    data_raw=data_raw[regular_variable_list+binary_variable_list+[target_col]].dropna()
    # prepare data for linear regression
    X = data_raw[binary_variable_list] 
    X = pd.get_dummies(data=X, drop_first=False)
    X_reg= data_raw[regular_variable_list]
    X = pd.concat([X, X_reg], axis=1)


    Y = data_raw[target_col]

    # Generate model 

    results, coeff_parameter_act_oph, intercept_act_oph, summary, val_sto_ttest = linear_model(X,Y,target_col)

    
    return results, coeff_parameter_act_oph, intercept_act_oph, summary, val_sto_ttest


def get_historic_values(archivefolder, sheetname):
    '''
    extract past financials from migrated ones
    '''
    try:
        list_of_relevant_files = [f for f in listdir(archivefolder)]
        migration_tracker = {}
        tracked_list=[]
        for filename in list_of_relevant_files:
            tracked_list=tracked_list+[filename.split("_")[3].split(".")[0]]
        max_date=max(tracked_list)
        filename_current=[f for f in listdir(archivefolder) if max_date in f][0]
        current_file = pd.read_excel(archivefolder + "/" + filename_current, sheet_name=sheetname)
    except:
        current_file=[]
        current_file=pd.DataFrame(current_file)    
    return current_file



def import_oracle_data_from_azure_cleaned(conn):
 
    query = """
            SELECT "contract number", "quote number oracle", "contract type oracle", "contract status", "customer name", "unit oks status", "unit serial number", "engine serial number", "eot date"
            FROM sot_gps_dp.dwh_rep_mya_agr_contract_landscape
            """
    oracle_landscape_raw = pd.read_sql(query, conn)
 
    # rename columns
    oracle_landscape_active_formatted = oracle_landscape_raw.rename(columns = {
        "contract number": "oracle_contract_number",
        "unit oks status": "oracle_unit_status",
        "eot date": "eot_date",
        "contract status": "oracle_contract_type"
    })
   
    oracle_landscape_active_formatted['oracle_contract_number'] = oracle_landscape_active_formatted['oracle_contract_number'].str.replace(r'(_EXP6|_EXP5|_EXP4|_EXP3|_EXP2|_EXP1|_EXP|_REP|_OLD)', '')
    return oracle_landscape_active_formatted
 


 
def harmonization_figures_total_waterfall(oracle_landscape_raw, harmonization_kpi, contract_type_oracle, contract_type_myac, date_filter):
    #Exemptions not_unit_level_execution
    
    not_unit_level_executed_customers=["INDUSTRIAS JUAN F SECCO SA","GREENERGY","BREITENER"]
    not_unit_level_executed_contract_name=["infinis"]
    not_unit_level_executed_installed_at_country=["bangladesh"]
    not_unit_level_usns=oracle_landscape_raw.loc[lambda x: (x["customer name"].str.upper().str.contains("|".join(not_unit_level_executed_customers))==True)|(x["contract name"].str.lower().str.contains("|".join(not_unit_level_executed_contract_name))==True)|(x["installed at country"].str.lower().str.contains("|".join(not_unit_level_executed_installed_at_country))==True),"unit serial - number only"].unique()


    #Oracle filtering 
    # 
    if contract_type_myac!=[]:

        oracle_landscape_raw=oracle_landscape_raw.loc[lambda x: (x["contract type myac"].isin(contract_type_myac)==True)&(x["contract type oracle"].isin(contract_type_oracle)==True),:]
    else:
        oracle_landscape_raw=oracle_landscape_raw.loc[lambda x: (x["contract type oracle"].isin(contract_type_oracle)==True),:]
    ib_types=["Active","Standby","Active Docu incomplete"]
    active_contract_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE"),harmonization_kpi].unique()
    active_unit_oks_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE"),harmonization_kpi].unique()
    active_unit_ib_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["unit status ib"].isin(ib_types)==True),harmonization_kpi].unique()
    active_unit_unit_level_usns_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["unit serial - number only"].isin(not_unit_level_usns)==False)&(x["unit status ib"].isin(ib_types)==True),harmonization_kpi].unique()
    active_unit_not_unit_level_usns_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["unit serial - number only"].isin(not_unit_level_usns)==True)&(x["unit status ib"].isin(ib_types)==True),harmonization_kpi].unique()
    
    active_unit_ib_total=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["unit status ib"].isin(ib_types)==True),harmonization_kpi].unique()
    active_unit_ib_total_unit_level=oracle_landscape_raw.loc[lambda x: (x["engine commissioning date"]<=date_filter)&(x["contract status"]=="ACTIVE")&(x["unit oks status"]=="ACTIVE")&(x["unit status ib"].isin(ib_types)==True)&(x["unit serial - number only"].isin(active_unit_not_unit_level_usns_total)==False),harmonization_kpi].unique()

    not_unit_operationalization="|".join(["customer:"]+not_unit_level_executed_customers+["contract:"]+not_unit_level_executed_contract_name+["country:"]+not_unit_level_executed_installed_at_country)

    

    dict_steerco_today = {}
    dict_steerco_today["KPI"] = contract_type_oracle[0]
    dict_steerco_today["Entries Total, Oracle contract Active"] = len(active_contract_total)
    dict_steerco_today["Entries Total, Oracle contract Active & Unit OKS Active"] = len(active_unit_oks_total)
    dict_steerco_today["Entries Total, Oracle contract Active & Unit OKS Active & IB Active"] = len(active_unit_ib_total)
    dict_steerco_today["thereof: Entries Total, Oracle contract Active & Unit OKS Active & IB Active & non-AB & unit-level-execution"] = len(active_unit_unit_level_usns_total)
    dict_steerco_today["thereof: Entries Total, Oracle contract Active & Unit OKS Active & IB Active & non-AB & not-unit-level-execution"] = len(active_unit_not_unit_level_usns_total)

    dict_operationalization={}
    dict_operationalization["Entries Total, Oracle contract Active"] = "contract status & unit oks status"
    dict_operationalization["Entries Total, Oracle contract Active & Unit OKS Active"] = "contract status & unit oks status"
    dict_operationalization["Entries Total, Oracle contract Active & Unit OKS Active & IB Active"] = "contract status & unit oks status & unit ib status"
    dict_operationalization["thereof: Entries Total, Oracle contract Active & Unit OKS Active & IB Active & non-AB & unit-level-execution"] = f"contract status & unit oks status & unit ib status & not {not_unit_operationalization}"
    dict_operationalization["thereof: Entries Total, Oracle contract Active & Unit OKS Active & IB Active & non-AB & not-unit-level-execution"] = f"contract status & unit oks status & unit ib status & {not_unit_operationalization}"
    
    
    # create todays table and transform
    df_steerco_today = pd.DataFrame.from_dict(dict_steerco_today, orient='index').T.set_index("KPI")
    df_steerco_overview_updated = pd.concat([df_steerco_today])[lambda x: ~x.index.duplicated(keep='last')].T

    df_operationalization = pd.DataFrame.from_dict(dict_operationalization, orient='index')
    df_operationalization = df_operationalization.rename(columns={0:"OPERATIONALIZATION"})

    df_steerco_overview_updated["TIMESTAMP"]=date_filter
    df_steerco_overview_updated=pd.concat([df_steerco_overview_updated,df_operationalization], axis=1)
    df_steerco_overview_updated["CATEGORY"]=contract_type_oracle[0]
    df_steerco_overview_updated=df_steerco_overview_updated.rename(columns={contract_type_oracle[0]:"VALUE"})
    df_steerco_overview_updated=df_steerco_overview_updated.reset_index().rename(columns={"index":"FILTER"})
    df_steerco_overview_updated["KPI"]=harmonization_kpi
    return active_contract_total, active_unit_oks_total, active_unit_ib_total, active_unit_unit_level_usns_total, active_unit_not_unit_level_usns_total, df_steerco_overview_updated



# ###Numbers ALEX

# oracle_landscape = import_oracle_data_from_azure_cleaned(conn)

# # Stripping the prefix from the unit and engine serial number in oracle
# oracle_landscape["unit serial number"] = oracle_landscape["unit serial number"].str.replace("GEJ-", "")
# oracle_landscape["unit serial number"] = oracle_landscape["unit serial number"].str.replace("JEN-", "")
# oracle_landscape["engine serial number"] = oracle_landscape["engine serial number"].str.replace("GEJ-", "")
# oracle_landscape["engine serial number"] = oracle_landscape["engine serial number"].str.replace("JEN-", "")

# ## Filter only for ACTIVE Oracle contracts
# oracle_landscape_active = oracle_landscape.loc[(oracle_landscape["oracle_contract_type"] == "ACTIVE")]
# oracle_landscape_active.drop_duplicates(subset=["oracle_contract_number"], inplace=True)

# # How many ACTIVE contracts are in the database
# print(len(oracle_landscape_active.groupby(['oracle_contract_number'])['oracle_contract_number'].count()))
# print(oracle_landscape_active["unit serial number"].nunique())
# print(oracle_landscape_active.groupby(['contract type oracle','oracle_unit_status'])['oracle_contract_number'].count())

# """
# FWA_T1                             45
# MMP                                18
# --> MSA BILLABLE SHIPPING            2010
# --> MSA PREVENTIVE AND CORRECTIVE     179
# --> MSA USAGE BILLED                  173
# OPERATION & MAINTENANCE             1
# Oil Management                     21
# --> PREVENTIVE AND CORRECTIVE        1507
# --> PREVENTIVE MAINTENANCE            262
# myPlant                             5
# myPlant Care                       39
# """

# # Filter only for MSA and CSA contracts
# oracle_landscape_active = oracle_landscape_active.loc[(
#     (oracle_landscape_active["contract type oracle"].str.contains("PREVENTIVE AND CORRECTIVE")) |
#     (oracle_landscape_active["contract type oracle"].str.contains("PREVENTIVE MAINTENANCE")) |
#     (oracle_landscape_active["contract type oracle"].str.contains("MSA")))]
    
# print(oracle_landscape_active.groupby(['contract type oracle'])['oracle_contract_number'].count())
# print(len(oracle_landscape_active.groupby(['oracle_contract_number'])['oracle_contract_number'].count()))