import os
import traceback
from util import db_connection
import pandas as pd
import configparser
from pathlib import Path
from transform.transformation import join_2_strings
from transform.transformation import obt_gender

parser = configparser.ConfigParser()

parser.read(".properties")

db_sectionName = "DbConnection"
stg_conn = db_connection.Db_Connection(
    parser.get(db_sectionName, "Type"),
    parser.get(db_sectionName, "Host"),
    parser.get(db_sectionName, "Port"),
    parser.get(db_sectionName, "User"),
    parser.get(db_sectionName, "Password"),
    parser.get(db_sectionName, "Stg"),
)
cvsSectionName = "CSVS"

def trans_customers(codigo):
    try:

        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        customers_dict = {
            "cust_id":[],
            "cust_fullname":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_number":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[],
            "codigo_etl": []
        }

        ext_customers = pd.read_sql("SELECT CUST_ID,CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers", conn)

        if not ext_customers.empty:
            for id,name,lastname,gender,birth,maritalS,address,postalC,city,province,counrtyId,phone,incomeL,creditL,email in zip(
                ext_customers["CUST_ID"],
                ext_customers["CUST_FIRST_NAME"],
                ext_customers["CUST_LAST_NAME"],
                ext_customers["CUST_GENDER"],
                ext_customers["CUST_YEAR_OF_BIRTH"],
                ext_customers["CUST_MARITAL_STATUS"],
                ext_customers["CUST_STREET_ADDRESS"],
                ext_customers["CUST_POSTAL_CODE"],
                ext_customers["CUST_CITY"],
                ext_customers["CUST_STATE_PROVINCE"],
                ext_customers["COUNTRY_ID"],
                ext_customers["CUST_MAIN_PHONE_NUMBER"],
                ext_customers["CUST_INCOME_LEVEL"],
                ext_customers["CUST_CREDIT_LIMIT"],
                ext_customers["CUST_EMAIL"]
            ):
                customers_dict["cust_id"].append(id)
                customers_dict["cust_fullname"].append(join_2_strings(name,lastname))
                customers_dict["cust_gender"].append(obt_gender(gender))
                customers_dict["cust_year_of_birth"].append(birth)
                customers_dict["cust_marital_status"].append(maritalS)
                customers_dict["cust_street_address"].append(address)
                customers_dict["cust_postal_code"].append(postalC)
                customers_dict["cust_city"].append(city)
                customers_dict["cust_state_province"].append(province)
                customers_dict["country_id"].append(counrtyId)
                customers_dict["cust_main_phone_number"].append(phone)
                customers_dict["cust_income_level"].append(incomeL)
                customers_dict["cust_credit_limit"].append(creditL)
                customers_dict["cust_email"].append(email)
                customers_dict["codigo_etl"].append(codigo)

        if customers_dict["cust_id"]:
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(customers_dict)
            df_channels.to_sql("customers_trans", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
    