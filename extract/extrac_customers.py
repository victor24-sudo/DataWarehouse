import traceback
from util import db_connection
import pandas as pd
import configparser


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


def ext_customers():
    try:
        
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

       

        colummns_customers_dict = {
            "cust_id": [],
            "cust_first_name": [],
            "cust_last_name": [],
            "cust_gender": [],
            "cust_year_of_birth": [],
            "cust_marital_status": [],
            "cust_street_address": [],
            "cust_postal_code": [],
            "cust_city": [],
            "cust_state_province": [],
            "country_id": [],
            "cust_main_phone_number": [],
            "cust_income_level": [],
            "cust_credit_limit": [],
            "cust_email": [],
        }

        
        read_customerscsv = pd.read_csv(parser.get(cvsSectionName, "CUSTOMERS"))

        if not read_customerscsv.empty:
            for (id,f_name,last_name,gender,y_birth,marital_status,street,postal,city,state_province,country_id,phone_number,income,credit,email) in zip(
                read_customerscsv["CUST_ID"],
                read_customerscsv["CUST_FIRST_NAME"],
                read_customerscsv["CUST_LAST_NAME"],
                read_customerscsv["CUST_GENDER"],
                read_customerscsv["CUST_YEAR_OF_BIRTH"],
                read_customerscsv["CUST_MARITAL_STATUS"],
                read_customerscsv["CUST_STREET_ADDRESS"],
                read_customerscsv["CUST_POSTAL_CODE"],
                read_customerscsv["CUST_CITY"],
                read_customerscsv["CUST_STATE_PROVINCE"],
                read_customerscsv["COUNTRY_ID"],
                read_customerscsv["CUST_MAIN_PHONE_NUMBER"],
                read_customerscsv["CUST_INCOME_LEVEL"],
                read_customerscsv["CUST_CREDIT_LIMIT"],
                read_customerscsv["CUST_EMAIL"],
            ):
                colummns_customers_dict["cust_id"].append(id)
                colummns_customers_dict["cust_first_name"].append(f_name)
                colummns_customers_dict["cust_last_name"].append(last_name)
                colummns_customers_dict["cust_gender"].append(gender)
                colummns_customers_dict["cust_year_of_birth"].append(y_birth)
                colummns_customers_dict["cust_marital_status"].append(marital_status)
                colummns_customers_dict["cust_street_address"].append(street)
                colummns_customers_dict["cust_postal_code"].append(postal)
                colummns_customers_dict["cust_city"].append(city)
                colummns_customers_dict["cust_state_province"].append(state_province)
                colummns_customers_dict["country_id"].append(country_id)
                colummns_customers_dict["cust_main_phone_number"].append(phone_number)
                colummns_customers_dict["cust_income_level"].append(income)
                colummns_customers_dict["cust_credit_limit"].append(credit)
                colummns_customers_dict["cust_email"].append(email)

        if colummns_customers_dict["cust_id"]:
            conn.connect().execute("TRUNCATE TABLE customers")
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(colummns_customers_dict)
            df_channels.to_sql("customers", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass