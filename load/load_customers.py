from util import db_connection
import traceback
import configparser
import pandas as pd

from util.sql import merge

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

db_sectionName = "DbConnection"
sor_conn = db_connection.Db_Connection(
    parser.get(db_sectionName, "Type"),
    parser.get(db_sectionName, "Host"),
    parser.get(db_sectionName, "Port"),
    parser.get(db_sectionName, "User"),
    parser.get(db_sectionName, "Password"),
    parser.get(db_sectionName, "Sor"),
)



def load_customers(codigo):
    try:

        conn_stg = stg_conn.start()
        conn_sor = sor_conn.start()

        customers_dict = {
            "CUST_ID":[],
            "CUST_FULLNAME":[],
            "CUST_GENDER":[],
            "CUST_YEAR_OF_BIRTH":[],
            "CUST_MARITAL_STATUS":[],
            "CUST_STREET_ADDRESS":[],
            "CUST_POSTAL_CODE":[],
            "CUST_CITY":[],
            "CUST_STATE_PROVINCE":[],
            "COUNTRY_ID":[],
            "CUST_MAIN_PHONE_NUMBER":[],
            "CUST_INCOME_LEVEL":[],
            "CUST_CREDIT_LIMIT":[],
            "CUST_EMAIL":[],
            "Codigo_etl": []
        }

        ext_customers_stg = pd.read_sql(f'SELECT CUST_ID,CUST_FULLNAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_trans WHERE Codigo_Etl = {codigo}', conn_stg)
        customers_key_subrrogate_countries = pd.read_sql_query('SELECT ID_surr, COUNTRY_ID FROM countries', conn_sor).set_index('COUNTRY_ID').to_dict()['ID_surr']

        ext_customers_stg['COUNTRY_ID'] = ext_customers_stg['COUNTRY_ID'].apply(lambda key: customers_key_subrrogate_countries[key])

        if not ext_customers_stg.empty:
            for id,name,gender,birth,maritalS,address,postalC,city,province,counrtyId,phone,incomeL,creditL,email in zip(
                ext_customers_stg["CUST_ID"],
                ext_customers_stg["CUST_FULLNAME"],
                ext_customers_stg["CUST_GENDER"],
                ext_customers_stg["CUST_YEAR_OF_BIRTH"],
                ext_customers_stg["CUST_MARITAL_STATUS"],
                ext_customers_stg["CUST_STREET_ADDRESS"],
                ext_customers_stg["CUST_POSTAL_CODE"],
                ext_customers_stg["CUST_CITY"],
                ext_customers_stg["CUST_STATE_PROVINCE"],
                ext_customers_stg["COUNTRY_ID"],
                ext_customers_stg["CUST_MAIN_PHONE_NUMBER"],
                ext_customers_stg["CUST_INCOME_LEVEL"],
                ext_customers_stg["CUST_CREDIT_LIMIT"],
                ext_customers_stg["CUST_EMAIL"]
            ):
                customers_dict["CUST_ID"].append(id)
                customers_dict["CUST_FULLNAME"].append(name)
                customers_dict["CUST_GENDER"].append(gender)
                customers_dict["CUST_YEAR_OF_BIRTH"].append(birth)
                customers_dict["CUST_MARITAL_STATUS"].append(maritalS)
                customers_dict["CUST_STREET_ADDRESS"].append(address)
                customers_dict["CUST_POSTAL_CODE"].append(postalC)
                customers_dict["CUST_CITY"].append(city)
                customers_dict["CUST_STATE_PROVINCE"].append(province)
                customers_dict["COUNTRY_ID"].append(counrtyId)
                customers_dict["CUST_MAIN_PHONE_NUMBER"].append(phone)
                customers_dict["CUST_INCOME_LEVEL"].append(incomeL)
                customers_dict["CUST_CREDIT_LIMIT"].append(creditL)
                customers_dict["CUST_EMAIL"].append(email)
                customers_dict["Codigo_etl"].append(codigo)


        if customers_dict["CUST_ID"]:
            stg_table = pd.DataFrame(customers_dict)
            # stg_table.to_sql("times", conn_sor, if_exists="append", index=False)
            merge(table_name='customers', natural_key_cols=['CUST_ID'], dataframe= stg_table, db_context=conn_sor)
            
            # Dispose db connection
            conn_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass