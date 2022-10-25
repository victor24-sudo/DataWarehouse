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



def load_countries(codigo):
    try:

        conn_stg = stg_conn.start()
        conn_sor = sor_conn.start()

        colummns_dict = {
            "COUNTRY_ID":[],
            "COUNTRY_NAME":[],
            "COUNTRY_REGION":[],
            "COUNTRY_REGION_ID":[],
            "Codigo_etl":[]
        }

        ext_countries_stg = pd.read_sql(f"SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,COUNTRY_REGION_ID FROM countries_trans WHERE Codigo_etl = {codigo}", conn_stg)

        if not ext_countries_stg.empty:
            for id,name,region,region_id in zip(
                    ext_countries_stg["COUNTRY_ID"],
                    ext_countries_stg["COUNTRY_NAME"],
                    ext_countries_stg["COUNTRY_REGION"],
                    ext_countries_stg["COUNTRY_REGION_ID"],
            ):
                colummns_dict["COUNTRY_ID"].append(id)
                colummns_dict["COUNTRY_NAME"].append(name)
                colummns_dict["COUNTRY_REGION"].append(region)
                colummns_dict["COUNTRY_REGION_ID"].append(region_id)
                colummns_dict["Codigo_etl"].append(codigo)


        if colummns_dict["COUNTRY_ID"]:
            stg_table = pd.DataFrame(colummns_dict)
            # stg_table.to_sql("countries", conn_sor, if_exists="append", index=False)
            merge(table_name='countries', natural_key_cols=['COUNTRY_ID'], dataframe= stg_table, db_context=conn_sor)
            
            # Dispose db connection
            conn_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass