import os
import traceback
from util import db_connection
import pandas as pd
import configparser
from pathlib import Path

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

def trans_countries(codigo):
    try:

        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            "codigo_etl": []
        }

        ext_countries = pd.read_sql("SELECT COUNTRY_ID,COUNTRY_NAME,COUNTRY_REGION,COUNTRY_REGION_ID FROM countries", conn)

        if not ext_countries.empty:
            for id,name,region,region_id in zip(
                    ext_countries["COUNTRY_ID"],
                    ext_countries["COUNTRY_NAME"],
                    ext_countries["COUNTRY_REGION"],
                    ext_countries["COUNTRY_REGION_ID"],
            ):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(name)
                countries_dict["country_region"].append(region)
                countries_dict["country_region_id"].append(region_id)
                countries_dict["codigo_etl"].append(codigo)

        if countries_dict["country_id"]:
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(countries_dict)
            df_channels.to_sql("countries_trans", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass