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


def ext_countries():
    try:
        
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

       

        colummns_countries_dict = {
             "country_id": [],
            "country_name": [],
            "country_region": [],
            "country_region_id": [],
        }

        
        read_countriescsv = pd.read_csv(parser.get(cvsSectionName, "COUNTRIES"))

        if not read_countriescsv.empty:
            for id, name, reg, reg_id in zip(
                read_countriescsv["COUNTRY_ID"],
                read_countriescsv["COUNTRY_NAME"],
                read_countriescsv["COUNTRY_REGION"],
                read_countriescsv["COUNTRY_REGION_ID"],
            ):

                colummns_countries_dict["country_id"].append(id)
                colummns_countries_dict["country_name"].append(name)
                colummns_countries_dict["country_region"].append(reg)
                colummns_countries_dict["country_region_id"].append(reg_id)
        if colummns_countries_dict["country_id"]:
            conn.connect().execute("TRUNCATE TABLE countries")
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(colummns_countries_dict)
            df_channels.to_sql("countries", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass