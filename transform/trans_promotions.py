import os
import traceback
from util import db_connection
import pandas as pd
import configparser
from pathlib import Path
from transform.transformation import obt_date

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

def trans_promotions(codigo):
    try:

        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[],
            "codigo_etl":[]
        }

        ext_promotions = pd.read_sql("SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions", conn)

        if not ext_promotions.empty:
            for id,name,promCost,promBegDate,promEndDate in zip(
                ext_promotions["PROMO_ID"],
                ext_promotions["PROMO_NAME"],
                ext_promotions["PROMO_COST"],
                ext_promotions["PROMO_BEGIN_DATE"],
                ext_promotions["PROMO_END_DATE"]
            ):
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(name)
                promotions_dict["promo_cost"].append(promCost)
                promotions_dict["promo_begin_date"].append(obt_date(promBegDate))
                promotions_dict["promo_end_date"].append(obt_date(promEndDate))
                promotions_dict["codigo_etl"].append(codigo)

        if promotions_dict["promo_id"]:
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(promotions_dict)
            df_channels.to_sql("promotions_trans", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass