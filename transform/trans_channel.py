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

def trans_channels():
    try:

        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        colummns_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
            "Codigo_etl": []
        }

        ext_channel = pd.read_sql("SELECT CHANNEL_ID,CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels", conn)

        if not ext_channel.empty:
            for id, desc, ch_class, ch_class_id, Codigo_etl in zip(
                    ext_channel["CHANNEL_ID"],
                    ext_channel["CHANNEL_DESC"],
                    ext_channel["CHANNEL_CLASS"],
                    ext_channel["CHANNEL_CLASS_ID"],
            ):
                colummns_dict["channel_id"].append(id)
                colummns_dict["channel_desc"].append(desc)
                colummns_dict["channel_class"].append(ch_class)
                colummns_dict["channel_class_id"].append(ch_class_id)
                colummns_dict["Codigo_etl"].append(Codigo_etl)
        if colummns_dict["channel_id"]:
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(colummns_dict)
            df_channels.to_sql("channels_trans", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
