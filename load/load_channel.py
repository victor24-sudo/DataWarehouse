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



def load_channels(codigo):
    try:

        conn_stg = stg_conn.start()
        conn_sor = sor_conn.start()
        if conn_stg == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn_stg == -2:
            raise Exception("Error trying to connect to cdnastaging")
        if conn_sor == -1:
            raise Exception(f"The database type {sor_conn.type} is not valid")
        elif conn_sor == -2:
            raise Exception("Error trying to connect to cdnastaging")

        colummns_dict = {
            "CHANNEL_ID":[],
            "CHANNEL_DESC":[],
            "CHANNEL_CLASS":[],
            "CHANNEL_CLASS_ID":[],
            "Codigo_etl":[]
        }

        ext_channel_stg = pd.read_sql(f"SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID FROM channels_trans WHERE Codigo_etl = {codigo}", conn_stg)

        if not ext_channel_stg.empty:
            for id, desc, ch_class, ch_class_id in zip(
                    ext_channel_stg["CHANNEL_ID"],
                    ext_channel_stg["CHANNEL_DESC"],
                    ext_channel_stg["CHANNEL_CLASS"],
                    ext_channel_stg["CHANNEL_CLASS_ID"],
            ):
                colummns_dict["CHANNEL_ID"].append(id)
                colummns_dict["CHANNEL_DESC"].append(desc)
                colummns_dict["CHANNEL_CLASS"].append(ch_class)
                colummns_dict["CHANNEL_CLASS_ID"].append(ch_class_id)
                colummns_dict["Codigo_etl"].append(codigo)


        if colummns_dict["CHANNEL_ID"]:
            stg_table = pd.DataFrame(colummns_dict)
            # stg_table.to_sql("channels", conn_sor, if_exists="append", index=False)
            merge(table_name='channels', natural_key_cols=['CHANNEL_ID'], dataframe= stg_table, db_context=conn_sor)
            
            # Dispose db connection
            conn_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
