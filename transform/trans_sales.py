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

def trans_sales(codigo):
    try:

        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
            "codigo_etl":[]
        }

        ext_sales = pd.read_sql("SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales", conn)

        if not ext_sales.empty:
            for prodId,custId,timeId,channelId,promoId,quantiSold,amountSold in zip(
                ext_sales["PROD_ID"],
                ext_sales["CUST_ID"],
                ext_sales["TIME_ID"],
                ext_sales["CHANNEL_ID"],
                ext_sales["PROMO_ID"],
                ext_sales["QUANTITY_SOLD"],
                ext_sales["AMOUNT_SOLD"],
            ):
                sales_dict["prod_id"].append(prodId)
                sales_dict["cust_id"].append(custId)
                sales_dict["time_id"].append(obt_date(timeId))
                sales_dict["channel_id"].append(channelId)
                sales_dict["promo_id"].append(promoId)
                sales_dict["quantity_sold"].append(quantiSold)
                sales_dict["amount_sold"].append(amountSold)
                sales_dict["codigo_etl"].append(codigo)

        if sales_dict["prod_id"]:
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(sales_dict)
            df_channels.to_sql("sales_trans", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass