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


def ext_sales():
    try:
        
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

       

        colummns_sales_dict = {
            "prod_id": [],
            "cust_id": [],
            "time_id": [],
            "channel_id": [],
            "promo_id": [],
            "quantity_sold": [],
            "amount_sold": [],
        }

        
        read_salescsv = pd.read_csv(parser.get(cvsSectionName, "SALES"))

        if not read_salescsv.empty:
            for (id, cus_id, ti_id, ch_id, promo_id, q_s, am,) in zip(
                read_salescsv["PROD_ID"],
                read_salescsv["CUST_ID"],
                read_salescsv["TIME_ID"],
                read_salescsv["CHANNEL_ID"],
                read_salescsv["PROMO_ID"],
                read_salescsv["QUANTITY_SOLD"],
                read_salescsv["AMOUNT_SOLD"],
            ):

                colummns_sales_dict["prod_id"].append(id)
                colummns_sales_dict["cust_id"].append(cus_id)
                colummns_sales_dict["time_id"].append(ti_id)
                colummns_sales_dict["channel_id"].append(ch_id)
                colummns_sales_dict["promo_id"].append(promo_id)
                colummns_sales_dict["quantity_sold"].append(q_s)
                colummns_sales_dict["amount_sold"].append(am)
        if colummns_sales_dict["prod_id"]:
            conn.connect().execute("TRUNCATE TABLE channels")
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(colummns_sales_dict)
            df_channels.to_sql("sales", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass