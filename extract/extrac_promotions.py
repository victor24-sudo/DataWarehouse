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


def ext_promotions():
    try:
        
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

       

        colummns_products_dict = {
            "promo_id": [],
            "promo_name": [],
            "promo_cost": [],
            "promo_begin_date": [],
            "promo_end_date": [],
        }

        
        read_promocsv = pd.read_csv(parser.get(cvsSectionName, "PROMOTIONS"))

        if not read_promocsv.empty:
            for (id, pr_name, pr_cost, pr_begin, pr_end) in zip(
                read_promocsv["PROMO_ID"],
                read_promocsv["PROMO_NAME"],
                read_promocsv["PROMO_COST"],
                read_promocsv["PROMO_BEGIN_DATE"],
                read_promocsv["PROMO_END_DATE"],
            ):
                colummns_products_dict["promo_id"].append(id)
                colummns_products_dict["promo_name"].append(pr_name)
                colummns_products_dict["promo_cost"].append(pr_cost)
                colummns_products_dict["promo_begin_date"].append(pr_begin)
                colummns_products_dict["promo_end_date"].append(pr_end)

        if colummns_products_dict["promo_id"]:
            conn.connect().execute("TRUNCATE TABLE promotions")
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(read_promocsv)
            df_channels.to_sql("promotions", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
    