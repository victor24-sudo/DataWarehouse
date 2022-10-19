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


def ext_products():
    try:
        
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

       

        colummns_products_dict = {
            "prod_id": [],
            "prod_name": [],
            "prod_desc": [],
            "prod_category": [],
            "prod_category_id": [],
            "prod_category_desc": [],
            "prod_weight_class": [],
            "supplier_id": [],
            "prod_status": [],
            "prod_list_price": [],
            "prod_min_price": [],
        }

        
        read_productscsv = pd.read_csv(parser.get(cvsSectionName, "PRODUCTS"))

        if not read_productscsv.empty:
            for (id,name,desc,cat,cat_id,cat_desc,w_class,supp_id,status,list,min) in zip(
                read_productscsv["PROD_ID"],
                read_productscsv["PROD_NAME"],
                read_productscsv["PROD_DESC"],
                read_productscsv["PROD_CATEGORY"],
                read_productscsv["PROD_CATEGORY_ID"],
                read_productscsv["PROD_CATEGORY_DESC"],
                read_productscsv["PROD_WEIGHT_CLASS"],
                read_productscsv["SUPPLIER_ID"],
                read_productscsv["PROD_STATUS"],
                read_productscsv["PROD_LIST_PRICE"],
                read_productscsv["PROD_MIN_PRICE"],
            ):
                colummns_products_dict["prod_id"].append(id)
                colummns_products_dict["prod_name"].append(name)
                colummns_products_dict["prod_desc"].append(desc)
                colummns_products_dict["prod_category"].append(cat)
                colummns_products_dict["prod_category_id"].append(cat_id)
                colummns_products_dict["prod_category_desc"].append(cat_desc)
                colummns_products_dict["prod_weight_class"].append(w_class)
                colummns_products_dict["supplier_id"].append(supp_id)
                colummns_products_dict["prod_status"].append(status)
                colummns_products_dict["prod_list_price"].append(list)
                colummns_products_dict["prod_min_price"].append(min)

        if colummns_products_dict["prod_id"]:
            conn.connect().execute("TRUNCATE TABLE products")
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(read_productscsv)
            df_channels.to_sql("products", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass