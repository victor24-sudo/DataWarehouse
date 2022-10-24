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

def trans_products(codigo):
    try:

        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        products_dict = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[],
            "codigo_etl": []
        }

        ext_products = pd.read_sql("SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products", conn)

        if not ext_products.empty:
            for id,name,prodD,prodCate,prodCateId,prodCateD,prodWeiC,supliId,prodS,prodLiPri,prodMinPri in zip(
                ext_products["PROD_ID"],
                ext_products["PROD_NAME"],
                ext_products["PROD_DESC"],
                ext_products["PROD_CATEGORY"],
                ext_products["PROD_CATEGORY_ID"],
                ext_products["PROD_CATEGORY_DESC"],
                ext_products["PROD_WEIGHT_CLASS"],
                ext_products["SUPPLIER_ID"],
                ext_products["PROD_STATUS"],
                ext_products["PROD_LIST_PRICE"],
                ext_products["PROD_MIN_PRICE"]
            ):
                products_dict["prod_id"].append(id)
                products_dict["prod_name"].append(name)
                products_dict["prod_desc"].append(prodD)
                products_dict["prod_category"].append(prodCate)
                products_dict["prod_category_id"].append(prodCateId)
                products_dict["prod_category_desc"].append(prodCateD)
                products_dict["prod_weight_class"].append(prodWeiC)
                products_dict["supplier_id"].append(supliId)
                products_dict["prod_status"].append(prodS)
                products_dict["prod_list_price"].append(prodLiPri)
                products_dict["prod_min_price"].append(prodMinPri)
                products_dict["codigo_etl"].append(codigo)

        if products_dict["prod_id"]:
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(products_dict)
            df_channels.to_sql("products_trans", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass