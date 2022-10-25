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



def load_products(codigo):
    try:

        conn_stg = stg_conn.start()
        conn_sor = sor_conn.start()

        products_dict = {
            "PROD_ID":[],
            "PROD_NAME":[],
            "PROD_DESC":[],
            "PROD_CATEGORY":[],
            "PROD_CATEGORY_ID":[],
            "PROD_CATEGORY_DESC":[],
            "PROD_WEIGHT_CLASS":[],
            "SUPPLIER_ID":[],
            "PROD_STATUS":[],
            "PROD_LIST_PRICE":[],
            "PROD_MIN_PRICE":[],
            "Codigo_etl":[]
        }

        ext_products_stg = pd.read_sql(f'SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_trans WHERE Codigo_Etl = {codigo}', conn_stg)
        if not ext_products_stg.empty:
            for id,name,prodD,prodCate,prodCateId,prodCateD,prodWeiC,supliId,prodS,prodLiPri,prodMinPri in zip(
                ext_products_stg["PROD_ID"],
                ext_products_stg["PROD_NAME"],
                ext_products_stg["PROD_DESC"],
                ext_products_stg["PROD_CATEGORY"],
                ext_products_stg["PROD_CATEGORY_ID"],
                ext_products_stg["PROD_CATEGORY_DESC"],
                ext_products_stg["PROD_WEIGHT_CLASS"],
                ext_products_stg["SUPPLIER_ID"],
                ext_products_stg["PROD_STATUS"],
                ext_products_stg["PROD_LIST_PRICE"],
                ext_products_stg["PROD_MIN_PRICE"]
            ):
                products_dict["PROD_ID"].append(id)
                products_dict["PROD_NAME"].append(name)
                products_dict["PROD_DESC"].append(prodD)
                products_dict["PROD_CATEGORY"].append(prodCate)
                products_dict["PROD_CATEGORY_ID"].append(prodCateId)
                products_dict["PROD_CATEGORY_DESC"].append(prodCateD)
                products_dict["PROD_WEIGHT_CLASS"].append(prodWeiC)
                products_dict["SUPPLIER_ID"].append(supliId)
                products_dict["PROD_STATUS"].append(prodS)
                products_dict["PROD_LIST_PRICE"].append(prodLiPri)
                products_dict["PROD_MIN_PRICE"].append(prodMinPri)
                products_dict["Codigo_etl"].append(codigo)


        if products_dict["PROD_ID"]:
            stg_table = pd.DataFrame(products_dict)
            # stg_table.to_sql("products", conn_sor, if_exists="append", index=False)
            merge(table_name='products', natural_key_cols=['PROD_ID'], dataframe= stg_table, db_context=conn_sor)
            
            # Dispose db connection
            conn_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass