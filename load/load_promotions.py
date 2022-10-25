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



def load_promotions(codigo):
    try:

        conn_stg = stg_conn.start()
        conn_sor = sor_conn.start()

        promotions_dict = {
            "PROMO_ID":[],
            "PROMO_NAME":[],
            "PROMO_COST":[],
            "PROMO_BEGIN_DATE":[],
            "PROMO_END_DATE":[],
            "Codigo_etl":[]
        }

        ext_promotions_stg = pd.read_sql(f'SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_trans WHERE Codigo_Etl = {codigo}', conn_stg)
        if not ext_promotions_stg.empty:
            for id,name,promCost,promBegDate,promEndDate in zip(
                ext_promotions_stg["PROMO_ID"],
                ext_promotions_stg["PROMO_NAME"],
                ext_promotions_stg["PROMO_COST"],
                ext_promotions_stg["PROMO_BEGIN_DATE"],
                ext_promotions_stg["PROMO_END_DATE"]
            ):
                promotions_dict["PROMO_ID"].append(id)
                promotions_dict["PROMO_NAME"].append(name)
                promotions_dict["PROMO_COST"].append(promCost)
                promotions_dict["PROMO_BEGIN_DATE"].append(promBegDate)
                promotions_dict["PROMO_END_DATE"].append(promEndDate)
                promotions_dict["Codigo_etl"].append(codigo)


        if promotions_dict["PROMO_ID"]:
            stg_table = pd.DataFrame(promotions_dict)
            # stg_table.to_sql("promotions", conn_sor, if_exists="append", index=False)
            merge(table_name='promotions', natural_key_cols=['PROMO_ID'], dataframe= stg_table, db_context=conn_sor)
            
            # Dispose db connection
            conn_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass