import configparser
import traceback
from transform.trans_countries import trans_countries
from transform.trans_customers import trans_customers
from transform.trans_products import trans_products
from transform.trans_promotions import trans_promotions
from transform.trans_sales import trans_sales
from transform.trans_times import trans_times
from util import db_connection
import pandas as pd

from transform.trans_channel import trans_channels


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

def CodigoEtl():

    conn = stg_conn.start()

    colummns_dict = {
        "usuario": [],
    }

    colummns_dict["usuario"].append("Victor")

    df_process = pd.DataFrame(colummns_dict)
    df_process.to_sql('codigo_etl',conn,if_exists='append',index=False)

    table_process = pd.read_sql('SELECT ID FROM codigo_etl ORDER by ID DESC LIMIT 1', conn)
    
    id = table_process['ID'][0]

    return id


try:
    etl = CodigoEtl()
    trans_channels(etl)
    trans_countries(etl)
    trans_customers(etl)
    trans_products(etl)
    trans_promotions(etl)
    trans_sales(etl)
    trans_times(etl)
    
except:
    traceback.print_exc()
finally:
    pass
