import traceback
import configparser
from extract.extract_channel import ext_channels
from extract.extract_sales import ext_sales
from extract.extract_countries import ext_countries
from extract.extrac_customers import ext_customers
from extract.extract_products import ext_products
from extract.extrac_promotions import ext_promotions
from extract.extract_times import ext_times
from load.load_channel import load_channels
from load.load_customers import load_customers
from load.load_sales import load_sales
from transform.trans_countries import trans_countries
from transform.trans_customers import trans_customers
from transform.trans_products import trans_products
from transform.trans_promotions import trans_promotions
from transform.trans_sales import trans_sales
from transform.trans_times import trans_times
from util import db_connection
import pandas as pd
from load.load_channel import load_channels
from load.load_countries import load_countries
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_times import load_times
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
    # Extraer
    ext_channels()
    ext_sales()
    ext_countries()
    ext_customers()
    ext_products()
    ext_promotions()
    ext_times()
    # Generador de Codigo Etl
    etl = CodigoEtl()
    #Transformar 
    trans_channels(etl)
    trans_countries(etl)
    trans_customers(etl)
    trans_products(etl)
    trans_promotions(etl)
    trans_sales(etl)
    trans_times(etl)

    #Cargar

    load_channels(etl)
    load_countries(etl)
    load_products(etl)
    load_promotions(etl)
    load_times(etl)  
    load_customers(etl)
    load_sales(etl)

except:
    traceback.print_exc()
finally:
    pass
