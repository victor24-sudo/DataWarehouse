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



def load_sales(codigo):
    try:

        conn_stg = stg_conn.start()
        conn_sor = sor_conn.start()

        sales_dict = {
            "PROD_ID":[],
            "CUST_ID":[],
            "TIME_ID":[],
            "CHANNEL_ID":[],
            "PROMO_ID":[],
            "QUANTITY_SOLD":[],
            "AMOUNT_SOLD":[],
            "Codigo_etl": []
        }

        ext_sales_stg = pd.read_sql(f'SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_trans WHERE Codigo_Etl = {codigo}', conn_stg)
        
        # FK de Productos 
        key_subrrogate_products = pd.read_sql_query('SELECT ID_surr, PROD_ID FROM products', conn_sor).set_index('PROD_ID').to_dict()['ID_surr']
        ext_sales_stg['PROD_ID'] = ext_sales_stg['PROD_ID'].apply(lambda key: key_subrrogate_products[key])

        #FK de Customers
        key_subrrogate_customers = pd.read_sql_query('SELECT ID_surr, CUST_ID FROM customers', conn_sor).set_index('CUST_ID').to_dict()['ID_surr']
        ext_sales_stg['CUST_ID'] = ext_sales_stg['CUST_ID'].apply(lambda key: key_subrrogate_customers[key])

        #FK de Times
        key_subrrogate_times = pd.read_sql_query('SELECT ID_surr, TIME_ID FROM times', conn_sor).set_index('TIME_ID').to_dict()['ID_surr']
        ext_sales_stg['TIME_ID'] = ext_sales_stg['TIME_ID'].apply(lambda key: key_subrrogate_times[key])

        #Fk de Channels
        key_subrrogate_channels = pd.read_sql_query('SELECT ID_surr, CHANNEL_ID FROM channels', conn_sor).set_index('CHANNEL_ID').to_dict()['ID_surr']
        ext_sales_stg['CHANNEL_ID'] = ext_sales_stg['CHANNEL_ID'].apply(lambda key: key_subrrogate_channels[key])

        #Fk de Promotions
        key_subrrogate_promotions = pd.read_sql_query('SELECT ID_surr, PROMO_ID FROM promotions', conn_sor).set_index('PROMO_ID').to_dict()['ID_surr']
        ext_sales_stg['PROMO_ID'] = ext_sales_stg['PROMO_ID'].apply(lambda key: key_subrrogate_promotions[key])



        if not ext_sales_stg.empty:
            for prodId,custId,timeId,channelId,promoId,quantiSold,amountSold in zip(
                ext_sales_stg["PROD_ID"],
                ext_sales_stg["CUST_ID"],
                ext_sales_stg["TIME_ID"],
                ext_sales_stg["CHANNEL_ID"],
                ext_sales_stg["PROMO_ID"],
                ext_sales_stg["QUANTITY_SOLD"],
                ext_sales_stg["AMOUNT_SOLD"],
            ):
                sales_dict["PROD_ID"].append(prodId)
                sales_dict["CUST_ID"].append(custId)
                sales_dict["TIME_ID"].append(timeId)
                sales_dict["CHANNEL_ID"].append(channelId)
                sales_dict["PROMO_ID"].append(promoId)
                sales_dict["QUANTITY_SOLD"].append(quantiSold)
                sales_dict["AMOUNT_SOLD"].append(amountSold)
                sales_dict["Codigo_etl"].append(codigo)


        if sales_dict["PROD_ID"]:
            stg_table = pd.DataFrame(sales_dict)
            # stg_table.to_sql("times", conn_sor, if_exists="append", index=False)
            merge(table_name='sales', natural_key_cols=['PROD_ID'], dataframe= stg_table, db_context=conn_sor)
            
            # Dispose db connection
            conn_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass