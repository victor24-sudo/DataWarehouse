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



def load_times(codigo):
    try:

        conn_stg = stg_conn.start()
        conn_sor = sor_conn.start()

        times_dict = {
            "TIME_ID":[],
            "DAY_NAME":[],
            "DAY_NUMBER_IN_WEEK":[],
            "DAY_NUMBER_IN_MONTH":[],
            "CALENDAR_WEEK_NUMBER":[],
            "CALENDAR_MONTH_NUMBER":[],
            "CALENDAR_MONTH_DESC":[],
            "END_OF_CAL_MONTH":[],
            "CALENDAR_MONTH_NAME":[],
            "CALENDAR_QUARTER_DESC":[],
            "CALENDAR_YEAR":[],
            "Codigo_etl": []
        }

        ext_times_stg = pd.read_sql(f'SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_MONTH_NAME,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_trans WHERE Codigo_Etl = {codigo}', conn_stg)
        if not ext_times_stg.empty:
            for timeId,dName,dnw,dnm,cwn,cmn,cmd,ecm,cmname,cqd,cy in zip(
                ext_times_stg["TIME_ID"],
                ext_times_stg["DAY_NAME"],
                ext_times_stg["DAY_NUMBER_IN_WEEK"],
                ext_times_stg["DAY_NUMBER_IN_MONTH"],
                ext_times_stg["CALENDAR_WEEK_NUMBER"],
                ext_times_stg["CALENDAR_MONTH_NUMBER"],
                ext_times_stg["CALENDAR_MONTH_DESC"],
                ext_times_stg["END_OF_CAL_MONTH"],
                ext_times_stg["CALENDAR_MONTH_NAME"],
                ext_times_stg["CALENDAR_QUARTER_DESC"],
                ext_times_stg["CALENDAR_YEAR"]
            ):
                times_dict["TIME_ID"].append(timeId)
                times_dict["DAY_NAME"].append(dName)
                times_dict["DAY_NUMBER_IN_WEEK"].append(dnw)
                times_dict["DAY_NUMBER_IN_MONTH"].append(dnm)
                times_dict["CALENDAR_WEEK_NUMBER"].append(cwn)
                times_dict["CALENDAR_MONTH_NUMBER"].append(cmn)
                times_dict["CALENDAR_MONTH_DESC"].append(cmd)
                times_dict["END_OF_CAL_MONTH"].append(ecm),
                times_dict["CALENDAR_MONTH_NAME"].append(cmname),
                times_dict["CALENDAR_QUARTER_DESC"].append(cqd)
                times_dict["CALENDAR_YEAR"].append(cy)
                times_dict["Codigo_etl"].append(codigo)


        if times_dict["TIME_ID"]:
            stg_table = pd.DataFrame(times_dict)
            # stg_table.to_sql("times", conn_sor, if_exists="append", index=False)
            merge(table_name='times', natural_key_cols=['TIME_ID'], dataframe= stg_table, db_context=conn_sor)
            
            # Dispose db connection
            conn_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass