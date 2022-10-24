import os
import traceback
from util import db_connection
import pandas as pd
import configparser
from pathlib import Path
from transform.transformation import obt_date
from transform.transformation import obt_month_number

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

def trans_times(codigo):
    try:

        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        times_dict = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[],
            "codigo_etl":[]
        }

        ext_times = pd.read_sql("SELECT TIME_ID,DAY_NAME,DAY_NUMBER_IN_WEEK,DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times", conn)

        if not ext_times.empty:
            for timeId,dName,dnw,dnm,cwn,cmn,cmd,ecm,cqd,cy in zip(
                ext_times["TIME_ID"],
                ext_times["DAY_NAME"],
                ext_times["DAY_NUMBER_IN_WEEK"],
                ext_times["DAY_NUMBER_IN_MONTH"],
                ext_times["CALENDAR_WEEK_NUMBER"],
                ext_times["CALENDAR_MONTH_NUMBER"],
                ext_times["CALENDAR_MONTH_DESC"],
                ext_times["END_OF_CAL_MONTH"],
                ext_times["CALENDAR_QUARTER_DESC"],
                ext_times["CALENDAR_YEAR"]
            ):
                times_dict["time_id"].append(obt_date(timeId))
                times_dict["day_name"].append(dName)
                times_dict["day_number_in_week"].append(dnw)
                times_dict["day_number_in_month"].append(dnm)
                times_dict["calendar_week_number"].append(cwn)
                times_dict["calendar_month_number"].append(cmn)
                times_dict["calendar_month_desc"].append(cmd)
                times_dict["end_of_cal_month"].append(obt_date(ecm)),
                times_dict["calendar_month_name"].append(obt_month_number(cmn)),
                times_dict["calendar_quarter_desc"].append(cqd)
                times_dict["calendar_year"].append(cy)
                times_dict["codigo_etl"].append(codigo)

        if times_dict["time_id"]:
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(times_dict)
            df_channels.to_sql("times_trans", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass