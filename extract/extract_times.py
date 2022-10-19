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


def ext_times():
    try:
        
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

       

        colummns_times_dict = {
            "time_id": [],
            "day_name": [],
            "day_number_in_week": [],
            "day_number_in_month": [],
            "calendar_week_number": [],
            "calendar_month_number": [],
            "calendar_month_desc": [],
            "end_of_cal_month": [],
            "calendar_quarter_desc": [],
            "calendar_year": [],
        }

        
        read_timescsv = pd.read_csv(parser.get(cvsSectionName, "TIMES"))

        if not read_timescsv.empty:
            for (id,name,day_nbr_w,day_nbr_m,cal_w,cal_m,cal_month_desc,end,qua_desc,cal_yr) in zip(
                read_timescsv["TIME_ID"],
                read_timescsv["DAY_NAME"],
                read_timescsv["DAY_NUMBER_IN_WEEK"],
                read_timescsv["DAY_NUMBER_IN_MONTH"],
                read_timescsv["CALENDAR_WEEK_NUMBER"],
                read_timescsv["CALENDAR_MONTH_NUMBER"],
                read_timescsv["CALENDAR_MONTH_DESC"],
                read_timescsv["END_OF_CAL_MONTH"],
                read_timescsv["CALENDAR_QUARTER_DESC"],
                read_timescsv["CALENDAR_YEAR"],
            ):
                colummns_times_dict["time_id"].append(id)
                colummns_times_dict["day_name"].append(name)
                colummns_times_dict["day_number_in_week"].append(day_nbr_w)
                colummns_times_dict["day_number_in_month"].append(day_nbr_m)
                colummns_times_dict["calendar_week_number"].append(cal_w)
                colummns_times_dict["calendar_month_number"].append(cal_m)
                colummns_times_dict["calendar_month_desc"].append(cal_month_desc)
                colummns_times_dict["end_of_cal_month"].append(end)
                colummns_times_dict["calendar_quarter_desc"].append(qua_desc)
                colummns_times_dict["calendar_year"].append(cal_yr)

        if colummns_times_dict["time_id"]:
            conn.connect().execute("TRUNCATE TABLE times")
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(read_timescsv)
            df_channels.to_sql("times", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass