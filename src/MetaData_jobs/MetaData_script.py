"""Module containing script which will update MetaData table if there is anychanges in the data stored
"""
import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

from MetaData_functions import (
    get_number_of_rows,
    get_number_of_Exchanges,
    get_current_bucket_interval,
    get_current_bucket_interval,
)
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime, timedelta
import yaml
import pandas as pd
import numpy as np

# Establish a connection to database
with open("/src/config.yml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
engine = create_engine(f"{config['database_connection']}/CryptoIntellectDB")

# Establishing a window to check MetaData since the last data check occured
EndTime = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
StartTime = (
    datetime.now() - timedelta(minutes=3 * config["bucketing_interval"])
).strftime("%Y-%m-%d %H:%M:%S")


# Pulling the current lastest entry from the MetaData table
query_current_MetaData = """select * from MetaData order by Id desc limit 1;"""
with engine.connect() as connection:
    current_MetaData = pd.read_sql_query(text(query_current_MetaData), con=connection)
current_MetaData_info = current_MetaData.drop(
    columns=["Id", "Effective_From", "Effective_To", "Assertive_From", "Assertive_To"],
    axis=1,
)

# Find the MetaData statistics for latest data window
Avg_rows = get_number_of_rows(engine=engine, StartTime=StartTime, EndTime=EndTime)
Avg_rows["Rows_per_Minute"] = Avg_rows["Rows_per_Minute"].apply(np.int64)
# num_coins = get_number_of_coins(engine=engine)
num_coins = pd.DataFrame(
    columns=["Number_of_Coins_Tracked"],
    data=[[config["n_coins_to_pull"] + len(config["list_coins_to_pull"])]],
)
num_exchanges = get_number_of_Exchanges(engine=engine)
current_interval = get_current_bucket_interval(
    engine=engine, StartTime=StartTime, EndTime=EndTime
)
current_interval["Bucket_Interval"] = current_interval["Bucket_Interval"].apply(
    np.int64
)
current_interval = get_current_bucket_interval(
    engine=engine, StartTime=StartTime, EndTime=EndTime
)
current_interval["Bucket_Interval"] = current_interval["Bucket_Interval"].apply(
    np.int64
)

info_dataframe_list = [Avg_rows, num_coins, num_exchanges, current_interval]
info_dataframe_list = [Avg_rows, num_coins, num_exchanges, current_interval]
info_dataframe = pd.concat(info_dataframe_list, axis=1)


# Logic to execute if there is no change in MetaData statistics
if info_dataframe.equals(current_MetaData_info) == True:
    print("MetaData Unchanged")
    engine.dispose()


# Logic to execute if there is a change in Metadata statistics
if info_dataframe.equals(current_MetaData_info) == False:
    # Updating the Assertive_To time in the current latest MetaData entry
    assertive_to = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_sql = f"""update MetaData set Assertive_To=('{assertive_to}') order by Id desc limit 1;"""

    with engine.connect() as connection:
        connection.execute(text(update_sql))
        connection.commit()

    # Adding a new entry to the MetaData table to track the chabge in Effective Time of the old MetaData
    current_MetaData["Assertive_To"] = None
    current_MetaData["Assertive_From"] = datetime.now()
    current_MetaData["Effective_To"] = datetime.now()
    current_MetaData["Id"] = current_MetaData["Id"] + 1

    current_MetaData.to_sql("MetaData", con=engine, index=False, if_exists="append")

    # Adding a new entry to MetaData table containing the updated MetaData table
    current_MetaData["Effective_From"] = datetime.now()
    current_MetaData["Effective_To"] = None
    current_MetaData["Assertive_From"] = datetime.now()
    current_MetaData["Assertive_To"] = None

    New_MetaData_list = [
        current_MetaData.drop(
            columns=[
                "Id",
                "Rows_per_Minute",
                "Number_of_Coins_Tracked",
                "Number_of_Exchanges_Tracked",
                "Bucket_Interval",
            ],
            axis=1,
        ),
        Avg_rows,
        num_coins,
        num_exchanges,
        current_interval,
    ]
    New_MetaData = pd.concat(New_MetaData_list, axis=1)

    New_MetaData.to_sql("MetaData", con=engine, index=False, if_exists="append")

    engine.dispose()
