"""Module containing function to fill in missing data from API response"""

import pandas as pd
from sqlalchemy.sql import text
from sqlalchemy import create_engine
import yaml


def feed_forward(dataframe, exchange_name):
    """Some APIs do not return any data if volume is recorded as 0 for that bucket. This function takes data pulled from an API and
    fills in missing coin entries with the most recent entry for that coin in the database and sets volume to 0.

    Args:
        dataframe (pandas.DataFrame): pandas dataframe containing data which has been pulled from API and normalised
        exchange_name (str): name of ther exchange API from which the passed data originated

    Returns:
        pandas.DataFrame containing original data and missing entries

    """
    try:
        with open("/src/config.yml", "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        if config["save_to_database"] == True:
            engine = create_engine(f"{config['database_connection']}/CryptoIntellectDB")

            date = dataframe["TimeStamp"].max()
            sql = f"""SELECT md.* FROM Klines_Data md INNER JOIN 
                (SELECT Coin, MAX(TimeStamp) as LatestTimeStamp from Klines_Data WHERE Exchange='{exchange_name}' AND TimeStamp<='{date}' GROUP BY Coin) latest 
                on md.Coin=latest.Coin and md.TimeStamp=latest.LatestTimeStamp WHERE md.Exchange = '{exchange_name}'
                """
            with engine.connect() as connection:
                latestCoinData = pd.read_sql_query(text(sql), con=connection)

            latestCoinData.drop("Unique_ID", axis=1, inplace=True)
            latestCoinData["Volume"] = 0
            latestCoinData["OpenPrice"] = latestCoinData["ClosePrice"]
            latestCoinData["HighPrice"] = latestCoinData["ClosePrice"]
            latestCoinData["LowPrice"] = latestCoinData["ClosePrice"]
            differences = pd.concat([dataframe, latestCoinData]).drop_duplicates(
                keep=False, subset="Coin", ignore_index=True
            )
            differences["TimeStamp"] = date
            dataframe = pd.concat([dataframe, differences], ignore_index=True)
            dataframe.drop_duplicates(keep="first", inplace=True, ignore_index=True)

            engine.dispose()

            return dataframe

        # else claused used to pass back original dataframe as feed forward feature is not accessible if no database is connected
        else:
            return dataframe

    except TypeError as error:
        print("feed_forward input data is not a pandas dataframe: ", repr(error))
