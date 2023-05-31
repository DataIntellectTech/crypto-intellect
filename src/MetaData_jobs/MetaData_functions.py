"""Module containing functions to find aggregate data which will be passed to MetaData table

Functions
---------
get_number_of_rows
get_number_of_coins
get_number_of_Bytes
get_number_of_Exchanges
    """

from sqlalchemy.sql import text
import pandas as pd


def get_number_of_rows(engine, StartTime, EndTime):
    """Function to pull the number of rows per minute stored in the Klines_Data table

    Args:
        engine (SQLAlchemy,engine): SQLAlchemy engine connected to DB
        StartTime (DateTime): Beginning of window over which MetaData will be checked
        EndTime (DateTime): End of window over which MetaData will be checked
    """

    sql = f"""SELECT ROUND(AVG(Counted_IDS)) AS Rows_per_Minute FROM (SELECT COUNT(Unique_ID) AS Counted_IDS, TimeStamp FROM Klines_Data
              WHERE TimeStamp>='{StartTime}' AND TimeStamp<='{EndTime}' GROUP BY TimeStamp) AS T;"""

    with engine.connect() as con:
        Avg_rows = pd.read_sql_query(text(sql), con=con)

    return Avg_rows


def get_number_of_coins(engine):
    """Function to pull the number of coins currently tracked in database

    Args:
        engine (SQLAlchemy,engine): SQLAlchemy engine connected to DB
    """

    sql = """select count(Coin) AS Number_of_Coins_Tracked from Coins;"""

    with engine.connect() as con:
        num_coins = pd.read_sql_query(text(sql), con=con)

    return num_coins


def get_number_of_Exchanges(engine):
    """Function to pull the number of Exchanges currently tracked in database

    Args:
        engine (SQLAlchemy,engine): SQLAlchemy engine connected to DB
    """

    sql = """select count(Exchange) AS Number_of_Exchanges_Tracked from Exchanges;"""

    with engine.connect() as con:
        num_exchanges = pd.read_sql_query(text(sql), con=con)

    return num_exchanges


def get_current_bucket_interval(engine, StartTime, EndTime):
    """Function to get the current bucketing interval being quired
    Args:
        engine (sqlalchemy.engine): sqlalchemy engine to connect to current database
        StartTime (dateime): start time from where we want to begin checking the bucketing interval being queried
        EndTime (datetime): end time to where we want to stop checking the bucketing interval being queried
    """

    sql = f"""Select Bucket_Interval AS Bucket_Interval from Klines_Data where TimeStamp>='{StartTime}' and TimeStamp<'{EndTime}' order by TimeStamp DESC limit 1"""

    with engine.connect() as con:
        current_interval = pd.read_sql_query(text(sql), con=con)

    return current_interval
