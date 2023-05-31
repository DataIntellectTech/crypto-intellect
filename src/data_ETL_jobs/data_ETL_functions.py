"""Module containing function to save exchange.data pandas dataframe to Data table in MySql DB

Functions
---------
Save_to_Data
"""

from sqlalchemy.sql import text
from sqlalchemy import exc
from time import sleep
import secrets


def add_UniqueID(dataframe):
    """Function to add a unique ID to input pandas dataframe

    Args:
        dataframe (pandas.DataFrame): input pandas dataframe must contain columns;
        'TimeStamp', 'Coin', 'Exchange' and 'Bucket_Interval'
    """
    try:
        cols = ["TimeStamp", "Coin", "Exchange", "Bucket_Interval"]
        dataframe["Unique_ID"] = dataframe[cols].apply(
            lambda row: "_".join(row.values.astype(str)), axis=1
        )
    except TypeError as error:
        print("add_UniqueID input data is not a pandas dataframe: ", repr(error))


def Save_to_Data(dataframe, engine):
    """Function to save in memory pandas dataframe to database

    Args:
        dataframe (pandas.dataframe): In memory pandas data frame that is going to be saved to Data table in DB
        engine (SQLAlchemy,engine): SQLAlchemy engine connected to DB
    """
    try:
        add_UniqueID(dataframe=dataframe)

        temp_table_postfix = secrets.token_hex(4)
        temp_table_stage = "TempDataStage_" + temp_table_postfix
        temp_table_unique = "TempDataUnique_" + temp_table_postfix

        with engine.begin() as connection:
            dataframe.to_sql(
                temp_table_stage, con=connection, index=False, if_exists="append"
            )
            sql_insert(connection, temp_table_stage, temp_table_unique)

    except AttributeError as error:
        print(repr(error))


def sql_insert(connection, temp_table_stage, temp_table_unique):
    """Function containing SQL logic to be executed when saving to Klines_Data table to ensure no duplicate entries are saved

    Args:
        connection (sqlalchemy.engine.connect): SqlAlchemy connection
        temp_table_stage (_str_): string name of temp stage table
        temp_table_unique (_str_): string name of temp stage table
    """
    attempts = 0
    while attempts <= 5:
        try:
            sql1 = f"""CREATE TABLE IF NOT EXISTS {temp_table_unique} AS SELECT DISTINCT (s.Unique_ID), s.Exchange, s.Coin, s.TimeStamp, s.OpenPrice, s.HighPrice, s.LowPrice, s.ClosePrice, 
                            s.Volume, s.Bucket_Interval FROM {temp_table_stage} s"""

            sql2 = f"""INSERT INTO Klines_Data (Unique_ID, Exchange, Coin, TimeStamp, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, Bucket_Interval)
                                SELECT t.Unique_ID, t.Exchange, t.Coin, t.TimeStamp, t.OpenPrice, t.HighPrice, t.LowPrice, t.ClosePrice, t.Volume, t.Bucket_Interval
                                FROM {temp_table_unique} t WHERE NOT EXISTS (SELECT 1 FROM Klines_Data f WHERE t.Unique_ID = f.Unique_ID)"""

            sql3 = f"""DROP TABLE {temp_table_stage}"""

            sql4 = f"""DROP TABLE {temp_table_unique}"""

            connection.execute(text(sql1))
            connection.execute(text(sql2))
            connection.execute(text(sql3))
            connection.execute(text(sql4))
            break

        except exc.OperationalError as e:
            if "Deadlock found when trying to get lock" in str(e):
                attempts += 1
                print(f"Deadlock detected. Retrying in 1s (attempt {attempts}/5)...")
                sleep(1)
            elif "Lock wait timeout exceeded; try restarting transaction" in str(e):
                attempts += 1
                print(f"Table Locked. Retrying in 1s (attempt {attempts}/5)...")
                sleep(1)
            else:
                print("Failed to save to Data table", e)
                connection.rollback()
                raise

        except exc.SQLAlchemyError as e:
            print("General error caught during insert: ", repr(e))
            connection.execute(text(sql3))
            connection.execute(text(sql4))
            break

    else:
        connection.execute(text(sql3))
        connection.execute(text(sql4))
        raise exc.OperationalError("Deadlock could not be resolved.")
