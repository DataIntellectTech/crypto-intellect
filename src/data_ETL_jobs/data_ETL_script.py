"""Script to pull 1 minute data candle stick data from listed APIs and stored coin base, then normalise 
pulled data and save to MySql Database (This script should be scheduled to do this data collection every minute)
    """

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DateTime, BigInteger
from datetime import datetime
from data_ETL_functions import Save_to_Data
from coin_ETL_jobs.coin_functions import finding_top_n_coins, find_specified_coins
from get_exchanges import get_exchanges_from_config
from multiprocessing import Pool
import asyncio
import yaml
import pandas as pd


def query_coins_table(engine):
    Base = declarative_base()

    class CoinTable(Base):
        """Class containing metadata for Coins table"""

        __tablename__ = "Coins"

        Coin = Column(String(10), primary_key=True)
        Date_Added = Column(DateTime, nullable=False)
        MarketCap = Column(BigInteger, nullable=False)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        results = session.query(CoinTable).order_by(desc(CoinTable.MarketCap)).all()

    Coins = []
    for entry in results:
        Coins.append(entry.Coin)

    return Coins


def run_data_pulling_database(exchange):
    """Function to pull data from exchange API and save to database. The time
    window for this data pull will be the latest n minutes defined from config.

    Args:
        exchange (exchange class): exchange class containing methods to pull data
    """
    exchange.get_time_interval(Date_Time=Date_Time)
    asyncio.run(exchange.get_data(*Coins))

    Save_to_Data(exchange.data, engine)


def run_data_pulling_csv(exchange):
    """Function to pull data from exchange API and save to CSVs inside local folder. The time
    window for this data pull will be the latest n minutes defined from config.

    Args:
        exchange (exchange class): exchange class containing methods to pull data
    """
    exchange.get_time_interval(Date_Time=Date_Time)
    asyncio.run(exchange.get_data(*Coins))
    path_to_csv = f"/src/csv_folder/{str(Date_Time.strftime('%Y-%m-%d'))}_{exchange.api_name}_{str(config['bucketing_interval'])}m.csv"
    exchange.data.to_csv(
        path_or_buf=path_to_csv, sep=",", header=False, index=False, mode="a"
    )


if __name__ == "__main__":
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    Exchanges = get_exchanges_from_config()
    Date_Time = datetime.now()

    if config["save_to_database"] == True:
        try:
            engine = create_engine(f"{config['database_connection']}/CryptoIntellectDB")
            Session = sessionmaker(bind=engine)

        except:
            print(
                "Error connecting to database check congif file for connection details"
            )

        Coins = query_coins_table(engine)

        with Pool(initializer=engine.dispose(close=False)) as pool:
            try:
                pool.map(run_data_pulling_database, Exchanges)
            except Exception as e:
                print("error in multiprocessing: ", e)

        engine.dispose()

    if config["save_to_csv"] == True:
        top_n_coins = finding_top_n_coins()

        list_coins = find_specified_coins(config["list_coins_to_pull"])

        Coins_dataframe = pd.concat([top_n_coins, list_coins], axis=0)
        Coins = Coins_dataframe["Coin"].reset_index(drop=True)

        with Pool() as pool:
            try:
                pool.map(run_data_pulling_csv, Exchanges)
            except Exception as e:
                print("error in multiprocessing: ", e)
