import pytest
from sqlalchemy import create_engine
import pytest
import pandas as pd
from .models import Base
from sqlalchemy.orm import sessionmaker
from datetime import datetime


def add_data():
    data = {
        "Unique_ID": [
            "2023-03-28 01:00:00_BTC_Binance_1.0",
            "2023-03-28 01:00:00_ETH_Poloneix_1.0",
            "2023-03-28 01:00:00_BTC_Kucoin_1.0",
            "2023-03-31 01:00:00_BTC_Poloneix_1.0",
            "2023-03-31 01:00:00_APE_Kucoin_1.0",
        ],
        "Exchange": ["Binance", "Poloneix", "Kucoin", "Poloneix", "Kucoin"],
        "Coin": ["BTC", "ETH", "BTC", "BTC", "APE"],
        "TimeStamp": [
            datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2023-03-31 01:00:00", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
        ],
        "OpenPrice": [28000, 1700, 28100, 2000.0, 1000.0],
        "HighPrice": [28500, 1789, 28450, 2500.0, 1500.0],
        "LowPrice": [27500, 1660, 28005, 1500.0, 500.0],
        "ClosePrice": [28300, 1710, 28250, 2100.0, 1100.0],
        "Volume": [5.0, 10.0, 7.0, 10.0, 10.0],
        "Bucket_Interval": [1.0, 1.0, 1.0, 1.0, 1.0],
    }

    exchanges = {"Exchange": ["Binance", "Kucoin", "Poloneix"]}

    coins = {
        "Coin": ["BTC", "ETH", "APE"],
        "MarketCap": [1000, 1000, 1000],
        "Date_Added": ["2023-03-01", "2023-03-01", "2023-03-01"],
    }

    return [pd.DataFrame(exchanges), pd.DataFrame(coins), pd.DataFrame(data)]


@pytest.fixture(scope="function")
def setup_database():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def fake_db(setup_database):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    session = setup_database
    engine = session.get_bind()
    (exchanges, coins, data) = add_data()
    exchanges.to_sql("Exchanges", engine, if_exists="append", index=False)
    coins.to_sql("Coins", engine, if_exists="append", index=False)
    data.to_sql("Klines_Data", engine, if_exists="append", index=False)

    yield session

    session.close()
    engine.dispose()
