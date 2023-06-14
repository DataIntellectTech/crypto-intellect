"""Module containing functions for pulling data from coingecko api and for saving normalised coin data to database

    Functions
    ---------
    finding_top_n_coins
    finding_specified_coins
    save_to_Coins_table
"""

import requests
import pandas as pd
from datetime import datetime
from sqlalchemy.sql import text
import yaml
from data_ETL_jobs.exchanges.exchange_functions.limiter import Limiter


@Limiter(calls_limit=1, period=60)
async def finding_top_n_coins():
    """Function to pull top n (specified in config) coins by market cap from coingecko api and
        return pandas dataframe of results along with datetime stamp of when API was queried.

    Returns:
    A pandas dataframe of top n coins by market cap with columns of 'Coins', 'Date_Added' amd 'Market_Cap'
    """

    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"
        )
        raw_coin_data = response.json()
        response.raise_for_status()

    except (
        requests.exceptions.HTTPError,
        requests.exceptions.InvalidURL,
        requests.exceptions.Timeout,
    ) as error:
        print(repr(error))
        raise

    coins = {"Coin": [], "MarketCap": []}

    for entry in raw_coin_data:
        coins["Coin"].append(entry["symbol"].upper())
        coins["MarketCap"].append(entry["market_cap"])

    Coins = pd.DataFrame.from_dict(coins)
    Coins = Coins[~Coins["Coin"].str.contains("USD")]
    Coins = Coins[: config["n_coins_to_pull"]]
    Coins["Date_Added"] = datetime.now()

    return Coins


@Limiter(calls_limit=1, period=60)
async def find_specified_coins(coins):
    """Function to pull coins specified in the list_coins_to_save config file from coin_gecko api

    Returns:
    A pandas dataframe of coins listed by market cap with columns of 'Coins', 'Date_Added' and 'Market_Cap'
    """

    coins = ",".join(coins).lower()

    try:
        response = requests.get(
            f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&locale=en",
            {"ids": coins},
        )
        response.raise_for_status()
        raw_coin_data = response.json()

    except (
        requests.exceptions.HTTPError,
        requests.exceptions.InvalidURL,
        requests.exceptions.Timeout,
    ) as error:
        print(repr(error))
        raise error

    coins_dict = {"Coin": [], "MarketCap": []}
    coins = []
    for entry in raw_coin_data:
        coins.append(entry["symbol"].upper())
        coins_dict["Coin"].append(entry["symbol"].upper())
        coins_dict["MarketCap"].append(entry["market_cap"])

    df = pd.DataFrame.from_dict(coins_dict)
    df["Date_Added"] = datetime.now()

    return df


def save_to_Coins_table(dataframe, engine):
    """Function to save in memory pandas dataframe to Coins table in SQL database
    Args:
        dataframe (pandas.dataframe): In memory pandas dataframe that is going to be saved to Coins table in database
        engine (SQLAlchemy,engine): SQLAlchemy engine connected to database
    """
    try:
        dataframe.to_sql("TempCoins", engine, index=False, if_exists="append")
        sql1 = "INSERT INTO Coins (Coin, Date_Added, MarketCap) SELECT t.Coin, t.Date_Added, t.MarketCap FROM TempCoins t WHERE NOT EXISTS (SELECT 1 FROM Coins f WHERE t.Coin = f.Coin)"
        sql2 = "UPDATE Coins SET MarketCap = (SELECT MarketCap FROM TempCoins WHERE Coins.Coin = TempCoins.Coin) WHERE EXISTS (SELECT 1 FROM TempCoins WHERE Coins.Coin = TempCoins.Coin)"
        sql3 = "DROP TABLE TempCoins"

        with engine.begin() as connection:
            connection.execute(text(sql1))
            connection.execute(text(sql2))
            connection.execute(text(sql3))

    except Exception as e:
        print("Failed to save to Coins table check input: ", repr(e))
        with engine.begin() as connection:
            connection.execute(text(sql3))
        raise
