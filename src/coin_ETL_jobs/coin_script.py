"""Script to pull data from coingecko api and save to a database"""

import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

from coin_ETL_jobs.coin_functions import (
    save_to_Coins_table,
    find_specified_coins,
    finding_top_n_coins,
)
from sqlalchemy import create_engine
import yaml


if __name__ == "__main__":
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    engine = create_engine(f"{config['database_connection']}/CryptoIntellectDB")

    top_n_Coins = finding_top_n_coins()
    list_Coins = find_specified_coins(config["list_coins_to_pull"])
    save_to_Coins_table(top_n_Coins, engine)
    save_to_Coins_table(list_Coins, engine)

    engine.dispose()
