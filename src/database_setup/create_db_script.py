"""Script to run to set up Crypto database and tables within database (run once on deployment)
"""
import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

from database_setup.create_functions import Create_database, Create_Tables
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import pandas as pd
import yaml

if __name__ == "__main__":
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    try:
        Create_database(database_connection_string=config["database_connection"])
    except:
        print("DataBase not created")

    engine = create_engine(f"{config['database_connection']}/CryptoIntellectDB")
    Create_Tables(engine)

    engine.dispose()
