"""Module which contains the script to run in order to build MetaData table and fill first entry in CryptoDB

Classes
-------
MetaData:
sqlalchemy Base class containing the Schema for the MetaData table
"""


from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, DateTime, Integer
import yaml
from sqlalchemy import create_engine

from datetime import datetime, timedelta
import pandas as pd


Base = declarative_base()
with open("/src/config.yml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
engine = create_engine(f"{config['database_connection']}/CryptoIntellectDB")


class MetaData(Base):
    """class containing metadata for MetaData table"""

    __tablename__ = "MetaData"

    Id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    Effective_From = Column(DateTime, nullable=False)
    Effective_To = Column(DateTime, nullable=True)
    Assertive_From = Column(DateTime, nullable=False)
    Assertive_To = Column(DateTime, nullable=True)
    Rows_per_Minute = Column(Integer, nullable=False)
    Number_of_Coins_Tracked = Column(Integer, nullable=False)
    Number_of_Exchanges_Tracked = Column(Integer, nullable=False)
    Bucket_Interval = Column(Integer, nullable=False)


Base.metadata.create_all(engine)

Date_dataframe = pd.DataFrame(
    columns=[
        "Effective_From",
        "Effective_To",
        "Assertive_From",
        "Assertive_To",
        "Rows_per_Minute",
        "Number_of_Coins_Tracked",
        "Number_of_Exchanges_Tracked",
        "Bucket_Interval",
    ],
    data=[[datetime.now(), None, datetime.now(), None, 0, 0, 0, 0]],
)

Date_dataframe.to_sql("MetaData", con=engine, index=False, if_exists="append")

engine.dispose()
