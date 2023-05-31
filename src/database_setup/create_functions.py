"""Module containing functions that manage the setup of the Crypto database 
"""

from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column,
    String,
    DateTime,
    ForeignKey,
    Float,
    BigInteger,
    create_engine,
)
from sqlalchemy.orm import relationship, mapped_column
from sqlalchemy.sql import text
from mysql.connector import Error


def Create_database(database_connection_string):
    """Function to create a database to store crypto data in connected MySQL Docker"""
    try:
        engine = create_engine(database_connection_string)
        with engine.begin() as conn:
            sql = "CREATE DATABASE CryptoIntellectDB"
            conn.execute(text(sql))
            print("Database is created")

    except Error as e:
        print("Error while connecting to MySQL on CREATE TABLES: ", e)


def Create_Tables(engine):
    """Function to create tables to match schema in DB

    Args:
        engine (SQLAlchemy.engine): SQLAlchemy engine to connect to DB
    """

    Base = declarative_base()

    class ExchangeTable(Base):
        """Class containing metadata for Exchanges table"""

        __tablename__ = "Exchanges"

        Exchange = mapped_column(String(50), primary_key=True)
        data_exchange = relationship("DataTable", back_populates="exchange_data")

    class CoinTable(Base):
        """Class containing the metadata for Coins table"""

        __tablename__ = "Coins"

        Coin = mapped_column(String(10), primary_key=True)
        data_coin = relationship("DataTable", back_populates="coin_data")
        MarketCap = Column(BigInteger, nullable=False)
        Date_Added = Column(DateTime, nullable=False)

    class DataTable(Base):
        """Class containing the metadata for Data table"""

        __tablename__ = "Klines_Data"

        Unique_ID = Column(String(100), primary_key=True)
        Exchange = mapped_column(ForeignKey("Exchanges.Exchange"))
        Coin = mapped_column(ForeignKey("Coins.Coin"))
        TimeStamp = Column(DateTime, nullable=False)
        OpenPrice = Column(Float, nullable=False)
        HighPrice = Column(Float, nullable=False)
        LowPrice = Column(Float, nullable=False)
        ClosePrice = Column(Float, nullable=False)
        Volume = Column(Float, nullable=False)
        Bucket_Interval = Column(Float, nullable=False)

        exchange_data = relationship("ExchangeTable", back_populates="data_exchange")
        coin_data = relationship("CoinTable", back_populates="data_coin")

    Base.metadata.create_all(engine)
