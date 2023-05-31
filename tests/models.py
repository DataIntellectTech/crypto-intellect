from sqlalchemy import Column, String, DateTime, Float, ForeignKey
from sqlalchemy.orm import declarative_base, mapped_column, relationship

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
    MarketCap = Column(Float, nullable=False)
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
