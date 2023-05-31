from sqlalchemy.sql import text
from sqlalchemy.orm import declarative_base, relationship, mapped_column
from sqlalchemy import create_engine, String
from data_ETL_jobs.get_exchanges import get_exchanges_from_config
import yaml
import pandas as pd

Base = declarative_base()


class ExchangeTable(Base):
    """Class containing metadata for Exchanges table"""

    __tablename__ = "Exchanges"

    Exchange = mapped_column(String(50), primary_key=True)
    data_exchange = relationship("DataTable", back_populates="exchange_data")


if __name__ == "__main__":
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    exchanges = get_exchanges_from_config()
    ex_lst = []
    for i in exchanges:
        ex_lst.append(i.api_name)

    df = pd.DataFrame(columns=["Exchange"], data=ex_lst)
    engine = create_engine(f"{config['database_connection']}/CryptoIntellectDB")
    df.to_sql("TempExchanges", engine, index=False, if_exists="append")
    sql1 = "INSERT INTO Exchanges (Exchange) SELECT t.Exchange FROM TempExchanges t WHERE NOT EXISTS (SELECT 1 FROM Exchanges f WHERE t.Exchange = f.Exchange)"
    sql2 = "DROP TABLE TempExchanges"

    with engine.begin() as connection:
        connection.execute(text(sql1))
        connection.execute(text(sql2))
