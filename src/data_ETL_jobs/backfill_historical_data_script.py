"""Script to fill database with candlestick data from a specified date up until a specified date. Script has 2 arguments that can be 
passed to it, from (required) and to. If 'to' not passed, default value of datetime.now() is used. 
     Usage:
          backfill_historical_data_script.py -f/--from "yyyy-mm-dd (H:M:S)"
          backfill_historical_data_script.py -f/--from "yyyy-mm-dd (H:M:S)" -t/--to "yyyy-mm-dd (H:M:S)"
"""

from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from exchanges.exchange_functions.validate_date import ValidateDate
from datetime import datetime, timedelta
from data_ETL_functions import Save_to_Data
from get_exchanges import get_exchanges_from_config
from multiprocessing import Pool
import yaml
import argparse
import asyncio

Base = declarative_base()


class CoinTable(Base):
    """Class containing metadata for Coins table"""

    __tablename__ = "Coins"

    Coin = Column(String(10), primary_key=True)

    Date_Added = Column(DateTime, nullable=False)


def parse_args():
    """Function to take in user input for tags --from and --to when script is called from command line

    Returns:
        parsed user arguments
    """
    parser = argparse.ArgumentParser(
        description="Back fill API Data from date specified to date specified or today."
    )
    parser.add_argument("-f", "--from", required=True, help="Date to fill from")
    parser.add_argument(
        "-t",
        "--to",
        required=False,
        help="Date to fill up until",
        default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    return vars(parser.parse_args())


def get_date():
    """Function to parse returned arguements of parse_args into datetime format

    Returns:
        start_date (datetime): datetime of parse_args return
        end_date (datetime): datetime of parse_args return
    """
    args = parse_args()
    start_date = ValidateDate.validate_date(args["from"])
    start_date = datetime.fromtimestamp(start_date)

    end_date = ValidateDate.validate_date(args["to"])
    end_date = datetime.fromtimestamp(end_date)

    if start_date > end_date:
        raise ValueError(
            "Start date greater than end date. Please enter a valid date range"
        )
    if start_date > datetime.now() or end_date > datetime.now():
        raise ValueError("Date entered is in the future. Please enter a valid date.")

    return (start_date, end_date)


def run_data_pulling(exchange, start_date, end_date):
    """Function to pull data from exchange APIs and save to a database

    Args:
        exchange (exchange class): exchange class containing methods to pull data
        start_date (datetime): date from which user wants to begin pulling data
        end_date (datetime): date to which user wants to pull data
    """
    asyncio.run(exchange.get_data(*Coins, startTime=start_date, endTime=end_date))
    Save_to_Data(exchange.data, engine=engine)


if __name__ == "__main__":
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    engine = create_engine(config["root_database_connection"])
    Session = sessionmaker(bind=engine)

    Exchanges = get_exchanges_from_config()

    with Session() as session:
        results = session.query(CoinTable).all()

    Coins = []
    for entry in results:
        Coins.append(entry.Coin)

    (start_date, end_date) = get_date()
    next_date = start_date + timedelta(minutes=config["bucketing_interval"])
    start_date = start_date + timedelta(minutes=1)

    with Pool(initializer=engine.dispose(close=False)) as pool:
        while start_date < end_date:
            args = [
                (Exchanges[0], start_date, next_date),
                (Exchanges[1], start_date, next_date),
                (Exchanges[2], start_date, next_date),
            ]

            try:
                pool.starmap(run_data_pulling, args)
            except Exception as e:
                print("error in multiprocessing: ", e)
            start_date = next_date + timedelta(minutes=1)
            next_date = next_date + timedelta(minutes=config["bucketing_interval"])

    engine.dispose()
