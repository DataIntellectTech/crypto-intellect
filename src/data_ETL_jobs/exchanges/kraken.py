"""Module containing class to manage Kraken API data
Classes
-------
KrakenExchange
"""
import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

import pandas as pd
from datetime import datetime, timedelta
from exchange_functions.validate_date import ValidateDate
from base import BaseExchange
import requests
import asyncio
from aiohttp import ClientSession
from exchange_functions.limiter import Limiter


class KrakenExchange(BaseExchange):
    """Class to pull and store data from Binance API
    ...
    Attributes
    ---------
    inherits BaseExchange class
    candle_sticks_url (str): url to the APIs candlestick data
    api_name (str): Name of the exchange API
    Methods
    -------
    request_data(asset_name, start_date,end_date, interval)
    map_data(asset_name, interval)
    get_time_interval(Date_time)
    get_data(self, *Coins, startTime=None, endTime=None)
    aggregate(self)
    pull_data(self, asset_name, startTime, endTime, session)
    """

    candle_sticks_url = "https://api.kraken.com/0/public/OHLC"
    api_name = "Kraken"
    _keys = [
        "TimeStamp",
        "OpenPrice",
        "HighPrice",
        "LowPrice",
        "ClosePrice",
        "Volume",
    ]

    @Limiter(calls_limit=15, period=1)
    async def request_data(self, asset_name, start_date, end_date, session):
        """Method to read data from Binance API
        Args:
            asset_name (str): name of crpto currancy
            start_date (str): datetime of when you wish to being pulling data
            end_date (str): datetime  of when you wish to end pulling data
            end_date (str): datetime  of when you wish to end pulling data
        Returns:
            Saves data to raw_data dictionary stored in the API class
        """

        start_time = ValidateDate.validate_date(start_date)
        end_time = ValidateDate.validate_date(end_date)

        try:
            query_url = f"{self.candle_sticks_url}?pair={asset_name}USDT&interval=1&since={start_time}"
            response = await session.get(query_url)
            raw_data = await response.json()
            response.raise_for_status()

            if asset_name == "BTC":
                self.raw_data = raw_data["result"]["XBTUSDT"]
            else:
                self.raw_data = raw_data["result"][f"{asset_name}USDT"]

            self.raw_data = [
                entry for entry in self.raw_data if int(entry[0]) <= int(end_time)
            ]

        except (
            requests.exceptions.HTTPError,
            requests.exceptions.InvalidURL,
            requests.exceptions.Timeout,
        ) as error:
            print(repr(error))

    def map_data(self, asset_name):
        """Method to map raw_data to a normalised format for analysis
        Args:
            asset_name (str): name of crypto currancy you wish to map
        Returns:
            Saves normalised data to normalised_data dictionary in API class
        """
        try:
            add_list = []

            for entry in self.raw_data:
                del entry[5]
                add_dict = {}

                for i in range(len(self._keys)):
                    add_dict[self._keys[i]] = float(entry[i])

                add_dict["TimeStamp"] = datetime.fromtimestamp(
                    int(add_dict["TimeStamp"])
                )
                add_dict["Coin"] = asset_name
                add_dict["Exchange"] = "Kraken"
                add_dict["Bucket_Interval"] = float(self.bucket_interval_value)
                add_list.append(add_dict)

            if not (isinstance(self.data, pd.DataFrame)):
                self.data = pd.DataFrame(add_list)
            else:
                self.data = pd.concat(
                    [self.data, pd.DataFrame(add_list)], ignore_index=True
                )

            del self.raw_data

        except IndexError as error:
            print(
                f"""You have tried to access and out of range list index in {self.api_name} raw_data, please ensure sure you have requested data to fill raw_data and check that keys
                  contains the following entries 'OpenTime', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice', 'Volume': """,
                repr(error),
            )
        except KeyError as error:
            print(
                f"""Error when indexing {self.api_name} raw_data please check keys and ensure entries are 
                  'OpenTime', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice', 'Volume': """,
                repr(error),
            )

    def get_time_interval(self, Date_Time):
        """Function to generate the previous n (specified in config) minutes start timestamp and end timestamp based on the input Date_Time. Example: for n =5
        input 2023-01-01 09:30:00
        starttime 2023-01-01 09:20:00
        endtime 2023-01-01 09:25:00
        Args:
            Date_Time (_datetime_): a datetimestamp
        """
        bucketing_seconds = self.bucket_interval_value * 60

        self.startTime = Date_Time.replace(microsecond=0, second=0) - timedelta(
            seconds=2 * bucketing_seconds - 59
        )
        self.endTime = Date_Time.replace(microsecond=0, second=0) - timedelta(
            seconds=bucketing_seconds
        )

    async def get_data(self, *Coins, startTime=None, endTime=None):
        try:
            if startTime == None:
                startTime = self.startTime
            if endTime == None:
                endTime = self.endTime

            async with ClientSession() as session:
                await asyncio.gather(
                    *(
                        self.pull_data(
                            asset_name=coin,
                            startTime=startTime.strftime("%Y-%m-%d %H:%M:%S"),
                            endTime=endTime.strftime("%Y-%m-%d %H:%M:%S"),
                            session=session,
                        )
                        for coin in Coins
                    )
                )

            if self.bucket_interval_value != 1:
                self.data.sort_values(by="OpenTime", inplace=True)
                self.aggregator.aggregate_to_bucket_interval(
                    self.data, self.bucket_interval_value
                )

        except AttributeError as error:
            print(repr(error))
