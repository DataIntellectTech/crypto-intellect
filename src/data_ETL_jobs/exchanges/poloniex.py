"""Module containing class to manage Kucoin API data

Classes
-------
PoloniexExchange
"""
import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

import requests
from base import BaseExchange
from exchange_functions.validate_date import ValidateDate
from datetime import datetime, timedelta
import pandas as pd
import asyncio
from aiohttp import ClientSession
from exchange_functions.limiter import Limiter


class PoloniexExchange(BaseExchange):
    """Class to pull and store data from Poloniex API
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

    candle_sticks_url = "https://api.poloniex.com/markets/"
    api_name = "Poloniex"
    _keys = [
        "TimeStamp",
        "OpenPrice",
        "HighPrice",
        "LowPrice",
        "ClosePrice",
        "Volume",
    ]

    @Limiter(calls_limit=10, period=1)
    async def request_data(self, asset_name, start_date, end_date, session):
        """Method to read data from Kucoin API

        Args:
            asset_name (str): name of crpto currancy
            start_date (str): datetime of when you wish to being pulling data
            end_date (str): datetime  of when you wish to end pulling data

        Returns:
            Saves data to raw_data dictionary stored in the API class
        """
        start_time = ValidateDate.validate_date(start_date)
        end_time = ValidateDate.validate_date(end_date)

        try:
            query_url = f"{self.candle_sticks_url}{asset_name}_USDT/candles?interval=MINUTE_1&startTime={start_time}000&endTime={end_time}000"

            response = await session.get(query_url)
            response.raise_for_status()  # Raises HTTPError if not status code 200

            self.raw_data = await response.json()

        except (
            requests.exceptions.HTTPError,
            requests.exceptions.InvalidURL,
            requests.exceptions.Timeout,
        ) as error:
            print(repr(error))

    def map_data(self, asset_name):
        try:
            mapped_data_list = []

            for entry in self.raw_data:
                add_dict = {
                    "LowPrice": float(entry[0]),
                    "HighPrice": float(entry[1]),
                    "OpenPrice": float(entry[2]),
                    "ClosePrice": float(entry[3]),
                    "Volume": float(entry[5]),
                    "TimeStamp": entry[12],
                }
                add_dict["TimeStamp"] = datetime.fromtimestamp(
                    int(add_dict["TimeStamp"]) // 1000
                )
                add_dict["Coin"] = asset_name
                add_dict["Exchange"] = "Poloniex"
                add_dict["Volume"] = float(add_dict["Volume"])
                add_dict["Bucket_Interval"] = float(self.bucket_interval_value)
                mapped_data_list.append(add_dict)

            if not (isinstance(self.data, pd.DataFrame)):
                self.data = pd.DataFrame(mapped_data_list)
                cols = [
                    "TimeStamp",
                    "OpenPrice",
                    "HighPrice",
                    "LowPrice",
                    "ClosePrice",
                    "Volume",
                    "Coin",
                    "Exchange",
                    "Bucket_Interval",
                ]
                self.data = self.data[cols]

            else:
                df = pd.DataFrame(mapped_data_list)
                cols = [
                    "TimeStamp",
                    "OpenPrice",
                    "HighPrice",
                    "LowPrice",
                    "ClosePrice",
                    "Volume",
                    "Coin",
                    "Exchange",
                    "Bucket_Interval",
                ]
                df = df[cols]
                self.data = pd.concat(
                    [self.data, pd.DataFrame(mapped_data_list)], ignore_index=True
                )

            del self.raw_data

        except IndexError:
            print(
                f"""You have tried to access and out of range list index in {self.api_name} raw_data, please ensure sure you have requested data to fill raw_data and check that keys
                  contains the foloowing entries 'TimeStamp', 'OpenPrice', 'ClosePrice', 'HighPrice', 'LowPrice', 'Volume': """,
                repr(error),
            )

        except KeyError as error:
            print(
                f"""Error when indexing {self.api_name} raw_data please check keys and ensure entries are 
                  'TimeStamp', 'OpenPrice', 'ClosePrice', 'HighPrice', 'LowPrice', 'Volume': """,
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
            seconds=bucketing_seconds
        )
        self.endTime = Date_Time.replace(microsecond=0, second=0) - timedelta(
            seconds=bucketing_seconds - 1
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
                self.data.sort_values(by="TimeStamp", inplace=True)
                self.data = self.aggregator.aggregate_to_bucket_interval(
                    self.data, self.bucket_interval_value
                )

        except AttributeError as error:
            print(repr(error))
