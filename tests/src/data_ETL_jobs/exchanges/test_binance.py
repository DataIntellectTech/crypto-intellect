"""Module containing tests for binance.py 

Classes
-------
TestBinance
"""

from src.data_ETL_jobs.exchanges import binance
from datetime import datetime
import pandas as pd
import unittest


class Test_Binance(unittest.IsolatedAsyncioTestCase):
    def test_mapdata_DataExist_False(self):
        Binance = binance.BinanceExchange()
        Binance.bucket_interval_value = 1.0
        Binance.raw_data = [
            [
                1499040000000,
                "1",
                "1",
                "1",
                "1",
                "0",
                1499644799999,
                "0",
                "1",
                "0",
                "0",
                "0",
            ]
        ]
        expected_return = pd.DataFrame(
            columns=[
                "TimeStamp",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
                "Coin",
                "Exchange",
                "Bucket_Interval",
            ],
            data=[
                [
                    datetime.fromtimestamp(1499040000000 // 1000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Binance",
                    1.0,
                ]
            ],
        )

        Binance.map_data(asset_name="BTC")
        self.assertTrue(expected_return.equals(Binance.data))

    def test_mapdata_DataExist_True(self):
        Binance = binance.BinanceExchange()
        Binance.bucket_interval_value = 1.0
        Binance.raw_data = [
            [
                1499040000000,
                "1",
                "1",
                "1",
                "1",
                "0",
                1499644799999,
                "0",
                "1",
                "0",
                "0",
                "0",
            ]
        ]
        expected_return = pd.DataFrame(
            columns=[
                "TimeStamp",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
                "Coin",
                "Exchange",
                "Bucket_Interval",
            ],
            data=[
                [
                    datetime.fromtimestamp(1499040000000 // 1000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Binance",
                    1.0,
                ],
                [
                    datetime.fromtimestamp(1499040000000 // 1000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Binance",
                    1.0,
                ],
            ],
        )

        Binance.data = pd.DataFrame(
            columns=[
                "TimeStamp",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
                "Coin",
                "Exchange",
                "Bucket_Interval",
            ],
            data=[
                [
                    datetime.fromtimestamp(1499040000000 // 1000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Binance",
                    1.0,
                ]
            ],
        )

        Binance.map_data(asset_name="BTC")
        self.assertTrue(expected_return.equals(Binance.data))


if __name__ == "__main__":
    unittest.main()
