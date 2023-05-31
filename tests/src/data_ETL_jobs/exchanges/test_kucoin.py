"""Module containing tests for Kucoin.py 

Classes
-------
TestKucoin
"""

import unittest
from src.data_ETL_jobs.exchanges import kucoin
from datetime import datetime
import pandas as pd


class TestKucoin(unittest.TestCase):
    def test_mapdata_DataExist_False(self):
        Kucoin = kucoin.KucoinExchange()
        Kucoin.bucket_interval_value = 1.0
        Kucoin.raw_data = [["1499040000", "1", "1", "1", "1", "1", "1"]]
        expected_output = pd.DataFrame(
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
                    datetime.fromtimestamp(1499040000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Kucoin",
                    1.0,
                ]
            ],
        )

        Kucoin.map_data("BTC")
        self.assertTrue(expected_output.equals(Kucoin.data))

    def test_mapdata_DataExist_True(self):
        Kucoin = kucoin.KucoinExchange()
        Kucoin.bucket_interval_value = 1.0
        Kucoin.raw_data = [["1499040000", "1", "1", "1", "1", "1", "1"]]
        expected_output = pd.DataFrame(
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
                    datetime.fromtimestamp(1499040000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Kucoin",
                    1.0,
                ],
                [
                    datetime.fromtimestamp(1499040000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Kucoin",
                    1.0,
                ],
            ],
        )

        Kucoin.data = pd.DataFrame(
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
                    datetime.fromtimestamp(1499040000),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "BTC",
                    "Kucoin",
                    1.0,
                ]
            ],
        )

        Kucoin.map_data("BTC")
        self.assertTrue(expected_output.equals(Kucoin.data))


if __name__ == "__main__":
    unittest.main()
