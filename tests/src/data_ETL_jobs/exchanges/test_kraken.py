"""Module containing tests for Kraken.py 

Classes
-------
TestKraken
"""

import unittest
from src.data_ETL_jobs.exchanges import kraken
from datetime import datetime
import pandas as pd


class TestPoloneix(unittest.TestCase):
    def test_mapdata_DataExist_False(self):
        Kraken = kraken.KrakenExchange()
        Kraken.bucket_interval_value = 1.0
        Kraken.raw_data = [
            [
                1499040000,
                "2",
                "3",
                "1",
                "1.5",
                "0",
                "20",
                5,
            ]
        ]
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
                    2.0,
                    3.0,
                    1.0,
                    1.5,
                    20.0,
                    "BTC",
                    "Kraken",
                    1.0,
                ]
            ],
        )

        Kraken.map_data("BTC")
        self.assertTrue(expected_output.equals(Kraken.data))

    def test_mapdata_DataExist_True(self):
        Kraken = kraken.KrakenExchange()
        Kraken.bucket_interval_value = 1.0
        Kraken.raw_data = [
            [
                1499040000,
                "2",
                "3",
                "1",
                "1.5",
                "0",
                "20",
                5,
            ]
        ]
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
                    "ETH",
                    "Kraken",
                    1.0,
                ],
                [
                    datetime.fromtimestamp(1499040000),
                    2.0,
                    3.0,
                    1.0,
                    1.5,
                    20.0,
                    "BTC",
                    "Kraken",
                    1.0,
                ],
            ],
        )

        Kraken.data = pd.DataFrame(
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
                    "ETH",
                    "Kraken",
                    1.0,
                ]
            ],
        )

        Kraken.map_data("BTC")
        self.assertTrue(expected_output.equals(Kraken.data))


if __name__ == "__main__":
    unittest.main()
