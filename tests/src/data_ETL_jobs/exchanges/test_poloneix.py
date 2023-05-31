"""Module containing tests for Poloneix.py 

Classes
-------
TestPoloneix
"""

import unittest
from src.data_ETL_jobs.exchanges import poloniex
from datetime import datetime
import pandas as pd


class TestPoloneix(unittest.TestCase):
    def test_mapdata_DataExist_False(self):
        Poloniex = poloniex.PoloniexExchange()
        Poloniex.bucket_interval_value = 1.0
        Poloniex.raw_data = [
            [
                "1",
                "3",
                "2",
                "1.5",
                "10",
                "20",
                "15",
                "25",
                "30",
                "0",
                "0",
                "0",
                1499040000000,
                1648707600000,
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
                    "Poloniex",
                    1.0,
                ]
            ],
        )

        Poloniex.map_data("BTC")
        self.assertTrue(expected_output.equals(Poloniex.data))

    def test_mapdata_DataExist_True(self):
        Poloniex = poloniex.PoloniexExchange()
        Poloniex.bucket_interval_value = 1.0
        Poloniex.raw_data = [
            [
                "1",
                "3",
                "2",
                "1.5",
                "10",
                "20",
                "15",
                "25",
                "30",
                "0",
                "0",
                "0",
                1499040000000,
                1648707600000,
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
                    "Poloniex",
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
                    "Poloniex",
                    1.0,
                ],
            ],
        )

        Poloniex.data = pd.DataFrame(
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
                    "Poloniex",
                    1.0,
                ]
            ],
        )

        Poloniex.map_data("BTC")
        self.assertTrue(expected_output.equals(Poloniex.data))


if __name__ == "__main__":
    unittest.main()
