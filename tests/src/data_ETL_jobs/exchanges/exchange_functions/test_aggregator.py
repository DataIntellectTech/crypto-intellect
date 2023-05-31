from datetime import datetime
import pandas as pd
from src.data_ETL_jobs.exchanges.exchange_functions import aggregator
import unittest


class Testaggregator(unittest.TestCase):
    def test_aggregate_to_bucket_interval(self):
        Aggregator = aggregator.aggregator()

        dataframe = pd.DataFrame(
            columns=[
                "TimeStamp",
                "Coin",
                "Exchange",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
                "Bucket_Interval",
            ],
            data=[
                [
                    datetime.strptime("2023-05-05 12:01:00", "%Y-%m-%d %H:%M:%S"),
                    "BTC",
                    "Binance",
                    10.0,
                    15.0,
                    10.0,
                    10.0,
                    100.0,
                    5,
                ],
                [
                    datetime.strptime("2023-05-05 12:02:00", "%Y-%m-%d %H:%M:%S"),
                    "BTC",
                    "Binance",
                    10.0,
                    25.0,
                    10.0,
                    10.0,
                    100.0,
                    5,
                ],
                [
                    datetime.strptime("2023-05-05 12:03:00", "%Y-%m-%d %H:%M:%S"),
                    "BTC",
                    "Binance",
                    10.0,
                    15.0,
                    5.0,
                    10.0,
                    100.0,
                    5,
                ],
                [
                    datetime.strptime("2023-05-05 12:04:00", "%Y-%m-%d %H:%M:%S"),
                    "BTC",
                    "Binance",
                    10.0,
                    15.0,
                    10.0,
                    12.0,
                    100.0,
                    5,
                ],
                [
                    datetime.strptime("2023-05-05 12:05:00", "%Y-%m-%d %H:%M:%S"),
                    "BTC",
                    "Binance",
                    10.0,
                    15.0,
                    10.0,
                    12.0,
                    100.0,
                    5,
                ],
            ],
        )

        expected_output = pd.DataFrame(
            columns=[
                "TimeStamp",
                "Coin",
                "Exchange",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
                "Bucket_Interval",
            ],
            data=[
                [
                    datetime.strptime("2023-05-05 12:05:00", "%Y-%m-%d %H:%M:%S"),
                    "BTC",
                    "Binance",
                    10.0,
                    25.0,
                    5.0,
                    12.0,
                    500.0,
                    5,
                ]
            ],
        )

        new_Dataframe = Aggregator.aggregate_to_bucket_interval(dataframe, 5)
        self.assertTrue(expected_output.equals(new_Dataframe))
