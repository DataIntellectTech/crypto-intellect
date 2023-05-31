from unittest.mock import patch
from datetime import datetime
import pandas as pd
from src.data_ETL_jobs.exchanges.exchange_functions import feed_forward
import unittest


class TestData_ETL_function:
    def mock_yaml_load(*args, **kwargs):
        mock_dict = {"save_to_database": True, "database_connection": "string"}
        return mock_dict

    @patch(
        "src.data_ETL_jobs.exchanges.exchange_functions.feed_forward.yaml.load",
        side_effect=mock_yaml_load,
    )
    @patch("src.data_ETL_jobs.exchanges.exchange_functions.feed_forward.create_engine")
    def test_Feed_forward(self, mock_engine, mock_open, fake_db):
        session = fake_db
        test_engine = session.get_bind()

        mock_reponse = test_engine
        mock_engine.return_value = mock_reponse

        test_data = pd.DataFrame(
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
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Kucoin",
                    1.0,
                ]
            ],
        )

        output = feed_forward.feed_forward(dataframe=test_data, exchange_name="Kucoin")

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
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Kucoin",
                    1.0,
                ],
                [
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    28250.0,
                    28250.0,
                    28250.0,
                    28250.0,
                    0.0,
                    "BTC",
                    "Kucoin",
                    1.0,
                ],
                [
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    1100.0,
                    1100.0,
                    1100.0,
                    1100.0,
                    0.0,
                    "APE",
                    "Kucoin",
                    1.0,
                ],
            ],
        )
        assert expected_output.equals(output) == True

    @patch(
        "src.data_ETL_jobs.exchanges.exchange_functions.feed_forward.yaml.load",
        side_effect=mock_yaml_load,
    )
    @patch("src.data_ETL_jobs.exchanges.exchange_functions.feed_forward.create_engine")
    def test_Feed_forward_duplicate(self, mock_engine, mock_open, fake_db):
        session = fake_db
        test_engine = session.get_bind()

        mock_reponse = test_engine
        mock_engine.return_value = mock_reponse

        test_data = pd.DataFrame(
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
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Kucoin",
                    1.0,
                ],
                [
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    2.0,
                    2.0,
                    2.0,
                    2.0,
                    10,
                    "BTC",
                    "Kucoin",
                    1.0,
                ],
            ],
        )

        output = feed_forward.feed_forward(dataframe=test_data, exchange_name="Kucoin")

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
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Kucoin",
                    1.0,
                ],
                [
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    2.0,
                    2.0,
                    2.0,
                    2.0,
                    10,
                    "BTC",
                    "Kucoin",
                    1.0,
                ],
                [
                    datetime.strptime("2023-03-28 01:05:00", "%Y-%m-%d %H:%M:%S"),
                    1100.0,
                    1100.0,
                    1100.0,
                    1100.0,
                    0.0,
                    "APE",
                    "Kucoin",
                    1.0,
                ],
            ],
        )

        assert expected_output.equals(output) == True


if __name__ == "__main__":
    unittest.main()
