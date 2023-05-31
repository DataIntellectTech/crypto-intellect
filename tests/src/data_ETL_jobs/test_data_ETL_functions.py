from datetime import datetime
import pandas as pd
from src.data_ETL_jobs import data_ETL_functions
from sqlalchemy.sql import text
import unittest


class TestData_ETL_functions:
    def test_Save_To_Data_Single(self, fake_db):
        session = fake_db
        engine = session.get_bind()

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
                    datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Binance",
                    1.0,
                ]
            ],
        )
        data_ETL_functions.Save_to_Data(dataframe=test_data, engine=engine)

        sql = "SELECT * FROM Klines_Data WHERE Exchange='Binance'"

        with engine.connect() as connection:
            data = pd.read_sql_query(sql=text(sql), con=connection)
            expected_output = pd.DataFrame(
                columns=[
                    "Unique_ID",
                    "Exchange",
                    "Coin",
                    "TimeStamp",
                    "OpenPrice",
                    "HighPrice",
                    "LowPrice",
                    "ClosePrice",
                    "Volume",
                    "Bucket_Interval",
                ],
                data=[
                    [
                        "2023-03-28 01:00:00_BTC_Binance_1.0",
                        "Binance",
                        "BTC",
                        "2023-03-28 01:00:00.000000",
                        28000.0,
                        28500.0,
                        27500.0,
                        28300.0,
                        5.0,
                        1.0,
                    ],
                    [
                        "2023-03-28 01:00:00_ETH_Binance_1.0",
                        "Binance",
                        "ETH",
                        "2023-03-28 01:00:00.000000",
                        1.0,
                        1.0,
                        1.0,
                        1.0,
                        1.0,
                        1.0,
                    ],
                ],
            )

        session.close()
        engine.dispose()

        assert expected_output.equals(data) == True

    def test_Save_To_Data_Duplicate(self, fake_db):
        session = fake_db
        engine = session.get_bind()

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
                    datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Binance",
                    1.0,
                ],
                [
                    datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Binance",
                    1.0,
                ],
            ],
        )
        data_ETL_functions.Save_to_Data(dataframe=test_data, engine=engine)

        sql = "SELECT * FROM Klines_Data WHERE Exchange='Binance'"

        with engine.connect() as connection:
            data = pd.read_sql_query(sql=text(sql), con=connection)
        expected_output = pd.DataFrame(
            columns=[
                "Unique_ID",
                "Exchange",
                "Coin",
                "TimeStamp",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
                "Bucket_Interval",
            ],
            data=[
                [
                    "2023-03-28 01:00:00_BTC_Binance_1.0",
                    "Binance",
                    "BTC",
                    "2023-03-28 01:00:00.000000",
                    28000.0,
                    28500.0,
                    27500.0,
                    28300.0,
                    5.0,
                    1.0,
                ],
                [
                    "2023-03-28 01:00:00_ETH_Binance_1.0",
                    "Binance",
                    "ETH",
                    "2023-03-28 01:00:00.000000",
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                ],
            ],
        )

        session.close()
        engine.dispose()

        assert expected_output.equals(data) == True

    def test_Save_To_Data_record_exists(self, fake_db):
        session = fake_db
        engine = session.get_bind()

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
                    datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
                    28000,
                    28500,
                    27500,
                    28300,
                    5,
                    "BTC",
                    "Binance",
                    "1.0",
                ],
                [
                    datetime.strptime("2023-03-28 01:00:00", "%Y-%m-%d %H:%M:%S"),
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Binance",
                    "1.0",
                ],
            ],
        )
        data_ETL_functions.Save_to_Data(dataframe=test_data, engine=engine)

        sql = "SELECT * FROM Klines_Data WHERE Exchange='Binance'"

        with engine.connect() as connection:
            data = pd.read_sql_query(sql=text(sql), con=connection)
        expected_output = pd.DataFrame(
            columns=[
                "Unique_ID",
                "Exchange",
                "Coin",
                "TimeStamp",
                "OpenPrice",
                "HighPrice",
                "LowPrice",
                "ClosePrice",
                "Volume",
                "Bucket_Interval",
            ],
            data=[
                [
                    "2023-03-28 01:00:00_BTC_Binance_1.0",
                    "Binance",
                    "BTC",
                    "2023-03-28 01:00:00.000000",
                    28000.0,
                    28500.0,
                    27500.0,
                    28300.0,
                    5.0,
                    1.0,
                ],
                [
                    "2023-03-28 01:00:00_ETH_Binance_1.0",
                    "Binance",
                    "ETH",
                    "2023-03-28 01:00:00.000000",
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                ],
            ],
        )

        session.close()
        engine.dispose()

        assert expected_output.equals(data) == True

    def test_add_Unique_ID(self):
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
            data=[["2023-03-01", 1.0, 1.0, 1.0, 1.0, 1.0, "ETH", "Binance", 1.0]],
        )

        data_ETL_functions.add_UniqueID(test_data)
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
                "Unique_ID",
            ],
            data=[
                [
                    "2023-03-01",
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    "ETH",
                    "Binance",
                    1.0,
                    "2023-03-01_ETH_Binance_1.0",
                ]
            ],
        )

        assert expected_output.equals(test_data) == True


if __name__ == "__main__":
    unittest.main()
