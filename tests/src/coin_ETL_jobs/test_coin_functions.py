from src.coin_ETL_jobs import coin_functions
from unittest.mock import patch, MagicMock
from datetime import datetime
import pandas as pd
import unittest
from sqlalchemy.sql import text
import requests
import pytest


class Testfinding_coins:
    @patch("src.coin_ETL_jobs.coin_functions.requests")
    def test_finding_top_n_coins_good(self, mock_requests):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 24116,
                "market_cap": 467413165315,
                "market_cap_rank": 1,
            }
        ]
        mock_requests.get.return_value = mock_response

        response = coin_functions.finding_top_n_coins()
        expected_output = pd.DataFrame(
            columns=["Coin", "MarketCap", "Date_Added"],
            data=[["BTC", 467413165315, datetime.now()]],
        )

        assert (response["Coin"].iloc[0] == expected_output["Coin"].iloc[0]) == True
        assert (
            response["Date_Added"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
            == expected_output["Date_Added"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        ) == True
        assert (
            response["MarketCap"].iloc[0] == expected_output["MarketCap"].iloc[0]
        ) == True

    @patch("src.coin_ETL_jobs.coin_functions.requests")
    def test_specified_coins(self, mock_requests):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 24116,
                "market_cap": 467413165315,
                "market_cap_rank": 1,
            }
        ]
        mock_requests.get.return_value = mock_response

        response = coin_functions.find_specified_coins(["Bitcoin"])
        expected_output = pd.DataFrame(
            columns=["Coin", "MarketCap", "Date_Added"],
            data=[["BTC", 467413165315, datetime.now()]],
        )

        assert (response["Coin"].iloc[0] == expected_output["Coin"].iloc[0]) == True
        assert (
            response["Date_Added"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
            == expected_output["Date_Added"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        ) == True
        assert (
            response["MarketCap"].iloc[0] == expected_output["MarketCap"].iloc[0]
        ) == True

    def test_save_to_coin_table(self, fake_db):
        session = fake_db
        engine = session.get_bind()

        test_input = pd.DataFrame(
            columns=["Coin", "MarketCap", "Date_Added"],
            data=[["TRX", 1000.0, "2023-03-01"]],
        )

        coin_functions.save_to_Coins_table(dataframe=test_input, engine=engine)

        sql = "SELECT * FROM Coins WHERE Coin='TRX'"

        with engine.connect() as connection:
            data = pd.read_sql_query(sql=text(sql), con=connection)

        assert (test_input.equals(data)) == True


if __name__ == "__main__":
    unittest.main()
