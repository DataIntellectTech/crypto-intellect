from src.database_setup.create_functions import Create_Tables, Create_database
from unittest.mock import patch
from sqlalchemy import create_engine, text
import pandas as pd


class TestCreateFunctions:
    @patch("src.database_setup.create_functions.create_engine")
    def test_create_funcitons(self, mock_engine):
        mock_engine = create_engine("sqlite://")
        Create_database(mock_engine)
        Create_Tables(mock_engine)

        sql = """select name from sqlite_master where type = 'table' """

        with mock_engine.begin() as conn:
            df = pd.read_sql(text(sql), conn)

        expected_output = pd.DataFrame(
            columns=["name"], data=["Exchanges", "Coins", "Klines_Data"]
        )

        assert expected_output.equals(df) == True
