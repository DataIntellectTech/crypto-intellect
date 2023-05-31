from src.MetaData_jobs.MetaData_functions import (
    get_current_bucket_interval,
    get_number_of_coins,
    get_number_of_Exchanges,
    get_number_of_rows,
)
import pandas as pd


class TestMetaDataFunctions:
    def test_get_number_of_coins(self, fake_db):
        num_coins = get_number_of_coins(fake_db.get_bind())
        expected_data = pd.DataFrame(columns=["Number_of_Coins_Tracked"], data=[2])
        assert num_coins.equals(expected_data) == True

    def test_get_number_of_exchanges(self, fake_db):
        num_exchanges = get_number_of_Exchanges(fake_db.get_bind())
        expected_data = pd.DataFrame(columns=["Number_of_Exchanges_Tracked"], data=[3])
        assert num_exchanges.equals(expected_data) == True

    def test_get_number_of_rows(self, fake_db):
        num_rows = get_number_of_rows(
            fake_db.get_bind(), StartTime="2023-01-01", EndTime="2023-12-01"
        )
        expected_value = pd.DataFrame(columns=["Rows_per_Minute"], data=[2.0])
        assert num_rows.equals(expected_value) == True

    def test_get_current_bucketing_interval(self, fake_db):
        bucketing_interval = get_current_bucket_interval(
            fake_db.get_bind(), StartTime="2023-01-01", EndTime="2023-12-01"
        )
        expected_value = pd.DataFrame(columns=["Bucket_Interval"], data=[1.0])
        assert bucketing_interval.equals(expected_value) == True
