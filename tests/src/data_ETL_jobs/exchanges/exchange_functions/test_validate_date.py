"""Module containing tests for validate_date.py 

Classes
-------
TestValidateDate
"""

import unittest
from src.data_ETL_jobs.exchanges.exchange_functions import validate_date


class TestValidateDate(unittest.TestCase):
    def test_validate_type1_good(self):
        test_input = "2022-05-13"
        self.assertTrue(validate_date.ValidateDate.validate_date(test_input))

    def test_validate_type2_good(self):
        test_input = "2022-05-13 14:45:15"
        self.assertTrue(validate_date.ValidateDate.validate_date(test_input))

    def test_validate_type3_good(self):
        test_input = "2022-05-12 15:36"
        self.assertTrue(validate_date.ValidateDate.validate_date(test_input))

    def test_validate_bad1(self):
        test_input = "12-15 12:45:13"
        self.assertRaises(
            ValueError, validate_date.ValidateDate.validate_date, test_input
        )

    def test_validate_bad2(self):
        test_input = "2022-01-12 14:75:13"
        self.assertRaises(
            ValueError, validate_date.ValidateDate.validate_date, test_input
        )


if __name__ == "__main__":
    unittest.main()
