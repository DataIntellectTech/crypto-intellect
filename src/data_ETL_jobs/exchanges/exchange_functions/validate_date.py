"""Module containing class to validate whether dates passed as arguements are of datetime format

Classes
-------
ValidateDate
"""

from datetime import datetime
import time


class ValidateDate:
    """Class containig methods to determine which datetime format is passed as arguement

    Methods
    -------
    __validate_type1
    __validate_type2
    __validate_type3
    """

    @staticmethod
    def __validate_type1(date):
        """Method to determine if string is of form %Y%m%d

        Args:
            date (str): datetime string to be tested

        Returns:
            boolean value based on whether string is of correct format
        """

        try:
            res = bool(datetime.strptime(date, "%Y-%m-%d"))
            return res
        except ValueError:
            return False

    @staticmethod
    def __validate_type2(date):
        """Method to determine if string is of form %Y%m%d %H:%M:%S

        Args:
            date (str): datetime string to be tested

        Returns:
            boolean value based on whether string is of correct format
        """

        try:
            res = bool(datetime.strptime(date, "%Y-%m-%d %H:%M:%S"))
            return res
        except ValueError:
            return False

    @staticmethod
    def __validate_type3(date):
        """Method to determine if string is of form %Y%m%d %H:%M

        Args:
            date (str): datetime string to be tested

        Returns:
            boolean value based on whether string is of correct format
        """
        try:
            res = bool(datetime.strptime(date, "%Y-%m-%d %H:%M"))
            return res
        except ValueError:
            return False

    @classmethod
    def validate_date(cls, date):
        """Method to convert datetime string to unix timestamp

        Args:
            date (str): datetime string to be tested

        Returns:
            integer unix timestamp corresponding to the datetime stamp passed
        """

        if cls.__validate_type1(date):
            return int(time.mktime(datetime.strptime(date, "%Y-%m-%d").timetuple()))
        elif cls.__validate_type2(date):
            return int(
                time.mktime(datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timetuple())
            )
        elif cls.__validate_type3(date):
            return int(
                time.mktime(datetime.strptime(date, "%Y-%m-%d %H:%M").timetuple())
            )
        else:
            raise (ValueError("Invalid DateTime Format"))

    @classmethod
    def validate_date_error_catch(cls, date):
        if cls.__validate_type1(date):
            return True
        elif cls.__validate_type2(date):
            return True
        elif cls.__validate_type3(date):
            return True
        else:
            return TypeError(
                "Date string of wrong formart please use fors %Y-%m-%d or %Y-%m-%d %H:%M:%S or %Y-%m-%d %H:%M"
            )
