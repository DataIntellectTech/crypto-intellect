"""Module containing abstract base class to be extended to different Exchange APIs

Classes
-------
BaseExchange
"""

from abc import ABC, abstractclassmethod
import yaml
from exchange_functions.limiter import Limiter
from exchange_functions.aggregator import aggregator

with open("/src/config.yml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


class BaseExchange(ABC):
    """Abstract base class to be inherited by each API exchange class"""

    def __init__(self):
        # A normalised data set which will contain the same data from each API
        self.data = None
        # A data set which will contain any raw data taken from the API which has not been mapped to normalised data
        self.raw_data = []
        # sets the interval length for bucketing data
        self.bucket_interval_value = config["bucketing_interval"]
        # aggregation interface
        self.aggregator = aggregator

    @abstractclassmethod
    @Limiter(calls_limit=15, period=1)
    # Required to have request method inside each API class as data is returned in different formats depending on the API
    async def request_data(self, asset_name, startTime, endtime):
        pass

    @abstractclassmethod
    # Required to have map method inside each API as the returned raw data differs between each API
    def map_data(self, asset_name):
        pass

    @abstractclassmethod
    # Required method to set up time intervals based on supplied bucketing interval
    def get_time_interval(self, Date_Time):
        pass

    async def pull_data(self, asset_name, startTime, endTime, session):
        """asynchronous function to await request responses and the call map data"""
        try:
            await self.request_data(asset_name, startTime, endTime, session)
            self.map_data(asset_name)

        except:
            pass
