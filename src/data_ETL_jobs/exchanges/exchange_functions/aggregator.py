import pandas as pd


class aggregator:
    @staticmethod
    def aggregate_to_bucket_interval(dataframe, bucket_interval):
        """Function to aggregate the dataframe into buckets of specified interval_size"""
        try:
            dataframe = dataframe.groupby(
                [
                    pd.Grouper(
                        key="TimeStamp",
                        freq=f"{bucket_interval}min",
                        closed="right",
                        label="right",
                    ),
                    "Coin",
                    "Exchange",
                ],
                as_index=False,
            ).agg(
                {
                    "TimeStamp": "last",
                    "Coin": "first",
                    "Exchange": "first",
                    "OpenPrice": "first",
                    "HighPrice": "max",
                    "LowPrice": "min",
                    "ClosePrice": "last",
                    "Volume": "sum",
                    "Bucket_Interval": "last",
                }
            )

            return dataframe

        except TypeError as error:
            print(
                "when using aggregate exchange.data is not in the required form of a pandas dataframe",
                repr(error),
            )

        except KeyError as error:
            print(
                """Dataframe passed to aggregate does not have required fields check self.data contains fields
                    'TimeStamp', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice', 'Volume', 'Coin', 'Exchange'"""
            )
