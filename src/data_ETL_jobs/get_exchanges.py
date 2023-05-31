"""Module containing function to create an instance of each exchange class from the config file

Functions
---------
get_exchanges_from_config()
"""
import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

import yaml
import importlib


def get_exchanges_from_config():
    """Function to create a list of exchange class instances from the listed exchanges in config file

    Returns:
        list of exchange class instances
    """
    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    exchanges = []

    for i in config["exchanges"]:
        try:
            m = importlib.import_module(f"exchanges.{i.lower()}")
            c = getattr(m, f"{i.capitalize()}Exchange")
            instance = c()
            exchanges.append(instance)
        except ModuleNotFoundError:
            print(f"No module/file named {i.lower()} found within exchanges folder")
            raise
        except AttributeError:
            print(
                f"No class named {i.capitalize()}Exchange found in module exchanges.{i.lower()}"
            )
            raise

    return exchanges
