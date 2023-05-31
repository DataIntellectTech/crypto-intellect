from setuptools import setup
from setuptools import find_packages

setup(
    name = 'query-exchanges',
    
    author = 'Shane Martin/Scott Lam-McGonnell',
    
    author_email= 'shane.martin@aquaq.co.uk',
    
    url = 'https://github.com/AquaQAnalytics/data-engineering-devops',
    
    #Read this as
    #   -MAJOR VERSION 0
    #   -MINOR VERSION 1
    #   -MAINTAINENCE VERSION 0
    #   -DEVEOPLEMENT VERSION
    version = '0.1.0.dev1',
    
    description= 'A python package to data on crypto currancies from exchange APIs',
    
    packages=find_packages(
        where=['src', 'src.data_operations', 'src.exchanges']
    ),
    
    package_data={'': ['csv-storage/*.csv']}
    
)