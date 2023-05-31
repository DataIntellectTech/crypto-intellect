# **Crypto-Intellect**

Crypto-Intellect is a dockerised data pipeline designed to pull candlestick data for crypto currencies from various exchange APIs, normalise it and save to a user defined output. The app provides a no code, easy to use data pipeline for crypto enthusiasts. 

The app is currently maintained by the Data Analytics and Engineering team at Data Intellect.

website: https://dataintellect.com

## **Layout**
Below is a reference for the layout of the readme:

* Overview
   - Exchanges
   - Normalised Data Format
   - Saving Data
      - Raw Mode
	  - Database Mode
   - Pipeline Management
   - Main Features
* Installation and Setup
    - No Config File
	- Supply a Config File
	- Aliased commands
	- Tags
* MetaData
  - Example Update
* Expanding App
  - Adding an Exchange

<br/>

# **Overview**

Crypto-Intellect is a dockerised data pipeline designed to pull real time candlesticks data for user specified cryptocurrencies and save it in a normalised format. The app is designed to generate candlesticks data based on user inputs such as what coins to pull data for, what exchanges to pull from, what data interval aggregation to use and where to store data. The app takes user inputs during start up _(see Installation and Setup)_, creates a data pipeline and sets up cronjobs to manage data pulling. The Crypto-Intellect docker conatiner is a no code data pipeline that will pull, normalise and save real time cryptocurrancy data.  

<br/>

## **Exchanges**

Crypto-Intellect currently can pull from any of the following exchanges:
* Binance
* Kraken
* Kucoin
* Poloneix

Between the exchanges provided, Crypto-Intellect will be able to provide data on at least the top 100 crypto coins by market cap. 

The small number of exchanges currently offered is a result of each exchange API returning a differently formatted response, therefore requiring that each exchange is represented by its own class. Despite this we plan to increase the number of exchanges offered in the future to provide users a much wider data source.  

<br/>

## **Normalised Data Format**

Crypto-Intellect aims to provide users with easily accessible and readable candlestick data which can be feed directly into their analysis tools, hence each candlstick entry saved will contain the following fields:

|TimeStamp|Exchange|Coin|OpenPrice|HighPrice|LowPrice|ClosePrice|Volume|Bucket_Interval|
|--------|--------|----|---------|---------|--------|----------|------|------|
|_(DateTime)_|_(String)_|_(String)_|_(Float)_|_(Float)_|_(Float)_|_(Float)_|_(Float)_|_(Float)_|

* Timestamp: The latest time to be included in data interval
* Exchange: The name of the exchange from which the data entry was pulled
* Coin: Ticker for the coin whose data is represented in data entry
* OpenPrice: The first price recorded for the coin for the data interval
* HighPrice: The highest price recorded for the coin in the data interval
* LowPrice: The lowest price recorded for the coin in the data interval
* ClosePrice: The latest price recorded for the coin in the data interval
* Volume: Total volume of the coin traded on the exchange over the data interval
* Bucket_Interval: The size of the data interval you are pulling candlesticks data for 

<br/>

The Crypto-Intellect app does its own data aggregation allowing users to pull data for any time interval larger than one minute, regardless of whether it is typically offered by the exchange API (_see **Setup and Installation** for more information_). 

<br/>

## **Data Saving**

The Crypto-Intellect app allows users to access two different modes for data saving depending on whether they wish to access to a full SQL database **_(Database Mode)_** or save data locally to CSV files **_(Raw Mode)_**. 

### **Raw Mode**

In raw mode the Crypto-Intellect app will save all data pulled to local CSV files inside the directory **_<current_working_directory>/cryptointellect/csv_** . The CSV files will be split up into files corresponding to a single day and a single exchange and follow the naming format **_datetime_exchange-name_bucket-interval.csv_** .

The columns of each csv file will match those listed in the **_Normalise Data Format_** section above, with each row representing a single candlesticks entry. Raw mode is enabled as standard when running the Crypto-Intllect app however if a user config is provided in order to access raw mode the **_raw_mode_** flag must be set to _**True**_ (for more information see **_Installation and Setup_**).

<br/> 

### **Database Mode**

In order to access Database mode a config file with a database connection must be supplied at startup, (see **_Installation and Setup_**). Assuming a database connection is supplied the app will create a database named **_CryptoIntellectDB_** which will contain the following schema:

<div style="width: 1500px;">
<div class="center">

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'fontSize': '10px'}}}%%
erDiagram
    Coins {
		Coin string PK
		MarketCap BigInteger
		DataAdded datetime 
	}
    Klines_Data {
		Unique_ID string PK
		Exchange string FK
		Coin string FK
		TimeStamp datetime
		OpenPrice float
		HighPrice float
		LowPrice float
		ClosePrice float
		Volume float
		Bucket_Interval Float
	}
    Exchanges{
		Exchange string PK
	}
	MetaData{
		Id integer PK
		Effective_From datetime 
		Effective_To datetime 
		Assertive_From datetime 
		Assertive_To datetime
		Rows_per_Minute integer
		Number_of_Coins_Tracked integer
		Number_of_Exchanges_Tracked integer 
		Bucket_Interval  integer

	}
	Coins ||--|{ Klines_Data : ""
	Exchanges ||--|{ Klines_Data : ""
```
</div>
</div>


In order to safely handle database connections and mappings the Crypto-Intellect app make use of [SqlAlchemy ORM](https://github.com/sqlalchemy) therefore users will only be able to connect to a database which is integrated in SqlAlchemy's dialects, for more information on this see the [SqlAclchemy docs](https://docs.sqlalchemy.org/en/20/). 

<br/>

## **Pipeline Management**

Control of the pipeline i.e how pulling methods, normalisation methods and saving methods are called is handled by the python script, **_src/data_ETL_jobs/data_ETL_script.py_**. The continuous running of this script to fetch live data is managed as a cronjob, setup inside the Crypto-Intellect app docker container, the frequency of the cronjob is dictated by the users defined **_bucket_interval_**. 

Alongside the **_src/data_ETL_jobs/data_ETL_script.py_** script being run as a cronjob, the script **_src/MetaData_jobs/MetaData_script.py_** is also set to run as a cronjob. This is in order to track any changes in the metadata of the **_Klines_Data_** table as new data is saved. 

<br/>

## **Main Features**

* **_Multi Threaded_**: In order to improve efficiency and speed up data pulling the Crypto-Intellect app will generate threads for each exchange, which handling data pulling, data normalisation and data saving for each exchange using pythons [mulitproceesing package](https://docs.python.org/3/library/multiprocessing.html). Enabling users to pull data simultaneously from different exchanges
* **_Asynchrous code_**: In order to ensure the data pipeline is not slowed down by large amounts of I/O bound operations, the Crypto-Intellect app uses the [asyncio](https://docs.python.org/3/library/asyncio.html) python library to set up pulling from different API endpoints as a asynchronous tasks.
* **_Limiter_**: Crypto_Intellect apps have their own limiter decorator classes, used to decorate asynchronous data request methods, which control the number of requests that can be sent in a given period of time. Ensuring the app doesn't hit sensitive rate limits of open APIs from which data is sourced. 
* **_Feed Forward_** : This is a database mode only feature. Some APIs do not return data if 0 volume is traded on that exchange in a given data interval. In order to ensure users do not have missing data entries, if using database mode, a feed forward method can be called to generate an entry with 0 volume and prices set to the last _ClosePrice_ for missing data, see **_src/data_ETL_jobs/exchanges/exchange_functions_** for details of this function.
* **_Backfill Historical Data_**: This is a database mode only feature. If using database mode users can run an aliased backfill script( see  **_Installation and Setup_** for runnning aliased commands) in order to fill database with candlesticks data from a specified date. 

<br/>

## **Installation and Setup**
Crypto-Intellect is a docker app that is run from a terminal therefore in order to run the application users must have docker installed and access to a terminal. 
 
In order to make the installation as easy as possible a bash script that handles pulling the docker image, building of the container, setting up alaised commands and creates file paths for saving csv data is available to download from this repo. The image of the docker container is also free to pull directly from our public AWS ERC repository and can be run using docker from the users terminal. 

<br/>

## **Using our Bash Script**
The bash script can be downlaoded from this repo, simply by selecting the document then in the right hand conor of the display select the drop down menu then download. The bash script will allow users to run either raw mode or database mode (this will only run if a config file is supplied by the user) or both. 

### **No Config File**

Setting up a Crypto-Intellect app without a config file is the fastest way to setup a the app however it removes the users ability to customise their data pulling. Running CryptoIntellect without a config file means that the app will only run on raw mode and you will not have access to any of the database features as well as not being able to costumise the coins pulled or your data aggregation bucket.

### **Setup**

After downlaoding the [bash script ADD LINK], users should move/copy the bash script into the directory they want to store their csv data then from their terminal run the command 
```linux
source cryptointellect.sh
```

This will setup a CryptoIntellect app on raw mode with csv file being written to the directory **_<current_working_directory>/cryptointellect/csv_**. The standard configuration set up when no configuration file is provided is

```yml
bucketing_interval: 1m
exchanges:
  - Binance
  - Kucoin
  - Poloniex
  - Kraken
database_connection: mysql+pymysql://root:root@database:3306
list_coins_to_poll:
  - Safemoon
  - Neo
  - Optimism
n_coins_to_poll: 30
save_to_csv: true
save_to_database: false
```

<br/>


### **Supply a Config File**

In order to be able to set their own configuration options and allow their app to access database mode users must supply a configuration file during setup. Supplying a configuration file will allow users to select which exchanges they wish to pull from, select what coins they wish to track, choose which modes they wish to use and what data bucket interval they wish to pull data for. 

### **Setup**
To begin users must build their own configuration file which must be a .yml file and contain all of the fields listed below

```yml
bucketing_interval: # Integer followed by M,H,D

exchanges: # List of exchanges 
  - 

database_connection: # String connection to database

list_coins_to_poll: # List of crypto currency coins 
  - 

n_coins_to_poll: # Integer between 1-100

save_to_csv: # Boolean value

save_to_database: #Boolean value
```
**Config Fields**

* **bucket_interval**: integer followed by a tag for time period, M-minute, H-hour, D-day
* **exchanges**: a list of exchanges from which user wishes to pull data and are provided by the app
* **database_connection**: a string containing a connection to a database hosted by the user. The database must be integration into SqlAlchemy _(see above)_
* **list_coins_to_pull**: a list of specific coins the user wishes to track, this can also be left as none
* **n_coins_to _poll**: the number of coins out of the top 100 coins by market cap the user wishes to track
* **save_to_csv**: Boolean value indicating whether user wishes to save data to csv files
* **save_to_database**: Boolean value indicating whether user wishes to save to a database

<br/>


After building a config file and downloading the bash scriptusers should move/copy the bash script into the directory they wish to store their csv then from their terminal run the command 
```linux
source cryptointellect.sh -c/--config <PATH_TO_CONFIG_FILE>
```
<br/>

## **Aliased Commands**

Regardless of what setup is chosen the bash script will alias docker commands to provide a convenient way to control the app.The alaised commands are shown below. 

```linux
alias ci_start="docker start cryptointellect_container"
alias ci_stop="docker stop cryptointellect_container"
alias ci_attach="docker exec -it cryptointellect_container bash"
```
* **_ci_start:_** running this command will start the cryptointellect app
* **_ci_stop:_** running this command will stop the cryptointellect app 
* **_ci_attach:_** running this command will connect the user to the docker container terminal to allow them access to the files controlling their app pipeline

<br/>

## **Tags**

The bash startup script may be run with a number of tags to provide users with some extra information. The **_-h/--help_** tag will allow users to view a small help document

```linux
source cryptointellect.sh -h/--help
```

The **_-a/--alias_** tag will allow the user to see a list of the aliased commands listed above and what they correspond to

```linus
source cryptointellect.sh -a/--alias
```

<br/>

## **Pulling the Image From AWS ECR**

Users are also free to pull the docker image directly from AWS ECR to run themselves using docker, users doing this will also be able to run either database mode or raw mode depending on weather they wish to attach a config file at startup. 

### **Without Config**

In order to pull and run the image of a Crypto-Intellect app users can enter the following commands into their terminal

```linux
docker pull public.ecr.aws/c5m8g6i6/cryptointellect:latest
docker run --name cryptointellect_container -d -v <path_to_local_csv_folder>:/src csv_folder/:rw public.ecr.aws/c5m8g6i6/cryptointellect:latest
  ```

This creates and starts a container named cryptointellect_container, which can be stopped and started using:

```Linux
docker stop cryptointellect_container
docker start cryptointellect_container
```

### **With Config**

The following instructions will allow users to pull the image from AWS ECR and attach a config file (which must follow the description given above). 

To begin users must create a directory in which they want to manage their Crypto-Intellect app outputs. Inside the directory users should create a docker file with the commands:

```dockerfile
FROM public.ecr.aws/c5m8g6i6/cryptointellect:latest
COPY <path_to_local_config.yml> /src/
RUN python3 src/Initial_startup.py
CMD ["cron", "-f"]
```

Next users will have to open their terminal and locate the directory in which the dockerfile now lives. Once inside the correct directory users should ener the following commands:

```Linux
docker build -f Dockerfile . -t cryptointellect
docker run --name cryptointellect_container -d -v <path_to_local_csv_folder>:/src/csv_folder/:rw cryptointellect
```

<br/>

# **MetaData Table**
As some databases that are complatible with SqlAlchmey do not have the capaicty to update bitemporal data, all of the metadata logic is handled in python and pushed to the database.

The apps MetaData table stores Bitemporal data with the 'Effective' time interval representing when the MetaData statistics where valid and the 'Assertive' time interval representing when we believed the current statistics and 'Effective' interval to be valid. The current MetaData table including what statistics we store looks like 

|Effective_From|Effective_To|Assertive_From|Assertive_To|Rows_per_Minute|Number_of_Coins_Tracked|Number_of_Exchanges_Tracked|Bucket_Interval|
|--------|--------|----|---------|---------|--------|----------|------|
|_(DateTime)_|_(DateTime)_|_(DateTime)_|_(DateTime)_|_(Integer)_|_(Integer)_|_(Integer)_|_(Integer)_|

### **Fields**
* **Effective From** The point from when the data stored in the row was valid 
* **Effective To**  The point to when the data stored in the row was valid
* **Assertive_From** The point from when we believed the data stored in the row was valid
* **Assertive_To** The point to when we believed the data stored in this row was valid 
* **Rows_per_Minute** The number of rows stored in each one minute interval
* **Number_of_Coins_Tracked** The number of coins currently tracked in the database
* **Number_of_Exchanges_Tracked** The number of exchanges currently tracked in the database
* **BUcket_INterval** The size of the bucket interval being quiered

<br/>

## **ExampleUpdate**

Lets assume we have a MetaData table setup and have filled in our first entry, so our table looks like the following



|Effective_From|Effective_To|Assertive_From|Assertive_To|Rows_per_Minute|Number_of_Coins_Tracked|Number_of_Exchanges_Tracked|Bucket_Interval|
|----------|--------|----|---------|---------|--------|----------|------|
|2023-03-08 12:40:00|NULL|2023-03-08 12:40:00|NULL|92|50|3|3|

<br/>

The above table lets us know that the data in that row is valid for our database from 2023-03-08 12:40:00 to the current time (this information is stored in the effective time) and that during the period from 2023-03-08 12:40:00 to the current time we believe that the information in this row was true.

Lets now say there has been in change in the MetaData, we now are tracking 52 coins. The first thing we do is update the current entry in the table to become:

|Effective_From|Effective_To|Assertive_From|Assertive_To|Rows_per_Minute|Number_of_Coins_Tracked|Number_of_Exchanges_Tracked|Bucket_Interval|
|----------|--------|----|---------|---------|--------|----------|------|
|2023-03-08 12:40:00|NULL|2023-03-08 12:40:00|2023-03-10 12:50:00|92|50|3|3|

<br/>

This table now tells us that for the time period between 2023-03-08 12:40:00 and 2023-03-10 12:50:00 we believed ('Assertive' interval) that the data in this row was valid from 2023-03-08 12:40:00 indefinitely. Following this update we need to now append a row indicating the change in the validity of the data in the row. Therefore our table looks like

|Effective_From|Effective_To|Assertive_From|Assertive_To|Rows_per_Minute|Number_of_Coins_Tracked|Number_of_Exchanges_Tracked|Bucket_Interval|
|----------|--------|----|---------|---------|--------|----------|------|
|2023-03-08 12:40:00|NULL|2023-03-08 12:40:00|2023-03-10 12:50:00|92|50|3|3|
|2023-03-08 12:40:00|2023-03-10 12:50:00|2023-03-08 12:40:00|NULL|92|50|3|3|

<br/>

The latest row lets us know that the information was valid from 2023-03-08 12:40:00 to 2023-03-10 12:50:00 ('Effective' interval) and that we believe this information ('Assertive' interval) from 2023-03-10 12:50:00 to current time. Lastly we just need to add a column to the MetaData table corresponding to the new information we believe after 2023-03-10 12:50:00. The table resulting will look something like

|Effective_From|Effective_To|Assertive_From|Assertive_To|Rows_per_Minute|Number_of_Coins_Tracked|Number_of_Exchanges_Tracked|Bucket_Interval|
|----------|--------|----|---------|---------|--------|----------|------|
|2023-03-08 12:40:00|NULL|2023-03-08 12:40:00|2023-03-10 12:50:00|92|50|3|3|
|2023-03-08 12:40:00|2023-03-10 12:50:00|2023-03-08 12:40:00|NULL|92|50|3|3|
|2023-03-10 12:50:00|NULL|2023-03-10 12:50:00|NULL|96|52|3|3|

<br/>


# **Expanding The App**

## **Adding Another Exchange**

The Crypto-Intellect app is a dockerised pipeline that pulls data, peforms data quality checks, normalises data and saves it to a user specified output. Although the individual logic changes depending of the format of the response from the API, as seen by each exchange being represented by its own class, the methods which need to be implemented are the same. In order to reflect this each exchange class inherits an abstract **_BaseExchange_** class forcing each exchange class to have specific methods when created. Using inheritance for each exchange class allows for the individual logic requirements for each exchange API to be met while allowing for generic Python scripts to be run as part of the pipeline, with the only requirement that the classes passed as arguments are of type BaseExchange.

This simple inheritance patterns allows users to exec into the app docker container _(see above alias)_ and from there nagigate to the **_src/data_ETL_jobs/exchanges_** folder where they can create their own exchange.

<br/>

### **Steps in Creating a New Exchange**

Each exchange must contain the URL to the API Candlesticks endpoint and the name of the exchange as class attributes

```python
class NewExchange(BaseExchange):
	candle_sticks_url = API_ENDPOINT
	api_name = EXCHANGE_NAME
```
After adding these attributes the user must create all abstract methods descibed in the **_BaseExchange_** class

```python
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
```

How each of these methods is implemented is dictated by the format of the json response the specific API returns and will be down to the user to decide. The **_request_data_** method is an asynchronous method and the API request must be made using pythons aiohttp librarys, [link to library github] **_ClientSession.get()_** method, the response of which must be awaited. 

Unfortunately as each API response is different users must be familiar with the format and have strong programming skills in order to add an exchange to the app. To see how we have implemented the above methods look through our exchange documents in **_src/data_ETL_jobs/exchanges_** folder. 

After building out all of the required abstract methods users will finally need to add a get data method to control asynchronous running of the requests. If users are pulling from and exchange that requires the **_feed_forward_** function to be called, it is recommended that it is called inside this function. A get data function that requires the **_feed_forward_** method to be called is shown below.

```python
    async def get_data(self, *Coins, startTime=None, endTime=None):
        try:
            if startTime == None:
                startTime = self.startTime
            if endTime == None:
                endTime = self.endTime
            else:
                endTime = endTime + timedelta(minutes=1)

            async with ClientSession() as session:
                await asyncio.gather(
                    *(
                        self.pull_data(
                            asset_name=coin,
                            startTime=startTime.strftime("%Y-%m-%d %H:%M:%S"),
                            endTime=endTime.strftime("%Y-%m-%d %H:%M:%S"),
                            session=session,
                        )
                        for coin in Coins
                    )
                )

            if self.bucket_interval_value != 1:
                self.data.sort_values(by="TimeStamp", inplace=True)
                self.data = self.aggregator.aggregate_to_bucket_interval(
                    self.data, self.bucket_interval_value
                )

            self.data = feed_forward(self.data, self.api_name)

        except AttributeError as error:
            print(repr(error))
```
	