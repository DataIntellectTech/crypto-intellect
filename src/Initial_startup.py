import yaml
from crontab import CronTab
from bucket_interval_conversion import convert_bucket_interval
from validate_config import validate_config_schema


if __name__ == "__main__":
    validate_config_schema()
    convert_bucket_interval()

    with open("/src/config.yml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    if config["save_to_database"] == True:
        with open("/src/database_setup/create_db_script.py", "r") as f:
            exec(f.read())

        with open("/src/coin_ETL_jobs/coin_script.py", "r") as f:
            exec(f.read())

        with open("/src/data_ETL_jobs/save_exchanges_script.py", "r") as f:
            exec(f.read())

        with open("/src/MetaData_jobs/setup_MetaData.py", "r") as f:
            exec(f.read())

    bucket_interval_time_mins = config["bucketing_interval"]
    with CronTab(user="root") as cron:
        cron.remove_all()
        job1 = cron.new(
            command=". $ROOT/src/config.yml; /usr/bin/python3 /src/data_ETL_jobs/data_ETL_script.py",
            comment="CoinETLjob",
        )
        job1.minute.every(bucket_interval_time_mins)

        job2 = cron.new(
            command=". $ROOT/src/config.yml; /usr/bin/python3 /src/MetaData_jobs/MetaData_script.py",
            comment="MetaDataJob",
        )
        job2.minute.every(bucket_interval_time_mins)

        cron.write()
