import yaml
from crontab import CronTab
from bucket_interval_conversion import convert_bucket_interval

with open("/src/config.yml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


if __name__ == "__main__":
    if isinstance(config["bucketing_interval"], str):
        convert_bucket_interval()
        with open("/src/config.yml", "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

    bucket_interval_time_mins = config["bucketing_interval"]
    with CronTab(user="root") as cron:
        for job in cron:
            if job.comment == "CoinETLjob":
                job.minute.every(bucket_interval_time_mins)
            elif job.comment == "MetaDataJob":
                job.minute.every(bucket_interval_time_mins)
        cron.write()

    if config["save_to_database"] == True:
        with open("/src/coin_ETL_jobs/coin_script.py", "r") as f:
            exec(f.read())

        with open("/src/data_ETL_jobs/save_exchanges_script.py", "r") as f:
            exec(f.read())
