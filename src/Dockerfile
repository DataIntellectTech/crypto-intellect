FROM ubuntu:22.04

VOLUME ["/src/csv_folder"]

RUN apt-get update && apt-get install -y python3.10

RUN apt-get update && apt-get install -y python3-pip

COPY . /src

RUN pip install -r /src/requirements.txt

RUN apt-get install -y cron

RUN apt-get install nano

RUN chmod +x /src/data_ETL_jobs/data_ETL_script.py 

RUN chmod +x /src/Initial_startup.py

RUN python3 src/Initial_startup.py

CMD ["cron", "-f"]
