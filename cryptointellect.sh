#!/bin/bash

if ! [ -x "$(command -v docker)" ]; then
    echo "Please install docker!"
	exit 1
fi

mkdir -p -- "./cryptointellect/csv/"
CONFIG_FILEPATH=""

while test $# -gt 0; do
  case "$1" in
    -h|--help)
      echo "---------- DATA INTELLECT CRYPTO CAPTURE APPLICATION: CryptoIntellect ----------"
      echo "Script to pull docker image, create aliases and mount custom config"
      echo ""
      echo "source install_cryptointellect.sh [options] [arguments]"
      echo ""
      echo "options:"
      echo "-h, --help                          show brief help"
      echo "-c, --config CONFIG_FILEPATH        specify custom config file to mount"
      echo "-a, --alias                         view aliases created for application control"
      exit 0
      ;;
    -c|--config)
      shift
      if test $# -gt 0; then
        CONFIG_FILEPATH=$1
        cp "$CONFIG_FILEPATH" ./cryptointellect/config.yml
      else
        echo "No config filepath specified!"
        exit 1
      fi
      shift
      ;;
    -a|--alias)
      echo "ci_start: Starts the application"
      echo "ci_stop: Stops the application"
      echo "ci_attach: Opens a terminal attached to the running application"
      echo "ci_backfill -f/--from YYYY-MM-DD -t/--to YYYY-MM-DD: Only compaitable with database mode. Fills crypto data from date specified to date specified"
      exit 0
      ;;
    *)
      echo "$1 is not a recognized flag! See -h/--help for help."
      break
      ;;
  esac
done

##remove old container 
docker rm -f cryptointellect_container

if ! [ -z "$CONFIG_FILEPATH" ]; then
  echo "FROM public.ecr.aws/c5m8g6i6/cryptointellect:latest" > ./cryptointellect/Dockerfile
  echo "COPY ./cryptointellect/config.yml /src/" >> ./cryptointellect/Dockerfile 
  echo "RUN python3 src/Initial_startup.py" >> ./cryptointellect/Dockerfile
  echo "CMD [\"cron\", \"-f\"]" >> ./cryptointellect/Dockerfile

  docker build -f ./cryptointellect/Dockerfile . -t cryptointellect
  docker run --name cryptointellect_container -d -v ./cryptointellect/csv/:/src/csv_folder/:rw cryptointellect
else
  docker pull public.ecr.aws/c5m8g6i6/cryptointellect:latest
  docker run --name cryptointellect_container -d -v ./cryptointellect/csv/:/src/csv_folder/:rw public.ecr.aws/c5m8g6i6/cryptointellect:latest
fi

echo "alias ci_start='docker start cryptointellect_container'" >> ~/.bashrc
echo "alias ci_stop='docker stop cryptointellect_container'" >> ~/.bashrc
echo "alias ci_attach='docker exec -it cryptointellect_container bash'" >> ~/.bashrc
echo "alias ci_backfill='docker exec -it cryptointellect_container python3 src/data_ETL_jobs/backfill_historical_data_script.py'" >> ~/.bashrc

source .bashrc