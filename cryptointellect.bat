@echo off
docker --version > nul && (
    break
) || (
    echo Please install docker!
    exit 1
)

set CONFIG_FILEPATH=%1
set DIR_FILEPATH=%~dp0cryptointellect\
set CSV_FILEPATH=%DIR_FILEPATH%csv\

mkdir %CSV_FILEPATH%
docker rm -f cryptointellect_container

if defined CONFIG_FILEPATH (
    copy %CONFIG_FILEPATH% %DIR_FILEPATH%
    echo # escape=` > %DIR_FILEPATH%/Dockerfile
    echo FROM public.ecr.aws/c5m8g6i6/cryptointellect:latest >> %DIR_FILEPATH%/Dockerfile
    echo COPY config.yml /src/ >> %DIR_FILEPATH%/Dockerfile 
    echo RUN python3 src/Initial_startup.py >> %DIR_FILEPATH%/Dockerfile
    echo CMD ["cron", "-f"] >> %DIR_FILEPATH%/Dockerfile

    cd %DIR_FILEPATH%
    docker build . -t cryptointellect
    docker run --name cryptointellect_container -d -v %CSV_FILEPATH%:/src/csv_folder/:rw cryptointellect
) else (
    docker pull public.ecr.aws/c5m8g6i6/cryptointellect:latest
    docker run --name cryptointellect_container -d -v %CSV_FILEPATH%:/src/csv_folder/:rw public.ecr.aws/c5m8g6i6/cryptointellect:latest
)
