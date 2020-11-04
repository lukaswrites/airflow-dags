# Container image that runs your code
FROM python:3.8-slim-buster

# Copies your code file from your action repository to the filesystem path `/` of the container

RUN apt update
RUN apt install --assume-yes git
RUN apt-get install --assume-yes postgresql
RUN apt-get install --assume-yes python-psycopg2
RUN apt-get install --assume-yes libpq-dev

COPY requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

COPY entrypoint.sh /entrypoint.sh
COPY sql_check.py /sql_check.py
# Code file to execute when the docker container starts up (`entrypoint.sh`)
ENTRYPOINT ["/entrypoint.sh"]