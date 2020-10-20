FROM ubuntu:18.04

WORKDIR /

COPY requirements.txt .

RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt install --assume-yes python3.8
RUN apt-get install --assume-yes python3.8-venv
RUN python3.8 -m venv env
RUN source env/bin/activate
RUN apt-get install --assume-yes postgresql
RUN apt-get install --assume-yes python-psycopg2
RUN apt-get install --assume-yes libpq-dev
RUN apt install --assume-yes libcurl4-openssl-dev libssl-dev
RUN apt-get install --assume-yes gcc python3.8-dev
RUN pip install -r requirements.txt


CMD [ "airflow", "worker" ]