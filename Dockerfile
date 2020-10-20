FROM ubuntu:18.04

USER root

ENV AIRFLOW_HOME=~/airflow
WORKDIR /
COPY requirements.txt .
RUN mkdir ~/airflow
COPY airflow.cfg ~/airflow/airflow.cfg
RUN apt-get update
RUN apt-get install --assume-yes software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt install --assume-yes python3.8
RUN apt-get install --assume-yes python3.8-venv
RUN apt-get install --assume-yes postgresql
RUN apt-get install --assume-yes python-psycopg2
RUN apt-get install --assume-yes libpq-dev
RUN apt install --assume-yes libcurl4-openssl-dev libssl-dev
RUN apt-get install --assume-yes gcc python3.8-dev
RUN apt install --assume-yes curl
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python3.8 get-pip.py
RUN pip3 install -r requirements.txt
RUN mkdir -p /home/cloud_user/repo/airflow-dags/
COPY dag_builder.py /home/cloud_user/repo/

CMD [ "airflow", "worker" ]