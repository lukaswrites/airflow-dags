FROM ubuntu:18.04

USER root

ENV AIRFLOW_HOME=/root/airflow
WORKDIR /
COPY requirements.txt .
RUN mkdir -p /root/airflow
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
RUN mkdir -p /airflow-dags
COPY airflow.cfg /root/airflow/airflow.cfg
COPY dag_builder.py /airflow-dags/dag_builder.py 

EXPOSE 8793

CMD airflow worker