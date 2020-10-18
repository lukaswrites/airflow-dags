#!/bin/bash
sudo /bin/dd if=/dev/zero of=/var/swap.1 bs=1M count=8192
sudo /sbin/mkswap /var/swap.1
sudo chmod 600 /var/swap.1
sudo /sbin/swapon /var/swap.1
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt install --assume-yes python3.8
sudo apt-get install --assume-yes python3.8-venv
python3.8 -m venv env
source env/bin/activate
sudo apt-get install --assume-yes postgresql
sudo apt-get install --assume-yes python-psycopg2
sudo apt-get install --assume-yes libpq-dev
sudo apt install --assume-yes libcurl4-openssl-dev libssl-dev
sudo apt-get install --assume-yes gcc python3.8-dev
cp airflow-dags/requirements.txt .
pip install -r requirements.txt
mkdir -p /home/cloud_user/repo/airflow-dags/
cp -R /airflow-dags /home/cloud_user/repo/
echo "init" > /sample.txt
airflow
cp /airflow-dags/airflow.cfg ~/airflow/airflow.cfg
supervisord -c /airflow-dags/supervisord.conf
airflow worker
