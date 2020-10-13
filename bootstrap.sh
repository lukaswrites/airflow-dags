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
airflow
cp airflow-dags/airflow.cfg airflow
airflow worker