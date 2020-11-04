import re
import yaml
import psycopg2
import sys


def validate_yaml(file_path):
    content = yaml.safe_load(open(file_path, 'r'))
    content = {k.lower(): v for k, v in content.items()}
    return content

def clean_sql(sql):
    m = re.compile('(?:(--.*)\n?)|(\/\*(?:.|[\r\n])*?\*\/)', flags=re.IGNORECASE)
    return re.sub(m, '', sql or '')

def get_transactions(sql):
    eof_mark = sql.find(";")
    if eof_mark > 0 :
        statements = sql.replace(";"," where 1= 0; ").split(";")
        transactions = [clean_sql('begin transaction; '+x+'; rollback; ') for x in statements[:-1]]
        return transactions
    return None

def test_execute_sql(transactions):
    db_host= '13.212.73.167'
    db = 'cloud_user'
    user= 'cloud_user'
    password='cybersoft'
    conn = psycopg2.connect(host=db_host, database=db,
                            user=user, password=password)

    cursor = conn.cursor()
    for t in transactions:
        print(t)
        cursor.execute(t)
    return None

def get_yaml_from_file(filepath):
    yaml_files = open('commit_list.txt', 'r')
    yaml_list = [x.rstrip("\n") for x in yaml_files.readlines() if ".yaml" in x.rstrip("\n")]
    print(yaml_list)
    return yaml_list

def run_test(yaml_list):
    yamls = get_yaml_from_file(yaml_list)
    for i in yamls:
        yaml_content = validate_yaml(i)
        sql = yaml_content['sql']
        transactions = get_transactions(sql)
        test_execute_sql(transactions)


if __name__ == '__main__':
    args = sys.argv
    run_test(args[1])
