import pandas as pd 
from deco import concurrent, synchronized

#df = pd.read_csv('20201013_1538_indo_banking_db_public_transactions.csv.gz', compression='gzip', header=0)

c_size = 200000

@concurrent
def async_process():
    for c_df in pd.read_csv('test.csv.gz', compression='gzip', header=0, chunksize=c_size):
        df_exploded = c_df['records'].apply(pd.Series)
        #c_df['meta'] = 'load'
        #print(c_df)

@synchronized
def run(size):
    for i in range(size):
        async_process()

if __name__ == '__main__':
    run(2)

