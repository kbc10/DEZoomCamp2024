#!/usr/bin/env python
# coding: utf-8

# import os
# import argparse
# import pandas as pd
# import pyarrow.parquet as pq
# from sqlalchemy import create_engine
# from time import time

# # df = pd.read_csv('yellow_tripdata_2021-01.csv', index_col=0, nrows = 100)
# # print(pd.io.sql.get_schema(df, name = 'yellow_taxi_data', con=engine))

# def main(params):
#     print("starting...")
#     user = params.user
#     password = params.password
#     host = params.host 
#     port = params.port
#     db = params.db
#     table_name = params.table_name
#     url = params.url

#     parquet_name = 'output.parquet'
    
#     os.system(f"wget {url} -O {parquet_name}")
    
#     csv_name = 'output.csv'

#     trips = pq.read_table('output.parquet')
#     trips = trips.to_pandas()
#     trips.to_csv('ouput.csv')

#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

#     df_iter = pd.read_csv(csv_name, index_col=0, iterator=True, chunksize=100000)

#     df = next(df_iter)

#     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

#     df.head(n=0).to_sql(name = table_name, con=engine, if_exists = 'replace')
    
#     while True:
#         t_start = time()
        
#         df = next(df_iter)
        
#         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
#         df.to_sql(name = table_name, con=engine, if_exists = 'append')

#         t_end = time()
        
#         print('inserted  another chunk..., took %.3f seconds' % (t_end - t_start))

# if __name__ == '__main__':
#     print("controller")
#     parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

#     parser.add_argument('--user', required=True, help='user name for postgres')
#     parser.add_argument('--password', required=True, help='password for postgres')
#     parser.add_argument('--host', required=True, help='host for postgres')
#     parser.add_argument('--port', required=True, help='port for postgres')
#     parser.add_argument('--db', required=True, help='database name for postgres')
#     parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
#     parser.add_argument('--url', required=True, help='url of the csv file')

#     args = parser.parse_args()

#     main(args)



import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pyarrow.parquet as pq

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    # if url.endswith('.csv.gz'):
    #     csv_name = 'output.csv.gz'
    # else:
    #     csv_name = 'output.csv'

    # os.system(f"wget {url} -O {csv_name}")

    parquet_name = 'output.parquet'
    
    os.system(f"wget {url} -O {parquet_name}")
    
    csv_name = 'output.csv'

    trips = pq.read_table('output.parquet')
    trips = trips.to_pandas()
    trips.to_csv('output.csv')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
