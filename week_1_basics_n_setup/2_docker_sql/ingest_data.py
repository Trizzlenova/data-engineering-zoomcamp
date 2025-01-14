#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import argparse
import os
from time import time
# from pyarrow.parquet import ParquetFile
# import pyarrow as pa
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # download the csv
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:

        try:
            t_start = time()
            
            df = next(df_iter)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            
            print(f'inserted another chunk, took {t_end - t_start} seconds')
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--table_name', help='table name where results will be written')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)