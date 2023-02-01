from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries = 3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    '''Download trip data from GCS'''
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('prefect-gcs')
    gcs_block.get_directory(from_path = gcs_path, local_path = f'../data/')
    return Path(f'../data/{gcs_path}')

@task()
def transform(path: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    '''Write dataframe to BigQuery'''
    gcp_credentials_block = GcpCredentials.load("prefect-gcp-creds")

    df.to_gbq(
        destination_table = 'deprefect.rides',
        project_id = 'prefect-de-zoomcamp-376404',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = 'append'
    )

@flow(log_prints = True)
def etl_gcs_to_bq(color: str = 'yellow', year: int = 2021, months: list[int] = [1, 2]):
    '''Main ETL flow to load data into Big Query'''
    row_count = 0

    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        row_count += len(df)
        write_bq(df)
    
    print(row_count)

if __name__ == '__main__':
    etl_gcs_to_bq(year=2019, months=[2,3])