from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries = 3)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read data from web into pandas dataframe'''

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints = True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    '''Fix dtype issues'''
    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task(log_prints = True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    '''Write dataframe out as a parquet file'''
    path = Path(f'data/fhv_parquet/{dataset_file}.parquet')
    df.to_parquet(path, compression = 'gzip')
    return path

@task(log_prints = True)
def write_gcs(path: Path) -> None:
    '''Uploading local parquet file to GCS'''
    gcp_cloud_storage_bucket_block = GcsBucket.load('prefect-gcs')
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = f'{path}',
        to_path = path
    )
    return

@flow(log_prints = True)
def etl_web_to_gcs(year: int, month: int) -> None:
    '''The main ETL function'''
    dataset_file = f'fhv_tripdata_{year}-{month:02}'
    print(dataset_file)
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    print(f'df: {len(df)}')
    df_clean = clean(df)
    print(f'df_clean: {len(df_clean)}')
    path = write_local(df_clean, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    year = 2019
    for month in range(1,13):
        etl_web_to_gcs(year, month)