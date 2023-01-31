from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(
    retries = 3, 
    cache_key_fn = task_input_hash, 
    cache_expiration = timedelta(days = 1)
)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read data from web into pandas dataframe'''

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints = True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    '''Fix dtype issues'''
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task(log_prints = True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    '''Write dataframe out as a parquet file'''
    path = Path(f'data/{color}/{dataset_file}.parquet')
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

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    '''The main ETL function'''
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = 'yellow'
) -> None:
    print('color: ' + color)
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    months = [1, 2, 3]
    year = 2021
    color = 'yellow'
    etl_parent_flow(months, year, color)


# steps to deploy
# 1. Terminal: $ prefect deployment build ./flows/03_deployment/parameterized_flow:etl_parent_flow -n "Parameterized ETL"
# 2. in new .yaml file, update "parameters" dict {"color": "yellow", "months": [1, 2, 3], "year": 2021}
# 3. Terminal: $ prefect deployment apply <.yaml file>
# 4. Check Deployments in Prefect UI
# 5. Check Work Queues in Prefect UI
# 6. Terminal: $ prefect agent start --work-queue "default"
# 7. Check Flow Runs to see if state is running
# 8. Setup Notification >> Run States = Failed >> Slack Notification

# need help? prefect deployment build --help

# create ETL - $ prefect deployment build <file_path>:<main_function_name> -n <etl_name> --cron "0 0 * * *" (look up CRON, this will run every day at 12am)