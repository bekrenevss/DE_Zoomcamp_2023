from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    #print(df.head(2))
    #print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet").as_posix()
    path_local = Path(f"C:/Users/bekrenevHome/git/week_2/data/{color}/{dataset_file}.parquet").as_posix()
    df.to_parquet(path_local, compression="gzip")
    return path, path_local

@task()
def write_gcs(path: Path, path_local: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path_local, to_path=path, timeout=600)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ELT function"""
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path, path_local = write_local(df_clean, color, dataset_file)
    print(path)
    print(path_local)
    write_gcs(path, path_local)
               
if __name__ == '__main__':
    etl_web_to_gcs()