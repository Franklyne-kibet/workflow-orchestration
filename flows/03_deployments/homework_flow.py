from pathlib import Path
import pandas as pd
from datetime import timedelta
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

@task(retries=3,cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame :
    """Read fhv taxi data from the web"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Clean the dataset"""
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    print(f"columns: {df.dtypes}")
    return df

@task()
def write_local(df:pd.DataFrame, taxi:str, dataset_file:str) -> Path:
    """Write data to a local file"""
    path = Path(f"data/{taxi}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path:Path) -> None:
    """ Upload Local File"""
    gcs_block = GcsBucket.load("zoom-gcs")
    path = Path(path).as_posix()
    gcs_block.upload_from_path(
        from_path  = path,
        to_path = path,
        timeout = (10,5000)
    )
    return

@flow()
def etl_web_to_gcs(year:int, month:int , taxi:str):
    """Main ETL Function"""
    dataset_file = f"{taxi}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,taxi, dataset_file) 
    write_gcs(path)

@flow()
def etl_parent_flow(
    months:list[int] = [1,2,3], year:int = 2019, taxi:str = "fhv"
):
    for month in months:
        etl_web_to_gcs(year,month,taxi)

if __name__ == "__main__":
    taxi = "fhv"
    year = 2019
    months = [1,2,3]
    etl_parent_flow(year,months,taxi)