from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color:str,year:int,month:int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../")
    return Path(f"../{gcs_path}")

total_rows = 0

@task()
def read_path(path: Path) -> pd.DataFrame:
    """Read the DataFrame"""
    df = pd.read_parquet(path)

    global total_rows
    total_rows += len(df)
    print(f"rows :{len(df)}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table = "de_zoomcamp.rides",
        project_id ="ny-rides-franklyne" ,
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = "append",
    )

@flow()
def el_gcs_to_bq(color: str ,month:int, year:int) -> None:
    """ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = read_path(path)
    write_bq(df)

@flow(log_prints = True)
def el_main_flow(color : str ,months:list[int] = [2,3], year: int = 2019):
    for month in months:
        el_gcs_to_bq(color,month,year)

    print(f"Total rows Processed :{total_rows}")
    
if __name__ == "__main__":
    color = "Yellow"
    months = [2,3]
    year = 2019
    el_main_flow(color,months,year)