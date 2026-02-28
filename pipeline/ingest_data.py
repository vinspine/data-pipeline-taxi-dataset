#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)


# we need to specify the type of the fields to avoid erorrs
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# indicate the fields that are date types
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)



@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run (pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
  # create a connection to postgresql 
  engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

  # split the records into batches
  df_iter = pd.read_csv(
      prefix + 'yellow_tripdata_2021-01.csv.gz',
      dtype=dtype,
      parse_dates=parse_dates,
      iterator=True,
      chunksize=100000
  )

  # insert data into the table in batches
  first_chunk = next(df_iter)

  # Create table with the correct schema by writing an empty DataFrame (with the same columns and data types) to the database. This ensures that the table is created with the appropriate structure before we start inserting data.
  first_chunk.head(0).to_sql(
      name=target_table,
      con=engine,
      if_exists="replace"
  )

  print("Table created")

  first_chunk.to_sql(
      name=target_table,
      con=engine,
      if_exists="append"
  )

  print("Inserted first chunk:", len(first_chunk))

  for df_chunk in tqdm(df_iter):
      df_chunk.to_sql(
          name=target_table,
          con=engine,
          if_exists="append"
      )
      print("Inserted chunk:", len(df_chunk))


if __name__ == "__main__":
    run()