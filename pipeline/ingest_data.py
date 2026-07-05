#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import os
import conf

def run ():
  # create a connection to postgresql 
  engine = create_engine(f'postgresql+psycopg://{conf.pg_user}:{conf.pg_pass}@{conf.pg_host}:{conf.pg_port}/{conf.pg_db}')

  # split the records into chunks of 100,000 rows to avoid memory issues when inserting 
  # into the database.
  df_iter = pd.read_csv(
      conf.link,
      dtype=conf.dtype,
      parse_dates=conf.parse_dates,
      iterator=True,
      chunksize=100000
  )

  # insert data into the database in chunks to avoid memory issues. The first chunk is 
  # used to create the table with the correct schema, and subsequent chunks are appended to the table.
  first_chunk = next(df_iter)

  # add two new columns: source and load_timestamp. The source column indicates the source 
  # of the data (in this case, the URL from which the data was downloaded), and the 
  # load_timestamp column records the time at which the data was loaded into the database. 
  # This is useful for tracking and auditing purposes.
  first_chunk["source"] = conf.link
  first_chunk["load_timestamp"] = pd.Timestamp.now()

  # Create table with the correct schema by writing an empty DataFrame (with the same 
  # columns and data types) to the database. This ensures that the table is created with 
  # the appropriate structure before we start inserting data.
  first_chunk.head(0).to_sql(
      name=conf.target_table,
      con=engine,
      if_exists="replace"
  )

  print("Table created")

  # Insert the first chunk of data into the database. This is done after creating the table.
  first_chunk.to_sql(
      name=conf.target_table,
      con=engine,
      if_exists="append"
  )

  print("Inserted first chunk:", len(first_chunk))

  # Insert the remaining chunks of data into the database. This is done in a loop to handle 
  #large datasets that may not fit into memory all at once. The tqdm library is used to 
  # provide a progress bar for better visibility of the insertion process.
  for df_chunk in tqdm(df_iter):
      df_chunk["source"] = conf.link
      df_chunk["load_timestamp"] = pd.Timestamp.now()
      df_chunk.to_sql(
          name=conf.target_table,
          con=engine,
          if_exists="append"
      )
      print("Inserted chunk:", len(df_chunk))
      