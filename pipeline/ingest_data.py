#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import os
import conf
import logging

logger = logging.getLogger(__name__)

def run ():
  logger.info("Starting data ingestion process...")

  logger.info(f"Creating database engine for PostgreSQL database...")
  engine = create_engine(f'postgresql+psycopg://{conf.pg_user}:{conf.pg_pass}@{conf.pg_host}:{conf.pg_port}/{conf.pg_db}')
  logger.info(f"Database engine created.")

  logger.info(f"Reading data from {conf.link}...")
  try:
    df_iter = pd.read_csv(
        conf.link,
        dtype=conf.dtype,
        parse_dates=conf.parse_dates,
        iterator=True,
        chunksize=100000
    )
  except FileNotFoundError as e:
    print(f"File not found: {conf.link}")
    raise
  except Exception as e:
    print(f"An error occurred: {e}")
    raise

  logger.info(f"Data read successfully from {conf.link}.")

  # insert data into the database in chunks to avoid memory issues. The first chunk is used to create the table with the correct schema, and subsequent chunks are appended to the table.
  first_chunk = next(df_iter)

  # add two new columns: source and load_timestamp. The source column indicates the source of the data (in this case, the URL from which the data was downloaded), and the 
  # load_timestamp column records the time at which the data was loaded into the database.
  first_chunk["source"] = conf.link
  first_chunk["load_timestamp"] = pd.Timestamp.now()

  logger.info(f"Creating table {conf.target_table} in the database...")
  # Create table with the correct schema by writing an empty DataFrame (with the same  columns and data types) to the database.
  try:
    first_chunk.head(0).to_sql(
        name=conf.target_table,
        con=engine,
        if_exists="replace"
    )
  except Exception as e:
    logger.error(f"Error occurred while creating table {conf.target_table}: {e}")
    raise

  logger.info(f"Table {conf.target_table} created successfully in the database.")

  # Insert the first chunk of data into the database
  logger.info(f"Inserting first chunk into table {conf.target_table}...")
  try:
    first_chunk.to_sql(
        name=conf.target_table,
        con=engine,
        if_exists="append"
    )
  except Exception as e:
    logger.error(f"Error occurred while inserting first chunk into table {conf.target_table}: {e}")
    raise
  logger.info(f"First chunk inserted into table {conf.target_table}.")

  # Insert the remaining chunks of data into the database. This is done in a loop to handle large datasets that may not fit into memory all at once. The tqdm library is used to 
  # provide a progress bar for better visibility of the insertion process.
  logger.info(f"Inserting remaining chunks into table {conf.target_table}...")  

  for i, df_chunk in enumerate(tqdm(df_iter), start=1):
    df_chunk["source"] = conf.link
    df_chunk["load_timestamp"] = pd.Timestamp.now()

    logger.info(f"Inserting chunk {i} into table {conf.target_table}...")
    try:
        df_chunk.to_sql(
            name=conf.target_table,
            con=engine,
            if_exists="append",
            index=False
        )
    except Exception as e:
        logger.error(
            f"Failed inserting chunk {i} into table {conf.target_table}: {e}"
        )
        raise
    logger.info(f"Chunk {i} inserted into table {conf.target_table}.")

  logger.info(f"Data ingestion process completed successfully. All data from {conf.link} has been inserted into table {conf.target_table}.")
