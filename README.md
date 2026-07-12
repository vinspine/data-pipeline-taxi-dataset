# Dockerized taxi data pipeline

This project implements a data pipeline that ingests NYC Taxi trip data from CSV files and loads it into a PostgreSQL database.

The pipeline downloads the data from the official NYC TLC dataset release and uses a Python ingestion script to process and populate PostgreSQL tables.

The Python script receives the year and month of the dataset as input parameters. It then:

1. Downloads the corresponding NYC Taxi CSV file.
2. Establishes a connection to the PostgreSQL database.
3. Creates a new table named `yellow_taxi_data_{db_year}_{db_month}`. If the table already exists, it is replaced.
4. Loads the CSV data into PostgreSQL in chunks of 100,000 records to avoid memory issues.

Before loading the data into the database, two additional columns are added:

- **source**: the URL from which the CSV file was downloaded.
- **load_timestamp**: the timestamp indicating when the record was loaded into the database.

To make the pipeline reproducible and easier to deploy, Docker Compose is used to define four services:

- **pgdatabase**: PostgreSQL database service.
- **pgadmin**: web interface used to inspect and query the database tables.
- **ingest_2019_01**: ingestion service based on the `ingest_data.py` script. It downloads the NYC Taxi Trip dataset for January 2019 and loads it into PostgreSQL.
- **ingest_2019_02**: ingestion service similar to the previous one, but using the February 2019 dataset.

The following diagram summarizes the Docker Compose architecture:


<p align="center">
<img width="371" height="461" alt="Docker_compose_content" src="https://github.com/user-attachments/assets/5aa28387-4666-4462-9238-37925a513430" />
</p>


While the entire worflow is summarized by the following picture: 

<p align="center">
<img width="791" height="501" alt="Workflow_taxi_data" src="https://github.com/user-attachments/assets/b8f6323b-90f1-41eb-ae69-55a1664de972" />
</p>



