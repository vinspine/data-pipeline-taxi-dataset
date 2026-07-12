# data-pipeline-taxi-dataset

This project builds a data pipeline that ingests NYC Taxi trip data from CSV files and loads it into a PostgreSQL database.
The pipeline downloads the data from the official NYC TLC dataset release and uses a Python script to transform and populate a PostgreSQL table.

In particular, the python script receives as input the month and the year of the file csv that will be downloaded, establishes a connection to postgreSQL database, creates a new table in the schema (if it already exists, it will be replaced with the new one) named: "yellow_taxi_data_{db_year}_{db_month}" and upload the csv file in chunks of  100.000 records. 

In order to make the pipeline even more interesting, we use a docker compose file, where we define 4 services:
- pgdatabase: the PostegreSQL service
- pgadmin: web interface service to query the tables
- ingest_2019_01: service based on the ingest_data.py script that downloads the csv of NYC Taxi Trip data from Jenuary 2019 and upload into the brand-new table
- ingest_2019_02: same as the previous point. It uses the csv of NYC Taxi Data from February 2019

We report this brief schema to summarize the docker compose:


<p align="center">
<img width="371" height="461" alt="Docker_compose_content" src="https://github.com/user-attachments/assets/5aa28387-4666-4462-9238-37925a513430" />
</p>

