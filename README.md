# data-pipeline-taxi-dataset

This project builds a data pipeline that ingests NYC Taxi trip data from CSV files and loads it into a PostgreSQL database.
The pipeline downloads the data from the official NYC TLC dataset release and uses a Python script to transform and populate a PostgreSQL table.
The entire workflow is containerized using Docker for reproducibility and easy deployment.
