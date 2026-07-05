import os

# Environment variables are used to store sensitive information such as database credentials.
pg_user = os.getenv("DB_USER")
pg_pass = os.getenv("DB_PASSWORD")
pg_host = os.getenv("DB_HOST")
pg_port = int(os.getenv("DB_PORT"))
pg_db = os.getenv("DB_NAME")
target_table = os.getenv("DB_TABLE")


# information about the year and month of the data to be ingested is also stored in environment variables.
db_year = os.getenv("DB_YEAR")
db_month = os.getenv("DB_MONTH")


# Read from the URL and limit to 100 rows for testing purposes. This allows us to quickly 
# check if the data is being read correctly without having to download the entire dataset, 
# which can be large and time-consuming.
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
link = f'{prefix}yellow_tripdata_{db_year}-{db_month}.csv.gz'

# We need to specify the type of the fields to avoid errors when inserting into 
# the database. For example, some fields may have missing values, which can cause 
# issues if the data type is not specified correctly. By specifying the data types, 
# we ensure that the data is read correctly and can be inserted into the database 
# without errors.
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

# in order to ensure that the date fields are read correctly, we specify the columns that 
# should be parsed as dates. This allows us to work with these fields as datetime objects 
# in pandas, which makes it easier to perform date-related operations and analyses.
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]
