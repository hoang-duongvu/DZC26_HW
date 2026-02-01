from pipeline.config import settings

YELLOW_TAXI_URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{settings.DATA_YEAR}-{settings.DATA_MONTH:02d}.csv.gz"

# Column data types for pandas
YELLOW_TAXI_DTYPES = {
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
    "congestion_surcharge": "float64",
}

# Columns to parse as datetime
YELLOW_TAXI_PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]
