import os
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from sqlalchemy import create_engine

def get_env():
    load_dotenv()
    return {
        "pg_user": os.getenv("POSTGRES_USER", "root"),
        "pg_password": os.getenv("POSTGRES_PASSWORD", "<PASSWORD>"),
        "pg_host": os.getenv("POSTGRES_HOST", "localhost"),
        "pg_port": os.getenv("POSTGRES_PORT", "5432"),
        "pg_db": os.getenv("POSTGRES_DB", "postgres"),
        "month": os.getenv("MONTH", 1),
        "year": os.getenv("YEAR", 2021),
        "target_table": os.getenv("TARGET_TABLE", "yellow_taxi_data"),
        "chunksize": os.getenv("CHUNKSIZE", 100)
    }

def read_data_from_file(prefix, month, year, dtype, parse_dates, chunksize):
    df_iter = pd.read_csv(
        f'{prefix}/yellow_tripdata_{year}-{int(month):02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=int(chunksize)
    )
    return df_iter

def get_db_engine(pg_user, pg_password, pg_host, pg_port, pg_db):
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
    return engine

def load_data_to_db(df_iter, engine, target_table, chunksize):
    print(pd.io.sql.get_schema(next(df_iter), name=target_table, con=engine))

    first = True
    for df in tqdm(df_iter):
        if first:
            df.head(0).to_sql(target_table, engine, if_exists='replace')
            first = False
            print(f"Created table {target_table}!")

        df.to_sql(target_table, engine, if_exists="append")
        print("Inserted chunk: ", len(df))

if __name__ == "__main__":
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

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"

    env = get_env()
    try:
        df_iter = read_data_from_file(
            prefix, env["month"], env["year"],
            dtype, parse_dates, env["chunksize"]
        )

        engine = get_db_engine(
            env["pg_user"], env["pg_password"],
            env["pg_host"], env["pg_port"], env["pg_db"]
        )

        load_data_to_db(df_iter, engine, env["target_table"], env["chunksize"])
    except Exception as e:
        print(f"ERROR: {e}")










