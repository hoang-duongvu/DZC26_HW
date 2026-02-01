from pipeline.extractors import CSVExtractor
from pipeline.loaders import PostgresLoader
from pipeline.schemas import GREEN_TAXI_DTYPES, GREEN_TAXI_PARSE_DATES, GREEN_TAXI_URL, YELLOW_TAXI_DTYPES, YELLOW_TAXI_PARSE_DATES, YELLOW_TAXI_URL
from pipeline.connectors import PostgresConnector
from pipeline.config import settings

def run_green_taxi_pipeline(url: str = GREEN_TAXI_URL, table_name: str = "green_taxi") -> int:
    print("Starting Green Taxi Pipeline")

    connector = PostgresConnector(settings.postgres_connect_url)

    # Extract
    print(url)
    extractor = CSVExtractor(
        source=url,
        dtype=GREEN_TAXI_DTYPES,
        parse_dates=GREEN_TAXI_PARSE_DATES,
        chunk_size=settings.CHUNK_SIZE,
        compression="gzip"
    )

    # Load
    loader = PostgresLoader(table_name=table_name, engine=connector.engine)

    # Execute
    chunks = extractor.extract_chunks()
    total_rows = loader.load_chunks(chunks, show_progress=True)

    print(f"Completed! Loaded {total_rows:,} rows into '{table_name}'")
    return total_rows

def run_yellow_taxi_pipeline(url: str = YELLOW_TAXI_URL, table_name: str = "yellow_taxi") -> int:
    connector = PostgresConnector(settings.postgres_connect_url)
    print(url)
    extractor = CSVExtractor(
        source=url,
        dtype=YELLOW_TAXI_DTYPES,
        parse_dates=YELLOW_TAXI_PARSE_DATES,
        compression="gzip",
        chunk_size=settings.CHUNK_SIZE
    )

    df_iter = extractor.extract_chunks()

    loader = PostgresLoader(table_name=table_name, engine=connector.engine)

    total_rows = loader.load_chunks(df_iter, show_progress=True)

    print(f"Completed! Loaded {total_rows:,} rows into '{table_name}'")
    return total_rows

def main():
    if settings.COLOR == "green":
        run_green_taxi_pipeline()
    elif settings.COLOR == "yellow":
        run_yellow_taxi_pipeline()

if __name__ == "__main__":
    main()
