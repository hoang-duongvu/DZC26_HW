from typing import Iterator, Optional
import pandas as pd


class CSVExtractor:
    def __init__(
        self,
        source: str,
        dtype: Optional[dict] = None,
        parse_dates: Optional[list] = None,
        compression: Optional[str] = None,
        chunk_size: Optional[int] = None
    ):
        self.source = source
        self.dtype = dtype
        self.parse_dates = parse_dates
        self.compression = compression
        self.chunk_size = chunk_size

    def extract(self) -> pd.DataFrame:
        return pd.read_csv(
            self.source,
            dtype=self.dtype,
            parse_dates=self.parse_dates,
            compression=self.compression
        )

    def extract_chunks(self) -> Iterator[pd.DataFrame]:
        return pd.read_csv(
            self.source,
            dtype=self.dtype,
            parse_dates=self.parse_dates,
            compression=self.compression,
            chunksize=self.chunk_size,
            iterator=True
        )
