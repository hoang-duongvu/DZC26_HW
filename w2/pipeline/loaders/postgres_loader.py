from typing import Iterator, Optional
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy.engine import Engine

class PostgresLoader:
    def __init__(
        self,
        table_name: str,
        engine: Engine,
        if_exists: str = "append",
        index: bool = False
    ):
        self.table_name = table_name
        self.engine = engine
        self.if_exists = if_exists
        self.index = index

    def load(self, df: pd.DataFrame) -> int:
        """Load full table"""
        df.to_sql(
            name=self.table_name,
            con=self.engine,
            if_exists=self.if_exists,
            index=self.index
        )
        return len(df)

    def load_chunks(
        self,
        chunks: Iterator[pd.DataFrame],
        show_progress: bool = True
    ) -> int:
        """Iterator data loading"""
        total_rows = 0
        iterator = tqdm(chunks) if show_progress else chunks

        for i, chunk in enumerate(iterator):
            if i == 0:
                chunk.head(0).to_sql(
                    name=self.table_name,
                    con=self.engine,
                    if_exists="replace",
                    index=self.index
                )
            chunk.to_sql(
                name=self.table_name,
                con=self.engine,
                if_exists="append",
                index=self.index
            )

            total_rows += len(chunk)

            if show_progress:
                iterator.set_postfix({"rows": total_rows})

        return total_rows
