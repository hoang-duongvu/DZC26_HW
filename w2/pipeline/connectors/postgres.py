from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

class PostgresConnector:
    def __init__(self, url, echo = False):
        self.postgres_url = url
        self.echo = echo
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                url=self.postgres_url,
                echo=self.echo
            )
        return self._engine

    def get_engine(self) -> Engine:
        return self.engine
        
    def dispose(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None

