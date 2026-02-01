from pydantic_settings import SettingsConfigDict
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    POSTGRES_USER: str = "root"
    POSTGRES_PASSWORD: str = "root"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5433
    POSTGRES_DB: str = "ny_taxi"

    CHUNK_SIZE: int = 10000
    ECHO_SQL: bool = False

    DATA_YEAR: int = 2021
    DATA_MONTH: int = 1

    COLOR: str = "green"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def postgres_connect_url(self) -> str:
        return f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

settings = Settings()