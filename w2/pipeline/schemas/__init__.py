"""
Schemas package - Data schema definitions.
"""

from pipeline.schemas.green_taxi import GREEN_TAXI_DTYPES, GREEN_TAXI_PARSE_DATES, GREEN_TAXI_URL
from pipeline.schemas.yellow_taxi import YELLOW_TAXI_DTYPES, YELLOW_TAXI_PARSE_DATES, YELLOW_TAXI_URL

__all__ = ["GREEN_TAXI_DTYPES", "GREEN_TAXI_PARSE_DATES", "GREEN_TAXI_URL", "YELLOW_TAXI_DTYPES", "YELLOW_TAXI_PARSE_DATES", "YELLOW_TAXI_URL"]
