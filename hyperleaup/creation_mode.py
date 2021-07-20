from enum import Enum


class CreationMode(Enum):
    INSERT = "INSERT"
    COPY = "COPY"
    PARQUET = "PARQUET"
