from pyspark.sql import SparkSession
from functools import lru_cache
import os


@lru_cache(maxsize=None)
def get_spark_session():
    if os.getenv("ENV") == "LOCAL":
        return SparkSession.builder\
            .master("local")\
            .appName("Hyperleaup")\
            .config("spark.sql.shuffle.partitions", "1")\
            .config("spark.driver.host", "localhost")\
            .getOrCreate()
    else:
        return SparkSession.builder\
            .appName("Hyperleaup")\
            .getOrCreate()
