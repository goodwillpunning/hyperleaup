from pyspark.sql import SparkSession
from functools import lru_cache


@lru_cache(maxsize=None)
def get_spark_session():
    return SparkSession.builder\
        .master("local")\
        .appName("Hyperleaup")\
        .config("spark.sql.shuffle.partitions", "1")\
        .config("spark.driver.host", "localhost")\
        .getOrCreate()
