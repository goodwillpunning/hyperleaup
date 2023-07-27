import os
import logging
from shutil import copyfile
from typing import List, Any
from hyperleaup.creation_mode import CreationMode
from hyperleaup.hyper_config import HyperFileConfig
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col
from tableauhyperapi import SqlType, TableDefinition, NULLABLE, NOT_NULLABLE, TableName, HyperProcess, Telemetry, \
    Inserter, Connection, CreateMode
from pathlib import Path


def clean_dataframe(df: DataFrame, allow_nulls=False, convert_decimal_precision=False) -> DataFrame:
    """Replaces null or NaN values with '' and 0s"""
    schema = df.schema
    integer_cols = []
    long_cols = []
    double_cols = []
    float_cols = []
    string_cols = []

    if allow_nulls == False:
        for field in schema:
            if field.dataType == IntegerType():
                integer_cols.append(field.name)
            elif field.dataType == LongType():
                long_cols.append(field.name)
            elif field.dataType == DoubleType():
                double_cols.append(field.name)
            elif field.dataType == FloatType():
                float_cols.append(field.name)
            elif field.dataType == StringType():
                string_cols.append(field.name)

        # Replace null and NaN values with 0
        if len(integer_cols) > 0:
            df = df.na.fill(0, integer_cols)
        elif len(long_cols) > 0:
            df = df.na.fill(0, long_cols)
        elif len(double_cols) > 0:
            df = df.na.fill(0.0, double_cols)
        elif len(float_cols) > 0:
            df = df.na.fill(0.0, float_cols)
        elif len(string_cols) > 0:
            df = df.na.fill('', string_cols)
        
    if convert_decimal_precision == True:
        for field in schema:
            if str(field.dataType).startswith("DecimalType") and field.dataType.precision > 18:
                scale = field.dataType.scale
                if scale >= 18:
                    raise ValueError("Decimal with scale >= 18 cannot be supported by HyperFile.")
                logging.warn(f"Converting {field.name} to lower precision -> (18, {scale}). This may reduce actual precision of values in this column.")
                df = df.withColumn(field.name, col(field.name).cast(DecimalType(18, scale))) 

    return df


def get_rows(df: DataFrame) -> List[Any]:
    """Returns an array of rows given a Spark DataFrame"""
    return df.rdd.map(lambda row: [x for x in row]).collect()


def convert_struct_field(column: StructField, timestamp_with_timezone: bool = False) -> TableDefinition.Column:
    """Converts a Spark StructField to a Tableau Hyper SqlType"""
    if column.dataType == IntegerType():
        sql_type = SqlType.int()
    elif column.dataType == LongType():
        sql_type = SqlType.big_int()
    elif column.dataType == ShortType():
        sql_type = SqlType.small_int()
    elif column.dataType == DoubleType():
        sql_type = SqlType.double()
    elif column.dataType == FloatType():
        sql_type = SqlType.double()
    elif column.dataType == BooleanType():
        sql_type = SqlType.bool()
    elif column.dataType == DateType():
        sql_type = SqlType.date()
    elif column.dataType == TimestampType():
        if timestamp_with_timezone:
          sql_type = SqlType.timestamp_tz()
        else:
          sql_type = SqlType.timestamp()
    elif column.dataType == StringType():
        sql_type = SqlType.text()
    else:
        # Trap the DecimalType case
        if str(column.dataType).startswith("DecimalType"):
            # Max precision is only up to 18 decimal places in Tableau Hyper API
            precision = column.dataType.precision if column.dataType.precision <= 18 else 18
            scale = column.dataType.scale
            sql_type = SqlType.numeric(precision, scale)
        else:
            raise ValueError(f'Invalid StructField datatype for column `{column.name}` : {column.dataType}')
    nullable = NULLABLE if column.nullable else NOT_NULLABLE
    return TableDefinition.Column(name=column.name, type=sql_type, nullability=nullable)


def get_table_def(df: DataFrame, schema_name: str, table_name: str, timestamp_with_timezone: bool = False) -> TableDefinition:
    """Returns a Tableau TableDefintion given a Spark DataFrame"""
    schema = df.schema
    cols = [convert_struct_field(col, timestamp_with_timezone) for col in schema]
    return TableDefinition(
        table_name=TableName("Extract", "Extract"),
        columns=cols
    )


def insert_data_into_hyper_file(data: List[Any], name: str, table_def: TableDefinition):
    """Helper function that inserts data into a .hyper file."""
    # first, create a temp directory on the driver node
    tmp_dir = f"/tmp/hyperleaup/{name}/"
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    hyper_database_path = f"/tmp/hyperleaup/{name}/{name}.hyper"
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
        with Connection(endpoint=hp.endpoint,
                        database=hyper_database_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema(schema=table_def.table_name.schema_name)
            connection.catalog.create_table(table_definition=table_def)
            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(rows=data)
                inserter.execute()

    return hyper_database_path


def copy_data_into_hyper_file(csv_path: str, name: str, table_def: TableDefinition) -> str:
    """Helper function that copies data from a CSV file to a .hyper file."""
    hyper_database_path = f"/tmp/hyperleaup/{name}/{name}.hyper"
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
        with Connection(endpoint=hp.endpoint,
                        database=Path(hyper_database_path),
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_schema(schema=table_def.table_name.schema_name)
            connection.catalog.create_table(table_definition=table_def)

            # The most efficient method for adding data to a table is with the COPY command
            copy_command = f"COPY \"Extract\".\"Extract\" from '{csv_path}' with (format csv, NULL 'null', delimiter ',', header)"
            count = connection.execute_command(copy_command)
            logging.info(f"Copied {count} rows.")

    return hyper_database_path


def copy_parquet_to_hyper_file(parquet_path: str, name: str, table_def: TableDefinition) -> str:
    """Helper function that copies data from a Parquet file to a .hyper file."""
    hyper_database_path = f"/tmp/hyperleaup/{name}/{name}.hyper"
  
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
        with Connection(endpoint=hp.endpoint,
                        database=Path(hyper_database_path),
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_schema(schema=table_def.table_name.schema_name)
            connection.catalog.create_table(table_definition=table_def)

            # The most efficient method for adding data to a table is with the COPY command
            copy_command = f"COPY \"Extract\".\"Extract\" from '{parquet_path}' with (format parquet)"
            count = connection.execute_command(copy_command)
            logging.info(f"Copied {count} rows.")

    return hyper_database_path


def write_csv_to_local_file_system(df: DataFrame, name: str, allow_nulls: bool = False, convert_decimal_precision = False) -> str:
    """Writes a Spark DataFrame to a single CSV file on the local filesystem."""
    tmp_dir = f"/tmp/hyperleaup/{name}/"
    
    cleaned_df = clean_dataframe(df, allow_nulls, convert_decimal_precision) 

    # write the DataFrame to local disk as a single CSV file
    cleaned_df.coalesce(1).write \
        .option("delimiter", ",") \
        .option("header", "true") \
        .mode("overwrite").csv(tmp_dir)

    # Spark DataFrameWriter will write metadata alongside the CSV,
    # ignore metedata and return only the CSV filename
    for root_dir, dirs, files in os.walk(tmp_dir):
        for file in files:
            if file.endswith(".csv"):
                return f"{tmp_dir}/{file}"


def write_csv_to_dbfs(df: DataFrame, name: str, allow_nulls: bool = False, convert_decimal_precision = False) -> str:
    """Moves a CSV written to a Databricks Filesystem to a temp directory on the driver node."""
    tmp_dir = f"/tmp/hyperleaup/{name}/"

    cleaned_df = clean_dataframe(df, allow_nulls, convert_decimal_precision) 

    # write the DataFrame to DBFS as a single CSV file
    cleaned_df.coalesce(1).write \
        .option("delimiter", ",") \
        .option("header", "true") \
        .mode("overwrite").csv(tmp_dir)

    # Spark DataFrameWriter will write metadata alongside the CSV,
    # ignore metedata and return only the CSV filename
    dbfs_tmp_dir = "/dbfs" + tmp_dir
    csv_file = None
    for root_dir, dirs, files in os.walk(dbfs_tmp_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_file = file

    if csv_file is None:
        raise FileNotFoundError(f"CSV file '{tmp_dir}' not found on DBFS.")

    # Copy CSV from DBFS location to temp dir on driver node
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    src_path = dbfs_tmp_dir + csv_file
    dest_path = tmp_dir + csv_file
    copyfile(src_path, dest_path)

    return dest_path


def write_parquet_to_local_file_system(df: DataFrame, name: str, allow_nulls: bool = False,
                                       convert_decimal_precision = False) -> str:
    """Writes a Spark DataFrame to a single Parquet file on the local filesystem."""
    tmp_dir = f"/tmp/hyperleaup/{name}/"

    cleaned_df = clean_dataframe(df, allow_nulls, convert_decimal_precision) 

    # write the DataFrame to local disk as a single CSV file
    cleaned_df.coalesce(1).write \
        .option("delimiter", ",") \
        .option("header", "true") \
        .mode("overwrite").parquet(tmp_dir)

    for root_dir, dirs, files in os.walk(tmp_dir):
        for file in files:
            if file.endswith(".parquet"):
                return f"{tmp_dir}/{file}"


def write_parquet_to_dbfs(df: DataFrame, name: str, allow_nulls = False, convert_decimal_precision = False) -> str:
    """Moves a Parquet file written to a Databricks Filesystem to a temp directory on the driver node."""
    tmp_dir = f"/tmp/hyperleaup/{name}/"

    cleaned_df = clean_dataframe(df, allow_nulls, convert_decimal_precision) 
    
    # write the DataFrame to DBFS as a single Parquet file
    cleaned_df.coalesce(1).write \
        .option("delimiter", ",") \
        .option("header", "true") \
        .mode("overwrite").parquet(tmp_dir)

    dbfs_tmp_dir = "/dbfs" + tmp_dir
    parquet_file = None
    for root_dir, dirs, files in os.walk(dbfs_tmp_dir):
        for file in files:
            if file.endswith(".parquet"):
                parquet_file = file

    if parquet_file is None:
        raise FileNotFoundError(f"Parquet file '{tmp_dir}' not found on DBFS.")

    # Copy Parquet file from DBFS location to temp dir on driver node
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    src_path = dbfs_tmp_dir + parquet_file
    dest_path = tmp_dir + parquet_file
    copyfile(src_path, dest_path)

    return dest_path


class Creator:

    def __init__(self, df: DataFrame, name: str,
                 is_dbfs_enabled: bool = False,
                 creation_mode: str = CreationMode.COPY.value,
                 null_values_replacement = None,
                 config: HyperFileConfig = HyperFileConfig()):
        if null_values_replacement is None:
            null_values_replacement = {}
        self.df = df
        self.name = name
        self.is_dbfs_enabled = is_dbfs_enabled
        self.creation_mode = creation_mode
        self.null_values_replacement = null_values_replacement
        self.config = config

    def create(self) -> str:
        """Creates a Tableau Hyper File given a SQL statement"""
        if self.creation_mode.upper() == CreationMode.COPY.value:

            # Write Spark DataFrame to CSV so that a file COPY can be done
            if not self.is_dbfs_enabled:
                logging.info("Writing Spark DataFrame to local disk...")
                csv_path = write_csv_to_local_file_system(self.df, self.name, self.config.allow_nulls, 
                                                          self.config.convert_decimal_precision)
            else:
                logging.info("Writing Spark DataFrame to DBFS...")
                csv_path = write_csv_to_dbfs(self.df, self.name, self.config.allow_nulls, 
                                             self.config.convert_decimal_precision)

            # Convert the Spark DataFrame schema to a Tableau `TableDefinition`
            logging.info("Generating Tableau Table Definition...")
            table_def = get_table_def(self.df, "Extract", "Extract", self.config.timestamp_with_timezone)

            # COPY data into a Tableau .hyper file
            logging.info("Copying data into Hyper File...")
            database_path = copy_data_into_hyper_file(csv_path, self.name, table_def)

        elif self.creation_mode.upper() == CreationMode.INSERT.value:

            # Collect the DataFrame rows into the Driver
            logging.info("Collecting rows back to Driver...")
            data = get_rows(self.df)

            # Convert the Spark DataFrame schema to a Tableau `TableDefinition`
            logging.info("Converting Spark DataFrame schema to Tableau Table Definition...")
            table_def = get_table_def(self.df, "Extract", "Extract", self.config.timestamp_with_timezone)

            # Insert data into a Tableau .hyper file
            logging.info("Inserting data into Hyper File...")
            database_path = insert_data_into_hyper_file(data, self.name, table_def)

        elif self.creation_mode.upper() == CreationMode.PARQUET.value:

            # Write Spark DataFrame to Parquet so that a file COPY can be done
            if not self.is_dbfs_enabled:
                logging.info("Writing Spark DataFrame to local disk...")
                parquet_path = write_parquet_to_local_file_system(self.df, self.name, self.config.allow_nulls,
                                                                  self.config.convert_decimal_precision)
            else:
                logging.info("Writing Spark DataFrame to DBFS...")
                parquet_path = write_parquet_to_dbfs(self.df, self.name, self.config.allow_nulls, 
                                                     self.config.convert_decimal_precision)

            # Convert the Spark DataFrame schema to a Tableau `TableDefinition`
            logging.info("Generating Tableau Table Definition...")
            table_def = get_table_def(self.df, "Extract", "Extract", self.config.timestamp_with_timezone)
            
            # COPY data into a Tableau .hyper file
            logging.info("Copying data into Hyper File...")
            database_path = copy_parquet_to_hyper_file(parquet_path, self.name, table_def)

        else:
            raise ValueError(f'Invalid "creation_mode" specified: {self.creation_mode}')

        logging.info("Hyper File successfully created!")

        return database_path
