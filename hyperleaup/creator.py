import os
from shutil import copyfile
from typing import List, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from tableauhyperapi import SqlType, TableDefinition, NULLABLE, NOT_NULLABLE, TableName, HyperProcess, Telemetry, \
    CreateMode, Inserter, Connection
from pathlib import Path


def get_rows(df: DataFrame) -> List[Any]:
    """Returns an array of rows given a Spark DataFrame"""
    return df.rdd.map(lambda row: [x for x in row]).collect()


def convert_struct_field(column: StructField) -> TableDefinition.Column:
    """Converts a Spark StructField to a Tableau Hyper SqlType"""
    if column.dataType == IntegerType():
        sql_type = SqlType.int()
    elif column.dataType == LongType():
        sql_type = SqlType.big_int()
    elif column.dataType == DoubleType():
        sql_type = SqlType.double()
    elif column.dataType == FloatType():
        sql_type = SqlType.double()
    elif column.dataType == BooleanType():
        sql_type = SqlType.bool()
    elif column.dataType == DateType():
        sql_type = SqlType.date()
    elif column.dataType == TimestampType():
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


def get_table_def(df: DataFrame, schema_name: str, table_name: str) -> TableDefinition:
    """Returns a Tableau TableDefintion given a Spark DataFrame"""
    schema = df.schema
    cols = list(map(convert_struct_field, schema))
    return TableDefinition(
        table_name=TableName("Extract", "Extract"),
        columns=cols
    )


def insert_data_into_hyper_file(data: List[Any], path: Path, table_def: TableDefinition):
    """Helper function that inserts data into a .hyper file."""
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
        with Connection(endpoint=hp.endpoint,
                        database=path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema(schema=table_def.table_name.schema_name)
            connection.catalog.create_table(table_definition=table_def)
            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(rows=data)
                inserter.execute()


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
            copy_command = f"COPY {table_def.table_name} from '{csv_path}' with (format csv, NULL 'NULL', delimiter ',', header)"
            count = connection.execute_command(copy_command)
            print(f"Copied {count} rows.")

            return hyper_database_path


def write_csv_to_local_file_system(df: DataFrame, name: str) -> str:
    """Writes a Spark DataFrame to a single CSV file on the local filesystem."""
    tmp_dir = f"/tmp/hyperleaup/{name}/"

    # write the DataFrame to local disk as a single CSV file
    df.coalesce(1).write \
        .option("delimiter", ",") \
        .option("header", "true") \
        .option("inferSchema", "true")\
        .mode("overwrite").csv(tmp_dir)

    # Spark DataFrameWriter will write metadata alongside the CSV,
    # ignore metedata and return only the CSV filename
    for root_dir, dirs, files in os.walk(tmp_dir):
        for file in files:
            if file.endswith(".csv"):
                return f"{tmp_dir}/{file}"


def write_csv_to_dbfs(df: DataFrame, name: str) -> str:
    """Moves a CSV written to a Databricks Filesystem to a temp directory on the driver node."""
    tmp_dir = f"/tmp/hyperleaup/{name}/"

    # write the DataFrame to DBFS as a single CSV file
    df.coalesce(1).write \
        .option("delimiter", ",") \
        .option("header", "true") \
        .option("inferSchema", "true")\
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


class Creator:

    def __init__(self, df: DataFrame, name: str, is_dbfs_enabled: bool = False):
        self.df = df
        self.name = name
        self.is_dbfs_enabled = is_dbfs_enabled

    def create(self) -> str:
        """Creates a Tableau Hyper File given a SQL statement"""
        # Write Spark DataFrame to CSV so that a file COPY can be done
        if not self.is_dbfs_enabled:
            print("Writing Spark DataFrame to local disk...")
            csv_path = write_csv_to_local_file_system(self.df, self.name)
        else:
            print("Writing Spark DataFrame to DBFS...")
            csv_path = write_csv_to_dbfs(self.df, self.name)

        # Convert the Spark DataFrame schema to a Tableau `TableDefinition`
        print("Generating Tableau Table Definition...")
        table_def = get_table_def(self.df, "Extract", "Extract")
        print("Done.")

        # COPY data into a Tableau .hyper file
        print("Copying data into Hyper File...")
        database_path = copy_data_into_hyper_file(csv_path, self.name, table_def)
        print("All done!")

        return database_path
