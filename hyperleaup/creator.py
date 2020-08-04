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


class Creator:

    def __init__(self, df: DataFrame, path: Path, is_dbfs_enabled: bool = False):
        self.df = df
        self.path = path
        self.is_dbfs_enabled = is_dbfs_enabled

    def write_dataframe_to_csv(self):
        path = "/tmp/output.hyper"
        self.df.coalesce(1).write.mode("overwrite").csv(path)
        return path

    def create(self) -> str:
        """Creates a Tableau Hyper File given a SQL statement"""
        # Execute the user SQL, reading into a Spark DataFrame
        print("Creating Spark DataFrame...")
        row_count = self.df.count()
        print(f"There are {row_count} records in the extract.")

        # Collect the DataFrame rows into the Driver
        print("Collecting rows back to Driver...")
        data = get_rows(self.df)
        print("Done.")

        # Convert the Spark DataFrame schema to a Tableau `TableDefinition`
        print("Converting Spark DataFrame schema to Tableau Table Definition...")
        table_def = get_table_def(self.df, "Extract", "Extract")
        print("Done.")

        # Insert data into a Tableau .hyper file
        print("Inserting data into Hyper File...")
        insert_data_into_hyper_file(data, Path(self.path), table_def)
        print("All done!")

        return str(self.path)
