from typing import List, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from tableauhyperapi import TableDefinition, SqlType, NULLABLE, NOT_NULLABLE, TableName


class HyperUtils:

    @staticmethod
    def get_rows(df: DataFrame) -> List[Any]:
        """Returns an array of rows given a Spark DataFrame"""
        return df.rdd.map(lambda row: [x for x in row]).collect()

    @staticmethod
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

    @staticmethod
    def get_table_def(df: DataFrame, schema_name: str = 'Extract', table_name: str = 'Extract') -> TableDefinition:
        """Returns a Tableau TableDefintion given a Spark DataFrame"""
        schema = df.schema
        cols = list(map(HyperUtils.convert_struct_field, schema))
        return TableDefinition(
            table_name=TableName(schema_name, table_name),
            columns=cols
        )
