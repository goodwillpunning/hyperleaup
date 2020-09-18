from pyspark.sql.functions import current_date, current_timestamp, to_date
from tableauhyperapi import SqlType, NOT_NULLABLE, NULLABLE, TableDefinition, TableName
from tableauhyperapi import Name

from hyperleaup.creator import convert_struct_field, get_table_def, get_rows, insert_data_into_hyper_file, Creator, \
    write_csv_to_local_file_system
from pyspark.sql.types import *

from hyperleaup.spark_fixture import get_spark_session

from tests.test_utils import TestUtils


class TestCreator(object):

    def test_convert_struct_field(self):
        # ensure strings can be converted correctly
        first_name_col = StructField('first_name', StringType(), False)
        converted_col = convert_struct_field(first_name_col)
        assert(converted_col.name == Name('first_name'))
        assert(converted_col.nullability is NOT_NULLABLE)
        assert(converted_col.type == SqlType.text())

        # ensure dates can be converted correctly
        date_col = StructField('update_date', DateType(), True)
        converted_col = convert_struct_field(date_col)
        assert(converted_col.name == Name('update_date'))
        assert(converted_col.nullability is NULLABLE)
        assert(converted_col.type == SqlType.date())

        # ensure timestamps can be converted correctly
        timestamp_col = StructField('created_at', TimestampType(), False)
        converted_col = convert_struct_field(timestamp_col)
        assert(converted_col.name == Name('created_at'))
        assert(converted_col.nullability is NOT_NULLABLE)
        assert(converted_col.type == SqlType.timestamp())

    def test_get_table_def(self):
        data = [
            (1001, 1, "Jane", "Doe", "2000-05-01", 29.0, False),
            (1002, 2, "John", "Doe", "1988-05-03", 33.0, False),
            (2201, 3, "Elonzo", "Smith", "1990-05-03", 21.0, True),
            (None, None, None, None, None, None, None)  # Test Nulls
        ]
        df = get_spark_session()\
            .createDataFrame(data, ["id", "dept_id", "first_name", "last_name", "dob", "age", "is_temp"])\
            .createOrReplaceTempView("employees")
        df = get_spark_session().sql("select id, cast(dept_id as short), first_name, "
                                     "last_name, dob, age, is_temp from employees")
        table_def = get_table_def(df, "Extract", "Extract")

        # Ensure that the Table Name matches
        assert(table_def.table_name.name == Name("Extract"))

        # Ensure that the the TableDefinition column names match
        assert(table_def.get_column(0).name == Name("id"))
        assert(table_def.get_column(1).name == Name("dept_id"))
        assert(table_def.get_column(2).name == Name("first_name"))
        assert(table_def.get_column(3).name == Name("last_name"))
        assert(table_def.get_column(4).name == Name("dob"))
        assert(table_def.get_column(5).name == Name("age"))
        assert(table_def.get_column(6).name == Name("is_temp"))

        # Ensure that the column data types were converted correctly
        assert(table_def.get_column(0).type == SqlType.big_int())
        assert(table_def.get_column(1).type == SqlType.small_int())
        assert(table_def.get_column(2).type == SqlType.text())
        assert(table_def.get_column(3).type == SqlType.text())
        assert(table_def.get_column(4).type == SqlType.text())
        assert(table_def.get_column(5).type == SqlType.double())
        assert(table_def.get_column(6).type == SqlType.bool())

    def test_get_rows(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29.0, False),
            (1002, "John", "Doe", "1988-05-03", 33.0, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 21.0, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        rows = get_rows(df)
        expected_row = [1001, "Jane", "Doe", "2000-05-01", 29.0, False]
        assert(len(rows) == 3)
        assert(rows[0] == expected_row)

    def test_insert_data_into_hyper_file(self):
        data = [
            (1001, "Jane", "Doe"),
            (1002, "John", "Doe"),
            (2201, "Elonzo", "Smith")
        ]
        name = "output"
        table_def = TableDefinition(
            table_name=TableName("Extract", "Extract"),
            columns=[
                TableDefinition.Column(name=Name("id"), type=SqlType.big_int(), nullability=NULLABLE),
                TableDefinition.Column(name=Name("first_name"), type=SqlType.text(), nullability=NULLABLE),
                TableDefinition.Column(name=Name("last_name"), type=SqlType.text(), nullability=NULLABLE)
            ]
        )
        path = insert_data_into_hyper_file(data, name, table_def)
        print(f'Database Path : {path}')
        tables = TestUtils.get_tables("Extract", "/tmp/hyperleaup/output/output.hyper")
        assert(len(tables) == 1)
        num_rows = TestUtils.get_row_count("Extract", "Extract", "/tmp/hyperleaup/output/output.hyper")
        assert(num_rows == 3)

    def test_write_csv_to_local_file_system(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29.0, False),
            (1002, "John", "Doe", "1988-05-03", 33.0, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 21.0, True),
            (2202, "James", "Towdry", "1980-05-03", 45.0, False),
            (2235, "Susan", "Sanders", "1980-05-03", 43.0, True)

        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        csv_file = write_csv_to_local_file_system(df, "employees")
        assert(csv_file.startswith("/tmp/hyperleaup/employees/"))

    def test_create(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29.0, False),
            (1002, "John", "Doe", "1988-05-03", 33.0, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 21.0, True),
            (2202, None, None, "1980-05-03", 45.0, False),  # Add a few nulls
            (2235, "", "", "1980-05-03", 43.0, True)

        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])

        # Ensure that a Hyper file can be created with date and timestamp columns
        df.withColumn("hire_date", current_date())
        df.withColumn("last_updated", current_timestamp())

        creator = Creator(df, 'employees', False)
        hyper_file_path = creator.create()
        assert(hyper_file_path == "/tmp/hyperleaup/employees/employees.hyper")
        tables = TestUtils.get_tables("Extract", "/tmp/hyperleaup/employees/employees.hyper")
        assert(len(tables) == 1)
        num_rows = TestUtils.get_row_count("Extract", "Extract", "/tmp/hyperleaup/employees/employees.hyper")
        assert(num_rows == 5)

    def test_creation_mode(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29.0, False),
            (1002, "John", "Doe", "1988-05-03", 33.0, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 21.0, True),
            (2202, None, None, "1980-05-03", 45.0, False),  # Add a few nulls
            (2235, "", "", "1980-05-03", 43.0, True)

        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])

        # creation_mode using a str
        creator = Creator(df=df, name='employees', is_dbfs_enabled=False, creation_mode="Insert")
        hyper_file_path = creator.create()
        assert (hyper_file_path == "/tmp/hyperleaup/employees/employees.hyper")
        num_rows = TestUtils.get_row_count("Extract", "Extract", "/tmp/hyperleaup/employees/employees.hyper")
        assert (num_rows == 5)
