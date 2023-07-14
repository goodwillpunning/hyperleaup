import os

from hyperleaup import HyperFile
from hyperleaup.spark_fixture import get_spark_session
from pyspark.sql.functions import current_timestamp

from tests.test_utils import TestUtils

class TestHyperFile(object):

    def test_print_rows(self, is_dbfs_enabled=False):
        # Ensure that a HyperFile can be created from a Spark DataFrame
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        df = df.withColumn("last_updated", current_timestamp())
        hf = HyperFile(name="employees", df=df, is_dbfs_enabled=is_dbfs_enabled, timestamp_with_timezone=True)
        hf.print_rows()

        # Ensure that a HyperFile can be created from Spark SQL
        data = [
            (101, "IT"),
            (103, "Engineering"),
            (104, "Management"),
            (105, "HR")
        ]
        df = get_spark_session()\
            .createDataFrame(data, ["id", "department"])\
            .withColumn("last_updated", current_timestamp())\
            .createOrReplaceGlobalTempView("departments")
        sql = "SELECT * FROM global_temp.departments"
        hf = HyperFile(name="employees", sql=sql, is_dbfs_enabled=is_dbfs_enabled, timestamp_with_timezone=True)
        hf.print_rows()

    def test_print_table_definition(self, is_dbfs_enabled=False):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        df = df.withColumn("last_updated", current_timestamp())
        hf = HyperFile(name="employees", df=df, is_dbfs_enabled=is_dbfs_enabled, timestamp_with_timezone=True)
        hf.print_table_def()

    def test_creation_mode(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        df = df.withColumn("last_updated", current_timestamp())
        hf = HyperFile(name="employees", df=df, is_dbfs_enabled=False, creation_mode="insert")
        assert(hf.path == "/tmp/hyperleaup/employees/employees.hyper")

    def test_save(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        df = df.withColumn("last_updated", current_timestamp())
        hf = HyperFile(name="employees", df=df, is_dbfs_enabled=False, creation_mode="insert")

        # Ensure that the Hyper File can be saved to an alternative location
        current_path = hf.path
        new_path = '/tmp/save/'
        expected_path = '/tmp/save/employees.hyper'
        hf.save(new_path)

        # Save operation should not update the current Hyper File's path
        assert(current_path == hf.path)
        assert(os.path.exists(expected_path))
        assert(os.path.isfile(expected_path))

    def test_load(self):
        # Ensure that existing Hyper Files can be loaded
        existing_hf_path = '/tmp/save/employees.hyper'
        assert(os.path.exists(existing_hf_path))
        assert(os.path.isfile(existing_hf_path))
        hf = HyperFile.load(path=existing_hf_path, is_dbfs_enabled=False)
        assert(hf.path == existing_hf_path)
        assert(hf.name == 'employees')

    def test_append(self):
        # Ensure that new data can be appended to an existing Hyper File
        existing_hf_path = '/tmp/save/employees.hyper'
        hf = HyperFile.load(path=existing_hf_path, is_dbfs_enabled=False)
        num_rows = TestUtils.get_row_count("Extract", "Extract", "/tmp/save/employees.hyper")
        assert(num_rows == 3)

        # Create new data
        data = [
            (3001, "Will", "Girten", "1990-05-01", 31, True),
            (3002, "Sammy", "Smith", "1988-05-03", 29, True),
            (3003, "Gregory", "Denver", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        df = df.withColumn("last_updated", current_timestamp())
        hf.append(df=df)
        num_rows = TestUtils.get_row_count("Extract", "Extract", "/tmp/save/employees.hyper")
        assert(num_rows == 6)
