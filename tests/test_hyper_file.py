import os
from hyperleaup import HyperFile
from hyperleaup.spark_fixture import get_spark_session
from tests.test_utils import TestUtils


class TestHyperFile(object):

    def test_print_rows(self):
        # Ensure that a HyperFile can be created from a Spark DataFrame
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        hf = HyperFile(name="employees", df=df)
        hf.print_rows()

        # Ensure that a HyperFile can be created from Spark SQL
        data = [
            (101, "IT"),
            (103, "Engineering"),
            (104, "Management"),
            (105, "HR")
        ]
        get_spark_session()\
            .createDataFrame(data, ["id", "department"])\
            .createOrReplaceGlobalTempView("departments")
        sql = "SELECT * FROM global_temp.departments"
        hf = HyperFile(name="employees", sql=sql)
        hf.print_rows()

    def test_print_table_definition(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        hf = HyperFile(name="employees", df=df)
        hf.print_table_def()

    def test_creation_mode(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
        hf = HyperFile(name="employees", df=df, is_dbfs_enabled=False, creation_mode="insert")
        assert(hf.path == "/tmp/hyperleaup/employees/employees.hyper")

    def test_save(self):
        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])
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
        hf.append(df=df)
        num_rows = TestUtils.get_row_count("Extract", "Extract", "/tmp/save/employees.hyper")
        assert(num_rows == 6)

    def test_hyper_process_parameters(self):
        data_path = "/tmp/process_parameters"

        log_dir = "/tmp/logs"
        log_file = f"{log_dir}/hyperd.log"
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)

        data = [
            (1001, "Jane", "Doe", "2000-05-01", 29, False),
            (1002, "John", "Doe", "1988-05-03", 29, False),
            (2201, "Elonzo", "Smith", "1990-05-03", 29, True)
        ]
        df = get_spark_session().createDataFrame(data, ["id", "first_name", "last_name", "dob", "age", "is_temp"])

        hyper_process_parameters = {"log_dir": log_dir}

        for mode in ["insert", "copy", "parquet"]:
            if os.path.exists(log_file):
                os.remove(log_file)

            HyperFile(name="employees", df=df, is_dbfs_enabled=False, creation_mode=mode,
                      hyper_process_parameters=hyper_process_parameters).save(data_path)

            # Make sure that the logs have been created in the non-standard location
            assert(os.path.exists(log_file))
            assert(os.path.isfile(log_file))
