from hyperleaup import HyperFile
from hyperleaup.spark_fixture import get_spark_session


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
