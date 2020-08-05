from pyspark.sql import DataFrame
from tableauhyperapi import HyperProcess, Telemetry, Connection

from hyperleaup.creator import Creator
from hyperleaup.publisher import Publisher
from tableauhyperapi import TableName
from hyperleaup.spark_fixture import get_spark_session


def get_spark_dataframe(sql):
    return get_spark_session().sql(sql)


class HyperFile:

    def __init__(self, name: str,
                 sql: str = None, df: DataFrame = None,
                 is_dbfs_enabled: bool = False):
        self.name = name
        if sql is not None and df is None:
            self.sql = sql
            self.df = get_spark_dataframe(sql)
        elif sql is None and df is not None:
            self.df = df
        else:
            raise Exception("Hyper file must have SQL as an argument.")
        self.is_dbfs_enabled = is_dbfs_enabled
        self.path = Creator(self.df, self.name, self.is_dbfs_enabled).create()
        self.luid = None

    def print_rows(self):
        """Prints the first 1,000 rows of a Hyper file"""
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint, database=self.path) as connection:
                rows = connection.execute_list_query(f"SELECT * FROM {TableName('Extract', 'Extract')} LIMIT 1000")
                print("Showing first 1,000 rows")
                for row in rows:
                    print(row)

    def print_table_def(self, schema: str = "Extract", table: str = "Extract"):
        """Prints the table definition for a table in a Hyper file."""
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint, database=self.path) as connection:
                table_name = TableName(schema, table)
                table_definition = connection.catalog.get_table_definition(name=table_name)
                # Print all column information
                print("root")
                for column in table_definition.columns:
                    print(f"|-- {column.name}: {column.type} (nullable = {column.nullability})")

    def publish(self, tableau_server_url: str,
                username: str, password: str, site_id: str = "",
                project_name: str = "Default", datasource_name: str = "Hyperleaup_Extract"):
        """Publishes a Hyper File to a Tableau Server"""
        print("Publishing Hyper File...")
        publisher = Publisher(tableau_server_url, username, password,
                              site_id, project_name, datasource_name, self.path)
        self.luid = publisher.publish()
        print(f"Hyper File published to Tableau Server with datasource LUID : {self.luid}")
