import os
import logging
from shutil import copyfile

from pyspark.sql import DataFrame
from tableauhyperapi import HyperProcess, TableName, Telemetry, Connection, CreateMode, Inserter
from hyperleaup.hyper_config import HyperFileConfig
from hyperleaup.creation_mode import CreationMode
from hyperleaup.creator import Creator
from hyperleaup.hyper_utils import HyperUtils
from hyperleaup.publisher import Publisher
from hyperleaup.spark_fixture import get_spark_session


def get_spark_dataframe(sql) -> DataFrame:
    return get_spark_session().sql(sql)


class HyperFile:

    def __init__(self, name: str,
                 sql: str = None, df: DataFrame = None,
                 is_dbfs_enabled: bool = False,
                 creation_mode: str = CreationMode.PARQUET.value,
                 null_values_replacement: dict = None,
                 config: HyperFileConfig = HyperFileConfig()):
        self.name = name
        # Create a DataFrame from Spark SQL
        if sql is not None and df is None:
            self.sql = sql
            self.df = get_spark_dataframe(sql)
        elif sql is None and df is not None:
            self.df = df
        self.creation_mode = creation_mode
        self.is_dbfs_enabled = is_dbfs_enabled
        self.null_values_replacement = null_values_replacement
        self.config = config
        # Do not create a Hyper File if loading an existing Hyper File
        if sql is None and df is None:
            self.path = None
        else:
            self.path = Creator(self.df,
                                self.name,
                                self.is_dbfs_enabled,
                                self.creation_mode,
                                self.null_values_replacement,
                                self.config).create()
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
                username: str = None, password: str = None, token_name: str = None, token_value: str = None,
                site_id: str = "", project_name: str = "Default", datasource_name: str = "Hyperleaup_Extract") -> str:
        """Publishes a Hyper File to a Tableau Server"""
        logging.info("Publishing Hyper File...")
        publisher = Publisher(tableau_server_url, username, password, token_name, token_value,
                              site_id, project_name, datasource_name, self.path)
        self.luid = publisher.publish()
        logging.info(f"Hyper File published to Tableau Server with datasource LUID : {self.luid}")

        return self.luid

    def save(self, path: str) -> str:
        """Saves a Hyper File to a destination path"""
        # Guard against invalid paths
        if path.lower().startswith("s3"):
            raise ValueError('Invalid path. S3 locations are not accepted.')
        elif path.lower().startswith("file:/"):
            raise ValueError('Invalid path. "file:/" is not recognized.')
        elif path.lower().startswith('dbfs:/'):
            raise ValueError('Invalid path. "dbfs:/" is not recognized.')

        # Check if using a Databricks filesystem
        if self.is_dbfs_enabled:
            # Must prepend '/dbfs' if using a Databricks filesystem
            path = '/dbfs' + path

        # Check if the destination directories exists and create them if they dne
        # Otherwise, a 'FileNotFoundError' will be raised
        if not os.path.exists(path):
            logging.info(f'Destination path "{path}" does not exist. Creating new directories.')
            os.makedirs(path)

        # Finally, construct the fully-qualified destination path
        if path.endswith("/"):
            dest_path = f'{path[:-1]}/{self.name}.hyper'  # remove trailing forward-slash, if exists
        else:
            dest_path = f'{path}/{self.name}.hyper'

        logging.info(f'Saving Hyper File to new location: {dest_path}')

        return copyfile(self.path, dest_path)

    @staticmethod
    def load(path: str, is_dbfs_enabled: bool = False):
        """Loads a Hyper File from a source path to a temp dir"""
        # Guard against invalid paths
        if path.lower().startswith("s3"):
            raise ValueError('Invalid path. S3 locations are not accepted.')
        elif path.lower().startswith("file:/"):
            raise ValueError('Invalid path. "file:/" is not recognized.')
        elif path.lower().startswith('dbfs:/'):
            raise ValueError('Invalid path. "dbfs:/" is not recognized.')
        elif not path.lower().endswith('.hyper'):
            raise ValueError('Invalid path. Must specify a ".hyper" file to be loaded.')

        logging.info(f'Loading existing Hyper File from path: {path}')

        # Extract the Hyper File name from the filename
        base_name = os.path.basename(path)
        name = str(base_name.split('.')[0])
        logging.info(f'Existing Hyper File name is: {name}')

        # Check if using a Databricks filesystem
        if is_dbfs_enabled:
            logging.info('Copying Hyper File from DBFS to tmp directory on driver.')

            # Must prepend '/dbfs' if using a Databricks filesystem
            dbfs_path = '/dbfs' + path

            # Copy the Hyper File to a temp directory on the driver node
            tmp_dir_path = f'/tmp/hyperleaup/{name}/'
            dest_path = f'{tmp_dir_path}{name}.hyper'

            # Create temp directories if they dne
            if not os.path.exists(tmp_dir_path):
                logging.info(f'Destination path "{tmp_dir_path}" does not exist. Creating new directories.')
                os.makedirs(tmp_dir_path)

            copyfile(dbfs_path, dest_path)  # move from DBFS to tmp dir
            logging.info(f'Hyper File copied locally to: {dest_path}')
            hyper_file_path = dest_path

        else:
            hyper_file_path = path

        # Create a HyperFile object with existing Hyper File path
        hf = HyperFile(name=name, is_dbfs_enabled=is_dbfs_enabled)
        hf.path = hyper_file_path

        return hf

    def append(self, sql: str = None, df: DataFrame = None):
        """Appends new data to a Hyper File"""
        # First, must materialize the new data back to the driver node
        if sql is not None and df is None:
            self.sql = sql
            self.df = get_spark_dataframe(sql)
        elif sql is None and df is not None:
            self.df = df
        else:
            raise ValueError('Missing either SQL statement or Spark DataFrame as argument.')
        data = HyperUtils.get_rows(self.df)

        # Convert the Spark DataFrame schema to a Tableau Table Def
        table_def = HyperUtils.get_table_def(self.df, "Extract", "Extract")

        # Insert, the new data into Hyper File
        hyper_database_path = self.path
        logging.info(f'Inserting new data into Hyper database: {hyper_database_path}')
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint,
                            database=hyper_database_path,
                            create_mode=CreateMode.NONE) as connection:
                with Inserter(connection, table_def) as inserter:
                    inserter.add_rows(rows=data)
                    inserter.execute()
