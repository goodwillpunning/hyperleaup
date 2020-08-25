from tableauhyperapi import HyperProcess, Connection, Telemetry, SchemaName, TableName


class TestUtils:

    @staticmethod
    def get_tables(schema: str, hyper_file_path: str):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint, database=hyper_file_path) as connection:
                catalog = connection.catalog
                # Query the Catalog API for all tables under the given schema
                return catalog.get_table_names(SchemaName(schema))

    @staticmethod
    def get_row_count(schema: str, table: str, hyper_file_path: str):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint, database=hyper_file_path) as connection:
                # Query the Hyper File for the number of rows in the table
                return connection.execute_scalar_query(f"SELECT COUNT(*) FROM {TableName(schema, table)}")
