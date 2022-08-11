"""RDB
"""
# pylint: disable=E0401
from yaml import safe_load
import pandas as pd # type: ignore
from db_object_config import DBObjectConfig
from rdb_type import MySQL, Synapse


class RDB():
    """RDB
    - Represents a relational database.
    """
    def __init__(self, config_yaml_path: str):
        with open(config_yaml_path, mode="rt", encoding="utf-8") as file:
            config_dict = safe_load(file)

        manifest_store_config = config_dict.get("manifest_store")
        manifest_store_type = manifest_store_config.get("type")
        if manifest_store_type == "synapse":
            manifest_store = Synapse(manifest_store_config)
        else:
            raise ValueError("manifest_store_type must be one of ['synapse']")

        database_config = config_dict.get("database")
        database_type = database_config.get("type")
        if database_type == "mysql":
            rdb_type = MySQL(database_config)
        elif database_type == "synapse":
            rdb_type = Synapse(database_config)
        else:
            raise ValueError("database_type must be one of ['mysql']")

        query_result_store_config = config_dict.get("query_result_store")
        query_result_store_type = query_result_store_config.get("type")
        if query_result_store_type == "synapse":
            query_result_store = Synapse(query_result_store_config)
        else:
            raise ValueError("query_result_store_type must be one of ['synapse']")

        self.manifest_store = manifest_store
        self.rdb_type = rdb_type
        self.query_result_store = query_result_store

    def update_database_table(self, manifest_table_name: str, table_config: DBObjectConfig):
        """Updates a table in the database based on the equivalent manifest
        If the table doesn't exist in the database it will be built with the table config.

        Args:
            manifest_table_name (str): The name of the table in the manifest store
            table_config (DBObjectConfig): A generic representation of the table as a
            DBObjectConfig object.
        """
        manifest_table_names = self.manifest_store.get_table_names()
        if manifest_table_name not in manifest_table_names:
            raise ValueError(
                f"manifest_table_name: {manifest_table_name} missing from manifest store"
            )
        manifest_table = self.manifest_store.query_table(manifest_table_name, table_config)

        database_table_names = self.rdb_type.get_table_names()
        table_name = table_config.name
        if table_name not in database_table_names:
            self.rdb_type.add_table(table_name, table_config)
        self.rdb_type.upsert_table_rows(table_name, manifest_table)

    def store_query_results(self, csv_path:str):
        """Stores the results of queries
        Takes a csv file with two columns named "query" and "table_name", and runs each query,
        storing the result in the query_result_store as a table.

        Args:
            csv_path (str): A path to a csv file.
        """
        csv = pd.read_csv(csv_path)
        for _, row in csv.iterrows():
            self.store_query_result(row['query'], row['table_name'])

    def store_query_result(self, query:str, table_name: str):
        """Stores the result of a query

        Args:
            query (str): A query in SQL form
            table_name (str): The name of the table the result will be stored as
        """
        query_result = self.rdb_type.execute_sql_query(query)
        if table_name in self.query_result_store.get_table_names():
            self.query_result_store.drop_table(table_name)
        self.query_result_store.build_table(table_name, query_result)
