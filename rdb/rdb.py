"""RDB
"""
from typing import List
import pandas as pd
from db_object_config import DBObjectConfigList, DBObjectConfig
from rdb_type import RDBType, Synapse
from .utils import normalize_table


class RDB:
    """RDB
    Represents a relational database.
    """

    def __init__(
        self, rdb_type: RDBType, manifest_store: Synapse, query_result_store: Synapse
    ):
        self.manifest_store = manifest_store
        self.rdb_type = rdb_type
        self.query_result_store = query_result_store

    def update_all_database_tables(
        self, manifest_table_names: List[List[str]], table_configs: DBObjectConfigList
    ):
        """
        Updates all tables in the list of table_configs

        Args:
            manifest_table_names (List[List[str]]): A list where each item is a list of the
                names of tables in the manifest store
            table_configs (DBObjectConfigList): A list of generic representations of each
                table as a DBObjectConfig object. The list must be in the correct order to
                update in regards to relationships.
        """
        zipped_list = zip(manifest_table_names, table_configs.configs)
        for tup in zipped_list:
            self.update_database_table(*tup)

    def update_database_table(
        self, manifest_table_names: List[str], table_config: DBObjectConfig
    ):
        """
        Updates a table in the database based on one or more manifests.
        If any of the manifests don't exist an exception will be raised.
        If the table doesn't exist in the database it will be built with the table config.

        Args:
            manifest_table_names (List[str]): A list of the names of tables in the manifest store
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        manifest_store_table_names = self.manifest_store.get_table_names()
        manifest_tables = []
        for name in manifest_table_names:
            if name not in manifest_store_table_names:
                raise ValueError(
                    f"manifest_table_name: {name} missing from manifest store"
                )
            table = self.manifest_store.query_table(name, table_config)
            manifest_tables.append(table)
        manifest_table = pd.concat(manifest_tables)
        manifest_table = normalize_table(manifest_table, table_config)

        database_table_names = self.rdb_type.get_table_names()
        table_name = table_config.name
        if table_name not in database_table_names:
            self.rdb_type.add_table(table_name, table_config)
        self.rdb_type.upsert_table_rows(table_name, manifest_table)

    def store_query_results(self, csv_path: str):
        """Stores the results of queries
        Takes a csv file with two columns named "query" and "table_name", and runs each query,
        storing the result in the query_result_store as a table.

        Args:
            csv_path (str): A path to a csv file.
        """
        csv = pd.read_csv(csv_path)
        for _, row in csv.iterrows():
            self.store_query_result(row["query"], row["table_name"])

    def store_query_result(self, query: str, table_name: str):
        """Stores the result of a query

        Args:
            query (str): A query in SQL form
            table_name (str): The name of the table the result will be stored as
        """
        query_result = self.rdb_type.execute_sql_query(query)
        if table_name in self.query_result_store.get_table_names():
            self.query_result_store.drop_table(table_name)
        self.query_result_store.build_table(table_name, query_result)

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ):
        # pylint: disable=missing-function-docstring
        self.rdb_type.delete_table_rows(table_name, data, table_config)

    delete_table_rows.__doc__ = RDBType.drop_table.__doc__

    def drop_table(self, table_name: str):
        # pylint: disable=missing-function-docstring
        self.rdb_type.drop_table(table_name)

    drop_table.__doc__ = RDBType.drop_table.__doc__
