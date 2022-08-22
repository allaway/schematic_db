"""RDB Queryer
"""
import pandas as pd
from rdb import RelationalDatabase
from query_store import QueryStore


class RDBQueryer:
    """Represents a relational database."""

    def __init__(
        self,
        rdb: RelationalDatabase,
        query_store: QueryStore,
    ):
        self.rdb = rdb
        self.query_store = query_store

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
        query_result = self.rdb.execute_sql_query(query)
        self.query_store.store_query_result(table_name, query_result)
