"""QueryStore
"""
from abc import ABC, abstractmethod
import pandas as pd


class QueryStore(ABC):
    """An interface for Query Store objects"""

    @abstractmethod
    def store_query_result(self, table_name: str, table: pd.DataFrame):
        """Stores The result of a query

        Args:
            table_name (str): The name of the table the result will be stored as
            table (pd.DataFrame): The query result
        """
