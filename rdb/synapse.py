"""Synapse
"""
import pandas as pd
from db_object_config import DBObjectConfig
from synapse import Synapse
from .rdb import RelationalDatabase


class SynapseDatabase(RelationalDatabase):
    """Synapse
    - Represents:
      - A database stored as Synapse tables
      - A source of manifest tables in Synapse
      - A destination of queries in Synapse
    - Implements the RDBType interface.
    - Handles Synapse specific functionality.
    """

    def __init__(self, config_dict: dict):
        """Init
        Args:
            config_dict (dict): A dict with synapse specific fields
        """
        self.synapse = Synapse(config_dict)

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        return self.synapse.execute_sql_query(query, include_row_data)

    def update_table(self, data: pd.DataFrame, table_config: DBObjectConfig):
        table_names = self.synapse.get_table_names()
        table_name = table_config.name
        if table_name not in table_names:
            self.synapse.add_table(table_name, table_config)
        # self.upsert_table_rows(table_name, data)

    def drop_table(self, table_name: str):
        self.synapse.drop_table(table_name)

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ):
        pass
