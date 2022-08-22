"""Synapse Query Store
"""
import pandas as pd
from synapse import Synapse
from .query_store import QueryStore


class SynapseQueryStore(QueryStore):
    """SynapseQueryStore
    - Represents a place to put query results in Synapse
    - An adaptor between Synapse class and QueryStore ABC
    """

    def __init__(self, config_dict: dict):
        """Init
        Args:
            config_dict (dict): A dict with synapse specific fields
        """
        self.synapse = Synapse(config_dict)

    def store_query_result(self, table_name: str, query_result: pd.DataFrame):
        if table_name in self.synapse.get_table_names():
            self.synapse.drop_table(table_name)
        self.synapse.build_table(table_name, query_result)
