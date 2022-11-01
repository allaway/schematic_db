"""Synapse Query Store
"""
import pandas as pd
from schematic_db.synapse import Synapse, SynapseConfig
from .query_store import QueryStore


class SynapseQueryStore(QueryStore):  # pylint: disable=too-few-public-methods
    """SynapseQueryStore
    - Represents a place to put query results in Synapse
    - An adaptor between Synapse class and QueryStore ABC
    """

    def __init__(self, config: SynapseConfig):
        """Init
        Args:
            config (SynapseConfig): A SynapseConfig object
        """
        self.synapse = Synapse(config)

    def store_query_result(self, table_name: str, query_result: pd.DataFrame) -> None:
        self.synapse.replace_table(table_name, query_result)
