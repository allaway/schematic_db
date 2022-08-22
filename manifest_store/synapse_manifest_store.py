"""Synapse Manifest Store
"""
import pandas as pd
from db_object_config import DBObjectConfig
from synapse import Synapse
from .manifest_store import ManifestStore


class SynapseManifestStore(ManifestStore):
    """SynapseManifestStore
    - Represents a source of manifest tables in Synapse
    - An adaptor between Synapse class and ManifestStore ABC
    """

    def __init__(self, config_dict: dict):
        """Init
        Args:
            config_dict (dict): A dict with synapse specific fields
        """
        self.synapse = Synapse(config_dict)

    def get_manifest_table(
        self, manifest_name: str, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        return self.synapse.query_table(manifest_name, table_config)
