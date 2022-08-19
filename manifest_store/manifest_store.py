"""ManifestStore
"""
from abc import ABC, abstractmethod
import pandas as pd
from db_object_config import DBObjectConfig


class ManifestStore(ABC):
    """An interface for Manifest Store objects"""

    @abstractmethod
    def get_manifest_table(
        self, manifest_name: str, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        """Gets a manifest in table form

        Args:
            manifest_name (str): The name of the manifest in the store
            table_config (DBObjectConfig): A generic representation of the table
                the manifest will be in

        Returns:
            pd.DataFrame: The  manifest in table form
        """
