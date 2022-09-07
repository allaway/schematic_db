"""ManifestStore
"""
from abc import ABC, abstractmethod
import pandas as pd


class ManifestStore(ABC):
    """An interface for Manifest Store objects"""

    @abstractmethod
    def get_manifest_table(self, manifest_name: str) -> pd.DataFrame:
        """Gets a manifest in table form

        Args:
            manifest_name (str): The name of the manifest in the store

        Returns:
            pd.DataFrame: The  manifest in table form
        """
