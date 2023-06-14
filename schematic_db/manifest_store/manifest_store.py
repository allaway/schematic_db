"""
ManifestStore is an abstract base class that implements an interface.
The interface is used to interact with manifests
"""
from abc import ABC, abstractmethod
import pandas
from schematic_db.api_utils.api_utils import ManifestMetadataList


class ManifestStore(ABC):
    """An interface for interacting with manifests"""

    @abstractmethod
    def create_sorted_table_name_list(self) -> list[str]:
        """
        Creates a table name list such tables always come after ones they
         depend on.
        This order is how tables in a database should be built and/or updated.

        Returns:
            list[str]: A list of tables names
        """

    @abstractmethod
    def get_manifest_metadata(self) -> ManifestMetadataList:
        """Gets the current objects manifest metadata."""

    @abstractmethod
    def get_manifest_ids(self, name: str) -> list[str]:
        """Gets the manifest ids for a table(component)

        Args:
            name (str): The name of the table

        Returns:
            list[str]: The manifest ids for the table
        """

    @abstractmethod
    def download_manifest(self, manifest_id: str) -> pandas.DataFrame:
        """Downloads the manifest

        Args:
            manifest_id (str): The synapse id of the manifest

        Returns:
            pandas.DataFrame: The manifest in dataframe form
        """
