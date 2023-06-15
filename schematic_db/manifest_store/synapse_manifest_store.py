"""
SynapseManifestStore implements the ManifestStore interface.
It interacts with Synapse through the Python client.
This is used to interact with manifests
"""
from typing import Optional
import pandas
from schematic_db.schema_graph.schema_graph import SchemaGraph
from schematic_db.api_utils.api_utils import ManifestMetadataList
from schematic_db.synapse.synapse import Synapse, SynapseConfig
from .manifest_store import ManifestStore


class SynapseManifestStore(ManifestStore):
    """An interface for interacting with manifests"""

    def __init__(
        self, schema_url: str, asset_view_id: str, synapse_config: SynapseConfig
    ) -> None:
        self.asset_view_id = asset_view_id
        self.synapse = Synapse(synapse_config)
        self.schema_graph = SchemaGraph(schema_url)
        self.manifest_metadata: Optional[ManifestMetadataList] = None

    def create_sorted_table_name_list(self) -> list[str]:
        """
        Creates a table name list such tables always come after ones they
         depend on.
        This order is how tables in a database should be built and/or updated.

        Returns:
            list[str]: A list of tables names
        """
        return self.schema_graph.create_sorted_table_name_list()

    def get_manifest_metadata(self) -> ManifestMetadataList:
        """Gets the current objects manifest metadata."""
        dataframe = self.synapse.execute_sql_query(
            f"SELECT id, name, parentId, Component FROM {self.asset_view_id} where type = 'file'"
        )
        manifest_list = []
        for _, row in dataframe.iterrows():
            print(row["Name"], row["Age"])
            manifest_list.append(
                {
                    "dataset_id": row["parentId"],
                    "dataset_name": "none",
                    "manifest_id": row["id"],
                    "manifest_name": row["name"],
                    "component_name": row["Component"],
                }
            )
        return ManifestMetadataList(manifest_list)

    def get_manifest_ids(self, name: str) -> list[str]:
        """Gets the manifest ids for a table(component)

        Args:
            name (str): The name of the table

        Returns:
            list[str]: The manifest ids for the table
        """
        return self.get_manifest_metadata().get_manifest_ids_for_component(name)

    def download_manifest(self, manifest_id: str) -> pandas.DataFrame:
        """Downloads the manifest

        Args:
            manifest_id (str): The synapse id of the manifest

        Returns:
            pandas.DataFrame: The manifest in dataframe form
        """
        return self.synapse.download_csv_as_dataframe(manifest_id)
