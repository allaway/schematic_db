"""RDBUpdater"""
import warnings
from schematic_db.rdb.rdb import RelationalDatabase
from schematic_db.manifest_store.manifest_store import ManifestStore


class NoManifestWarning(Warning):
    """Raised when trying to update a database table there are no manifests"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class RDBUpdater:
    """An for updating a database."""

    def __init__(self, rdb: RelationalDatabase, manifest_store: ManifestStore) -> None:
        self.rdb = rdb
        self.manifest_store = manifest_store

    def update_database(self) -> None:
        """
        Updates all tables in the database.
        """
        table_names = self.manifest_store.create_sorted_object_name_list()
        for name in table_names:
            self.update_database_table(name)

    def update_database_table(self, table_name: str) -> None:
        """
        Updates a table in the database based on one or more manifests.
        If any of the manifests don't exist a warning will be raised.

        Args:
            table_name (str): The name of the table to be updated
        """
        dataset_ids = self.manifest_store.get_dataset_ids(table_name)

        # If there are no manifests a warning is raised and breaks out of function.
        if len(dataset_ids) == 0:
            msg = f"There were no manifests found for table: {table_name}"
            warnings.warn(NoManifestWarning(msg))
            return

        for dataset_id in dataset_ids:
            self.upsert_table_with_dataset_id(table_name, dataset_id)

    def upsert_table_with_dataset_id(self, table_name: str, dataset_id: str) -> None:
        """Updates a table int he database with a manifest

        Args:
            table_name (str): The name of the table
            dataset_id (str): The id of the dataset
        """
        config = self.rdb.get_table_config(table_name)
        manifest_table = self.manifest_store.get_manifest(dataset_id)

        # normalize table
        table_columns = set(config.get_attribute_names())
        manifest_columns = set(manifest_table.columns)
        columns = list(table_columns.intersection(manifest_columns))
        manifest_table = manifest_table[columns]
        manifest_table = manifest_table.drop_duplicates(subset=config.primary_key)
        manifest_table.reset_index(inplace=True, drop=True)

        self.rdb.upsert_table_rows(table_name, manifest_table)
