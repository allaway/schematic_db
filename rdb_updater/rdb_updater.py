"""RDBUpdater"""
import pandas as pd
from db_object_config import DBObjectConfigList, DBObjectConfig
from rdb import RelationalDatabase
from manifest_store import ManifestStore
from .utils import normalize_table


class UpdateDatabaseError(Exception):
    """UpdateDatabaseError"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class RDBUpdater:
    """An for updating a database."""

    def __init__(self, rdb: RelationalDatabase, manifest_store: ManifestStore):
        self.manifest_store = manifest_store
        self.rdb = rdb

    def update_all_database_tables(self, table_configs: DBObjectConfigList):
        """
        Updates all tables in the list of table_configs

        Args:
            table_configs (DBObjectConfigList): A list of generic representations of each
                table as a DBObjectConfig object. The list must be in the correct order to
                update in regards to relationships.
        """
        for config in table_configs.configs:
            self.update_database_table(config)

    def update_database_table(self, table_config: DBObjectConfig):
        """
        Updates a table in the database based on one or more manifests.
        If any of the manifests don't exist an exception will be raised.
        If the table doesn't exist in the database it will be built with the table config.

        Args:
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        manifest_tables = [
            self.manifest_store.get_manifest_table(id)
            for id in table_config.manifest_ids
        ]
        manifest_table = pd.concat(manifest_tables)
        manifest_table = normalize_table(manifest_table, table_config)
        self.rdb.update_table(manifest_table, table_config)
