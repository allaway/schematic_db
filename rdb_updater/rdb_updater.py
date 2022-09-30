"""RDBUpdater"""
import pandas as pd
from db_object_config import DBConfig, DBObjectConfig
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

    def __init__(
        self,
        rdb: RelationalDatabase,
        manifest_store: ManifestStore,
        db_config: DBConfig,
    ) -> None:
        self.manifest_store = manifest_store
        self.rdb = rdb
        self.db_config = db_config

    def update_all_database_tables(self) -> None:
        """Updates all tables in the db_config"""
        for config in self.db_config.configs:
            self.update_database_table(config)

    def update_database_table(self, table_config: DBObjectConfig) -> None:
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
