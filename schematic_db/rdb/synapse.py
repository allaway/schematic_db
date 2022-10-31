"""SynapseDatabase"""
import pandas as pd
from schematic_db.db_config import DBObjectConfig, ForeignKey
from schematic_db.synapse import Synapse, SynapseConfig
from .rdb import RelationalDatabase


def create_foreign_key_annotation_string(key: ForeignKey) -> str:
    """Creates a string that will serve as a foreign key Synapse annotation

    Args:
        key (ForeignKey): The foreign key that to be turned into a string

    Returns:
        str: The foreign key in string form.
    """
    return f"{key.name};{key.foreign_object_name};{key.foreign_attribute_name}"


class SynapseDatabase(RelationalDatabase):
    """Represents a database stored as Synapse tables"""

    def __init__(self, config: SynapseConfig):
        """Init
        Args:
            config (SynapseConfig): A SynapseConfig object
        """
        self.synapse = Synapse(config)

    def drop_all_tables(self) -> None:
        table_names = self.synapse.get_table_names()
        for name in table_names:
            self.drop_table(name)

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        return self.synapse.execute_sql_query(query, include_row_data)

    def update_table(self, data: pd.DataFrame, table_config: DBObjectConfig) -> None:
        table_names = self.synapse.get_table_names()
        table_name = table_config.name

        # table doesn't exist in Synapse, and must be built
        if table_name not in table_names:
            self.synapse.build_table(table_name, data)
            self.annotate_table(table_name, table_config)
            return

        # table exists but has no columns/rows, both must be added
        current_columns = self.synapse.get_table_column_names(table_name)
        if len(list(current_columns)) == 0:
            self.synapse.add_table_columns(table_name, data)
            self.synapse.insert_table_rows(table_name, data)
            self.annotate_table(table_name, table_config)
            return

        # table exists and possibly has data, upsert method must be used
        self.synapse.upsert_table_rows(table_name, data, table_config)

    def drop_table(self, table_name: str) -> None:
        synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
        self.synapse.delete_all_table_rows(synapse_id)
        self.synapse.delete_all_table_columns(synapse_id)
        self.synapse.clear_entity_annotations(synapse_id)

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        pass

    def get_table_names(self) -> list[str]:
        return self.synapse.get_table_names()

    def annotate_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Annotates the table with it's primary key and foreign keys

        Args:
            table_name (str): The name of the table to be annotated
            table_config (DBObjectConfig):The config for the table
        """
        synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
        annotations = {
            "primary_key": table_config.primary_key,
            "foreign_keys": [
                create_foreign_key_annotation_string(key)
                for key in table_config.foreign_keys
            ],
        }
        self.synapse.set_entity_annotations(synapse_id, annotations)
