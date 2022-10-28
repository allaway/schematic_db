"""SynapseDatabase"""
import pandas as pd
import synapseclient as sc  # type: ignore
from schematic_db.db_config import DBObjectConfig
from schematic_db.synapse import Synapse, SynapseConfig
from .rdb import RelationalDatabase


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
        if table_name not in table_names:
            self.synapse.build_table(table_name, data)
            return
        synapse_id = self.synapse.get_synapse_id_from_table_name(table_name)
        current_table = self.synapse.syn.get(synapse_id)
        current_columns = self.synapse.syn.getTableColumns(current_table)
        if len(list(current_columns)) == 0:
            new_columns = sc.as_table_columns(data)
            for col in new_columns:
                current_table.addColumn(col)
            self.synapse.syn.store(current_table)
            self.synapse.insert_table_rows(table_name, data)
            return

        self.synapse.upsert_table_rows(table_name, data, table_config)

    def drop_table(self, table_name: str) -> None:
        self.synapse.clear_table(table_name)

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        pass

    def get_table_names(self) -> list[str]:
        return self.synapse.get_table_names()
