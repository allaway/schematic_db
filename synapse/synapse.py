"""Synapse
"""
from functools import partial
import synapseclient as sc
import pandas as pd
from db_object_config import DBObjectConfig, DBDatatype


SYNAPSE_DATATYPES = {
    DBDatatype.TEXT: partial(sc.Column, columnType="STRING", maximumSize=100),
    DBDatatype.DATE: partial(sc.Column, columnType="DATE"),
    DBDatatype.INT: partial(sc.Column, columnType="INTEGER"),
    DBDatatype.FLOAT: partial(sc.Column, columnType="DOUBLE"),
    DBDatatype.BOOLEAN: partial(sc.Column, columnType="BOOLEAN"),
}


PANDAS_DATATYPES = {DBDatatype.INT: "Int64", DBDatatype.BOOLEAN: "boolean"}


class Synapse:
    """Synapse
    - Represents:
      - A database stored as Synapse tables
      - A source of manifest tables in Synapse
      - A destination of queries in Synapse
    - Implements the RDBType interface.
    - Handles Synapse specific functionality.
    """

    def __init__(self, config_dict: dict):
        """Init
        obj = MySQL({
            username: "firstname.lastname@sagebase.org"
            auth_token: "xxx"
            project_id: "syn1"
        })

        Args:
            config_dict (dict): A dict with synapse specific fields
        """
        username = config_dict.get("username")
        auth_token = config_dict.get("auth_token")
        project_id = config_dict.get("project_id")

        syn = sc.Synapse()
        syn.login(username, authToken=auth_token)

        self.syn = syn
        self.project_id = project_id

    def get_table_names(self) -> list[str]:
        tables = self._get_tables()
        return [table["name"] for table in tables]

    def get_table_id_from_name(self, table_name: str) -> str:
        tables = self._get_tables()
        matching_tables = [table for table in tables if table["name"] == table_name]
        if len(matching_tables) == 0:
            raise ValueError(f"No matching tables with name {table_name}")
        if len(matching_tables) > 1:
            raise ValueError(f"Multiple matching tables with name {table_name}")
        return matching_tables[0]["id"]

    def get_table_name_from_id(self, table_id: str) -> str:
        tables = self._get_tables()
        return [table["name"] for table in tables if table["id"] == table_id][0]

    def execute_sql_statement(self, statement: str, include_row_data: bool = False):
        return self.syn.tableQuery(
            statement, includeRowIdAndRowVersion=include_row_data
        )

    def _get_tables(self) -> list[sc.Table]:
        project = self.syn.get(self.project_id)
        return list(self.syn.getChildren(project, includeTypes=["table"]))

    def build_table(self, table_name: str, table: pd.DataFrame):
        """Adds a table to the project based on the input table

        Args:
            table_name (str): The name fo the table
            table (pd.DataFrame): A dataframe of the table
        """
        project = self.syn.get(self.project_id)
        table = sc.table.build_table(table_name, project, table)
        self.syn.store(table)

    def query_table(
        self, table_name: str, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        table_id = self.get_table_id_from_name(table_name)
        query = f"SELECT * FROM {table_id}"
        table = self.execute_sql_query(query)
        for att in table_config.attributes:
            if att.datatype == DBDatatype.INT:
                table = table.astype({att.name: "Int64"})
            elif att.datatype == DBDatatype.DATE:
                table[att.name] = pd.to_datetime(table[att.name], unit="ms").dt.date
            elif att.datatype == DBDatatype.BOOLEAN:
                table = table.astype({att.name: "boolean"})
        return table

    def execute_sql_query(
        self, query: str, include_row_data: bool = False
    ) -> pd.DataFrame:
        result = self.execute_sql_statement(query, include_row_data)
        table = pd.read_csv(result.filepath)
        return table

    def add_table(self, table_name: str, table_config: DBObjectConfig):
        columns = []
        values = {}
        for att in table_config.attributes:
            column = self._create_synapse_column(att.name, att.datatype)
            columns.append(column)
            values[att.name] = []

        schema = sc.Schema(name=table_name, columns=columns, parent=self.project_id)
        table = sc.Table(schema, values)
        table = self.syn.store(table)

    def drop_table(self, table_name: str):
        synapse_id = self.get_table_id_from_name(table_name)
        self.syn.delete(synapse_id)

    def _create_synapse_column(self, name: str, datatype: str) -> sc.Column:
        func = SYNAPSE_DATATYPES.get(datatype)
        return func(name=name)
