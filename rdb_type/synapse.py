"""Synapse
"""
# pylint: disable=E0401
from typing import List, Dict
from yaml import safe_load
import synapseclient as sc # type: ignore
import pandas as pd # type: ignore
from db_object_config import DBObjectConfig, DBDatatype
from .rdb_type import RDBType

class Synapse(RDBType):
    """MYSQL
    - Represents:
      - A database stored as Synapse tables
      - A source of manifest tables in Synapse
      - A destination of queries in Synapse
    - Implements the RDBType interface.
    - Handles Synapse specific functionality.
    """
    def __init__(self, config_yaml_path: str):
        """Init
        obj = Synapse(config_yaml_path="tests/data/synapse_config.yml")

        The config_yaml should look like:
            username: "firstname.lastname@sagebase.org"
            auth_token: "xxx"
            project_id: "syn1"

        Args:
            config_yaml_path (str): The path to the config yaml
        """
        with open(config_yaml_path, mode="rt", encoding="utf-8") as file:
            config_dict = safe_load(file)
        username = config_dict.get("username")
        auth_token = config_dict.get("auth_token")
        project_id = config_dict.get("project_id")

        syn = sc.Synapse()
        syn.login(username, authToken=auth_token)
        project = syn.get(project_id)

        self.syn = syn
        self.project = project
        self.project_id = project_id

    def get_table_names(self) -> List[str]:
        tables = self._get_tables()
        return [table['name'] for table in tables]

    def get_column_names_from_table(self, table_id: str) -> List[str]:
        columns = self._get_columns_from_table(table_id)
        return [column['name'] for column in columns]

    def add_table(self, table_id: str, table_config: DBObjectConfig):
        columns = []
        values = {}
        for att in table_config.attributes:
            column = self._create_synapse_column(att.name, att.datatype)
            columns.append(column)
            values[att.name] = []

        schema = sc.Schema(name=table_id, columns=columns, parent=self.project_id)
        table = sc.Table(schema, values)
        table = self.syn.store(table)

    def drop_table(self, table_id: str):
        synapse_id = self._get_table_synapse_id(table_id)
        self.syn.delete(synapse_id)

    def add_table_column(self, table_id: str, column_name: str, datatype: DBDatatype):
        column = self._create_synapse_column(column_name, datatype)
        column = self.syn.store(column)
        synapse_id = self._get_table_synapse_id(table_id)
        table = self.syn.get(synapse_id)
        table.addColumn(column)
        table = self.syn.store(table)

    def drop_table_column(self, table_id: str, column_name: str):
        synapse_id = self._get_table_synapse_id(table_id)
        table = self.syn.get(synapse_id)
        columns = self._get_columns_from_table(table_id)
        for col in columns:
            if col.name == column_name:
                table.removeColumn(col)
        table = self.syn.store(table)

    def execute_sql_statement(self, statement: str, include_row_data: bool = True):
        return self.syn.tableQuery(statement, includeRowIdAndRowVersion = include_row_data)

    def execute_sql_query(self, query: str, include_row_data: bool = True):
        result = self.execute_sql_statement(query, include_row_data)
        return pd.read_csv(result.filepath)

    def insert_table_rows(self, table_id: str, data: pd.DataFrame):
        synapse_id = self._get_table_synapse_id(table_id)
        table = self.syn.get(synapse_id)
        self.syn.store(sc.Table(table, data))

    # Need to figure out the best way of doing deletes and updates.
    def update_table_rows(self, table_id: str, data: pd.DataFrame):
        """ Placeholder
        """

    def delete_table_rows(self, table_id: str, column: str, values: List[str]):
        pass

    def upsert_table_rows(self, table_id: str, rows: List[Dict]):
        pass

    def _get_tables(self) -> List:
        return list(self.syn.getChildren(self.project, includeTypes=['table']))

    def _get_columns_from_table(self, table_id: str) -> List[sc.Column]:
        synapse_id = self._get_table_synapse_id(table_id)
        return list(self.syn.getColumns(x=synapse_id))

    def _get_table_synapse_id(self, table_id: str) -> str:
        tables = self._get_tables()
        return [table['id'] for table in tables if table['name'] == table_id][0]

    def _create_synapse_column(self, name: str, datatype: str) -> sc.Column:
        if datatype == DBDatatype.Text:
            syn_column = sc.Column(name=name, columnType='STRING', maximumSize=100)
        elif datatype == DBDatatype.Date:
            syn_column = sc.Column(name=name, columnType='DATE')
        elif datatype == DBDatatype.Int:
            syn_column =  sc.Column(name=name, columnType='INTEGER')
        elif datatype == DBDatatype.Float:
            syn_column = sc.Column(name=name, columnType='DOUBLE')
        elif datatype == DBDatatype.Boolean:
            syn_column= sc.Column(name=name, columnType='BOOLEAN')
        else:
            raise ValueError ()
        return syn_column
