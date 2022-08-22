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
    - Handles Synapse specific functionality.
    """

    def __init__(self, config_dict: dict) -> None:
        """Init

        Args:
            config_dict (dict): A dict with fields ["username", "auth_token", "project_id"]
        """
        username = config_dict.get("username")
        auth_token = config_dict.get("auth_token")
        project_id = config_dict.get("project_id")

        syn = sc.Synapse()
        syn.login(username, authToken=auth_token)

        self.syn = syn
        self.project_id = project_id

    def get_table_names(self) -> list[str]:
        """Gets the names of the tables in the schema

        Returns:
            list[str]: A list of table names
        """
        tables = self._get_tables()
        return [table["name"] for table in tables]

    def get_synapse_id_from_table_name(self, table_name: str) -> str:
        """Gets the synapse id from the table name

        Args:
            table_name (str): The name of the table

        Raises:
            ValueError: When no tables match the name
            ValueError: When multiple tables match the name

        Returns:
            str: A synapse id
        """
        tables = self._get_tables()
        matching_tables = [table for table in tables if table["name"] == table_name]
        if len(matching_tables) == 0:
            raise ValueError(f"No matching tables with name {table_name}")
        if len(matching_tables) > 1:
            raise ValueError(f"Multiple matching tables with name {table_name}")
        return matching_tables[0]["id"]

    def get_table_name_from_synapse_id(self, synapse_id: str) -> str:
        """Gets the table name from the synapse id

        Args:
            synapse_id (str): A synapse id

        Returns:
            str: The name of the table with the synapse id
        """
        tables = self._get_tables()
        return [table["name"] for table in tables if table["id"] == synapse_id][0]

    def execute_sql_statement(
        self, statement: str, include_row_data: bool = False
    ) -> str:
        """Execute a SQL statement

        Args:
            statement (str): A SQL statement that can be run by Synapse
            include_row_data (bool, optional): Include row_id and row_etag. Defaults to False.

        Returns:
            str: A path to a csv file containing the query
        """
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
        """Queries a whole table

        Args:
            table_name (str): The name of the table to query
            table_config (DBObjectConfig): The config for the table

        Returns:
            pd.DataFrame: The queried table
        """
        table_id = self.get_synapse_id_from_table_name(table_name)
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
        """Execute a Sql query

        Args:
            query (str): A SQL statement that can be run by Synapse
            include_row_data (bool, optional): Include row_id and row_etag. Defaults to False.

        Returns:
            pd.DataFrame: The queried table
        """
        result = self.execute_sql_statement(query, include_row_data)
        table = pd.read_csv(result.filepath)
        return table

    def add_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Adds a synapse table

        Args:
            table_name (str): The name of the table to be added
            table_config (DBObjectConfig): The config the table to be added
        """
        columns = []
        values = {}
        for att in table_config.attributes:
            column = self._create_synapse_column(att.name, att.datatype)
            columns.append(column)
            values[att.name] = []

        schema = sc.Schema(name=table_name, columns=columns, parent=self.project_id)
        table = sc.Table(schema, values)
        table = self.syn.store(table)

    def drop_table(self, table_name: str) -> None:
        """Drops a Synapse table

        Args:
            table_name (str): The name of the table to be dropped
        """
        synapse_id = self.get_synapse_id_from_table_name(table_name)
        self.syn.delete(synapse_id)

    def _create_synapse_column(self, name: str, datatype: str) -> sc.Column:
        func = SYNAPSE_DATATYPES.get(datatype)
        return func(name=name)
