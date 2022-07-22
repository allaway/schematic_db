"""RDBType
"""
from abc import ABC, abstractmethod
from typing import List, Dict
from db_object_config import DBObjectConfig

class RDBType(ABC):
    """ An interface for relational database types
    """

    @abstractmethod
    def execute_sql_statement(self, statement: str):
        """Executes a valid SQL statement
        Should be used when a result isn't expected.
        mysql.execute_sql_statement("DROP TABLE IF EXISTS table_name;")

        Args:
            statement (str): A SQL statement

        Returns:
            sqlalchemy.engine.cursor.LegacyCursorResult: A SQL result
        """

    @abstractmethod
    def execute_sql_query(self, query: str) -> List:
        """Executes a valid SQL statement
        Should be used when a result is expected.
        mysql.execute_sql_query("SHOW DATABASES;")

        Args:
            statement (str): A SQL statement

        Returns:
            list: A SQL result
        """

    @abstractmethod
    def add_table(self, table_id: str, table_config: DBObjectConfig):
        """Adds a table to the schema

        mysql.add_table(
            "table_id",
            DBObjectConfig(
                name = "table_id",
                attributes = [
                    DBAttributeConfig(name="p_key", datatype=DBDatatype.Text),
                    DBAttributeConfig(name="string", datatype=DBDatatype.Text),
                    DBAttributeConfig(name="int", datatype=DBDatatype.Int)
                ],
                primary_keys = ["p_key"],
                foreign_keys = []
            )
        )

        Args:
            table_id (str): The id(name) of the table to be added
            table_config (DBObjectConfig): A generic representation of the table as a
            DBObjectConfig object.
        """

    @abstractmethod
    def drop_table(self, table_id: str):
        """Drops a table from the schema

        mysql.drop_table("table_id")

        Args:
            table_id (str): The id(name) of the table to be dropped
        """

    @abstractmethod
    def add_table_column(self, table_id: str, column_name: str, datatype: str):
        """Adds a column to the given table

        mysql.add_table_column("table_id", "name", "varchar(100)")

        Args:
            table_id (str): The id(name) of the table the column will be added to
            column_name (str): The name of the column being added
            datatype (str): The SQL datatype of the column being added
        """

    @abstractmethod
    def drop_table_column(self, table_id: str, column_name: str):
        """Removes a column from the given table

        Args:
            table_id (str): The id(name) of the table the column will be removed from
            column_name (str): The name of the column being removed
        """

    @abstractmethod
    def insert_table_rows(self, table_id: str, rows: List[Dict]):
        """Inserts rows to the given table

        mysql.insert_table_rows(
            "table_id",
            [
                {"p_key": "key1", "string": "a", "int": 1},
                {"p_key": "key2", "string": "b", "int": 2}
            ]
        )

        Args:
            table_id (str): The id(name) of the table the rows will be added to
            rows (List[Dict]): A list of rows in dictionary form where each key is a column name
            and each value is is a value in that column
        """

    @abstractmethod
    def delete_table_rows(self, table_id: str, column: str, values: List[str]):
        """Deletes rows from the given table

        mysql.delete_table_rows("table_id", "p_key", ["key1"])

        Args:
            table_id (str): The id(name) of the table the rows will be deleted from
            column (str): The column name used to identify the rows
            values (List[str]): A list of values. Rows with these values in the given column will
            be deleted
        """

    @abstractmethod
    def upsert_table_rows(self, table_id: str, rows: List[Dict]):
        """Updates or inserts rows into the given table

        mysql.upsert_table_rows(
            "table_id",
            [
                {"p_key": "key1", "string": "a", "int": 1},
                {"p_key": "key2", "string": "b", "int": 2}
            ]
        )

        Args:
            table_id (str): The id(name) of the table the rows will be updated or added to
            rows (List[Dict]): A list of rows in dictionary form where each key is a column name
            and each value is is a value in that column
        """

    @abstractmethod
    def get_table_names(self) -> List[str]:
        """Gets the names of the tables in the database

        Returns:
            List[str]: A list of table names
        """

    @abstractmethod
    def get_column_names_from_table(self, table_id: str) -> List[str]:
        """Gets the names of the columns from the given table

        Args:
            table_id (str): The id(name) of the table the columns will be returned from

        Returns:
            List[str]: A list fo column names
        """
