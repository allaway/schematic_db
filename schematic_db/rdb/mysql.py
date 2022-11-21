"""MySQLDatabase"""
from typing import Any
from dataclasses import dataclass
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import exc
from schematic_db.db_config import DBObjectConfig, DBDatatype
from schematic_db.db_config.db_config import DBAttributeConfig
from .rdb import RelationalDatabase, UpdateDBTableError

MYSQL_DATATYPES = {
    DBDatatype.TEXT: sa.Text(5000),
    DBDatatype.DATE: sa.Date,
    DBDatatype.INT: sa.Integer,
    DBDatatype.FLOAT: sa.Float,
    DBDatatype.BOOLEAN: sa.Boolean,
}

PANDAS_DATATYPES = {DBDatatype.INT: "Int64", DBDatatype.BOOLEAN: "boolean"}


class DataframeKeyError(Exception):
    """DataframeKeyError"""

    def __init__(self, message: str, key: str) -> None:
        self.message = message
        self.key = key
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}:{self.key}"


def create_foreign_key_column(
    name: str,
    datatype: str,
    foreign_table_name: str,
    foreign_table_column: str,
    nullable: bool,
) -> sa.Column:
    """Creates a sqlalchemy.column that is a foreign key

    Args:
        name (str): The name of the column
        datatype (str): The SQL datatype of the column
        foreign_table_name (str): The name of the table the foreign key is referencing
        foreign_table_column (str): The name of the column the foreign key is referencing
        nullable (bool): If the column is nullable

    Returns:
        sa.Column: A sqlalchemy.column
    """
    col = sa.Column(
        name,
        datatype,
        sa.ForeignKey(
            f"{foreign_table_name}.{foreign_table_column}",
            ondelete="CASCADE",
        ),
        nullable=nullable,
    )
    return col


@dataclass
class MySQLConfig:
    """A config for a MySQL database."""

    username: str
    password: str
    host: str
    name: str


class MySQLDatabase(RelationalDatabase):
    """MySQLDatabase
    - Represents a mysql database.
    - Implements the RelationalDatabase interface.
    - Handles MYSQL specific functionality.
    """

    def __init__(self, config: MySQLConfig, verbose: bool = False):
        """Init
        An initial connection is created to the database server without the database.
        The database will be created if it doesn't exist.
        A second connection is created with the database.
        The second connection is used to create the sqlalchemy connection and metadata.

        Args:
            config (MySQLConfig): A MySQL config
            verbose (bool): Sends much more to logging.info
        """
        self.username = config.username
        self.password = config.password
        self.host = config.host
        self.name = config.name
        self.verbose = verbose

        self.create_database()
        self.metadata = sa.MetaData()

    def drop_database(self) -> None:
        """Drops the database from the server"""
        statement = f"DROP DATABASE {self.name};"
        self.engine.execute(statement)

    def create_database(self) -> None:
        """Creates the database"""
        url = f"mysql://{self.username}:{self.password}@{self.host}/"
        engine = sa.create_engine(url, encoding="utf-8", echo=self.verbose)
        statement = f"CREATE DATABASE IF NOT EXISTS {self.name};"
        engine.execute(statement)

        url2 = f"mysql://{self.username}:{self.password}@{self.host}/{self.name}"
        engine2 = sa.create_engine(url2, encoding="utf-8", echo=self.verbose)
        self.engine = engine2

    def drop_all_tables(self) -> None:
        self.drop_database()
        self.metadata.clear()
        self.create_database()

    def execute_sql_query(self, query: str) -> pd.DataFrame:
        result = self._execute_sql_statement(query).fetchall()
        table = pd.DataFrame(result)
        return table

    def update_table(self, data: pd.DataFrame, table_config: DBObjectConfig) -> None:
        table_names = self.get_table_names()
        table_name = table_config.name
        if table_name not in table_names:
            self.add_table(table_name, table_config)
        try:
            self.upsert_table_rows(table_name, data)
        except exc.SQLAlchemyError as error:
            error_msg = str(error.__dict__["orig"])
            raise UpdateDBTableError(table_name, error_msg) from error

    def drop_table(self, table_name: str) -> None:
        self._execute_sql_statement(f"DROP TABLE IF EXISTS `{table_name}`;")
        self.metadata.clear()

    def delete_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        query = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"
        table = self.execute_sql_query(query)
        primary_key = table["Column_name"].tolist()[0]
        values = data[primary_key].tolist()
        values = [f"'{i}'" for i in values]
        statement = (
            f"DELETE FROM {table_name} WHERE {primary_key} IN ({','.join(values)})"
        )
        self._execute_sql_statement(statement)

    def get_table_names(self) -> list[str]:
        inspector = sa.inspect(self.engine)
        return inspector.get_table_names()

    def add_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Adds a table to the schema

        Args:
            table_name (str): The name of the table
            table_config (DBObjectConfig): The config for the table to be added
        """
        columns = self._create_columns(table_config)
        sa.Table(table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine)

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Inserts and/or updates the rows of the table

        Args:
            table_name (str): _The name of the table to be upserted
            data (pd.DataFrame): The rows to be upserted
        """
        data = data.replace({np.nan: None})
        rows = data.to_dict("records")
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        for row in rows:
            statement = insert(table).values(row).on_duplicate_key_update(**row)
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(statement)

    def query_table(
        self, table_name: str, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        """Queries an entire table

        Args:
            table_name (str): The table to be queried
            table_config (DBObjectConfig): The config for the table to be queried

        Returns:
            pd.DataFrame: The query result
        """
        query = f"SELECT * FROM {table_name};"
        table = self.execute_sql_query(query)
        for att in table_config.attributes:
            pandas_value = PANDAS_DATATYPES.get(att.datatype, None)
            if pandas_value is not None:
                table = table.astype({att.name: pandas_value})
        return table

    def _execute_sql_statement(self, statement: str) -> Any:
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def _create_columns(self, table_config: DBObjectConfig) -> list[sa.Column]:
        columns = [
            self._create_column(att, table_config) for att in table_config.attributes
        ]
        columns.append(sa.PrimaryKeyConstraint(table_config.primary_key))
        return columns

    def _create_column(
        self, attribute: DBAttributeConfig, table_config: DBObjectConfig
    ) -> sa.Column:
        att_name = attribute.name
        primary_key = table_config.primary_key
        foreign_keys = table_config.get_foreign_key_names()
        nullable = not attribute.required

        # If column is a key, set datatype to sa.String(100)
        if att_name == primary_key or att_name in foreign_keys:
            sql_datatype = sa.String(100)
        else:
            sql_datatype = MYSQL_DATATYPES.get(attribute.datatype)

        if att_name in foreign_keys:
            key = table_config.get_foreign_key_by_name(att_name)
            return create_foreign_key_column(
                att_name,
                sql_datatype,
                key.foreign_object_name,
                key.foreign_attribute_name,
                nullable,
            )
        return sa.Column(att_name, sql_datatype, nullable=nullable)
