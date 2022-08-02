"""MYSQL
"""
# pylint: disable=E0401
from typing import List, Dict
import logging
from yaml import safe_load
import pandas as pd # type: ignore
import sqlalchemy as sa # type: ignore
from sqlalchemy.dialects.mysql import insert # type: ignore
from db_object_config import DBObjectConfig, DBDatatype
from .rdb_type import RDBType

class MySQL(RDBType):
    """MYSQL
    - Represents a mysql database.
    - Implements the RDBType interface.
    - Handles MYSQL specific functionality.
    """
    def __init__(self, config_yaml_path: str):
        """Init
        obj = MySQL(config_yaml_path="tests/data/mysql_config.yml")

        The config_yaml should look like:
            username: "root"
            password: "root"
            host: "localhost"
            schema: "test_schema"

        An initial connection is created to the database without the schema.
        The schema will be created if it doesn't exist.
        A second connection is created with the schema.
        The second connection is used to create the sqlalchemy connection and metadata.

        Args:
            config_yaml_path (str): The path to the config yaml
        """
        with open(config_yaml_path, mode="rt", encoding="utf-8") as file:
            config_dict = safe_load(file)
        username = config_dict.get("username")
        password = config_dict.get("password")
        host = config_dict.get("host")
        schema = config_dict.get("schema")

        url = f"mysql://{username}:{password}@{host}/"
        engine = sa.create_engine(url, encoding = 'utf-8', echo = True)
        create_statement = f"CREATE DATABASE IF NOT EXISTS {schema};"
        engine.execute(create_statement)

        url2 = f"mysql://{username}:{password}@{host}/{schema}"
        engine2 = sa.create_engine(url2, encoding = 'utf-8', echo = True)
        self.engine = engine2
        self.metadata = sa.MetaData()

    def execute_sql_statement(self, statement: str):
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def execute_sql_query(self, query: str):
        result = self.execute_sql_statement(query).fetchall()
        return result

    def add_table(self, table_id: str, table_config: DBObjectConfig):
        columns = []
        for att in table_config.attributes:
            att_name = att.name
            att_datatype = att.datatype
            if att_datatype == DBDatatype.Text:
                sql_datatype = sa.String(100)
            elif att_datatype == DBDatatype.Date:
                sql_datatype = sa.Date
            elif att_datatype == DBDatatype.Int:
                sql_datatype = sa.Integer
            elif att_datatype == DBDatatype.Float:
                sql_datatype = sa.Float
            elif att_datatype == DBDatatype.Boolean:
                sql_datatype = sa.Boolean
            else:
                logging.warning(att_datatype)
                raise ValueError ()
            columns.append(sa.Column(att_name, sql_datatype))

        if table_config.primary_keys != []:
            columns.append(sa.PrimaryKeyConstraint(*table_config.primary_keys))
        if table_config.foreign_keys != []:
            columns.append(sa.ForeignKeyConstraint(*table_config.foreign_keys))

        sa.Table(table_id, self.metadata, *columns)
        self.metadata.create_all(self.engine)

    def drop_table(self, table_id: str):
        self.execute_sql_statement(f"DROP TABLE IF EXISTS {table_id};")

    def add_table_column(self, table_id: str, column_name: str, datatype: str):
        self.execute_sql_statement(f"ALTER TABLE {table_id} ADD {column_name} {datatype};")

    def drop_table_column(self, table_id: str, column_name: str):
        self.execute_sql_statement(f"ALTER TABLE {table_id} DROP {column_name};")

    def insert_table_rows(self, table_id: str, data: pd.DataFrame):
        rows = data.to_dict("records")
        table = sa.Table(table_id, self.metadata, autoload_with=self.engine)
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(insert(table), rows)

    def delete_table_rows(self, table_id: str, column: str, values: List[str]):
        table = sa.Table(table_id, self.metadata, autoload_with=self.engine)
        statement = sa.delete(table).where(table.c[column].in_(values))
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(statement)

    def upsert_table_rows(self, table_id: str, rows: List[Dict]):
        table = sa.Table(table_id, self.metadata, autoload_with=self.engine)
        for row in rows:
            statement = insert(table).values(row).on_duplicate_key_update(**row)
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(statement)

    def get_table_names(self) -> List[str]:
        inspector = sa.inspect(self.engine)
        return inspector.get_table_names()

    def get_column_names_from_table(self, table_id: str) -> List[str]:
        columns = self._get_columns_from_table(table_id)
        names = [col.get("name") for col in columns]
        return names

    def _get_columns_from_table(self, table_id: str) -> List[Dict]:
        """Gets the columns form the given table

        Args:
            table_id (str): The id(name) of the table the columns will be returned from

        Returns:
            List[Dict]: A list of columns in dict form
        """
        inspector = sa.inspect(self.engine)
        return inspector.get_columns(table_id)

    def _get_schemas(self) -> List[str]:
        """Gets the database schemas

        Returns:
            List[str]: A list of names of the the schemas
        """
        inspector = sa.inspect(self.engine)
        return inspector.get_schema_names()

    def _get_current_schema(self) -> str:
        """Gets the current database schema

        Returns:
            str: The name of the current schema
        """
        return self.execute_sql_query("SELECT DATABASE();")[0][0]

    def _change_current_schema(self, schema_name: str):
        """Changes the current schema

        Args:
            schema_name (str): The name of the schema to change to
        """
        self.execute_sql_statement(f"USE {schema_name};")

    def _create_schema(self, schema_name: str):
        """Creates a schema

        Args:
            schema_name (str): The name of the schema to create
        """
        self.execute_sql_statement(f"CREATE DATABASE IF NOT EXISTS {schema_name};")

    def _drop_schema(self, schema_name: str):
        """Drops a schema

        Args:
            schema_name (str): The name of the schema to drop
        """
        self.execute_sql_statement(f"DROP DATABASE {schema_name};")
