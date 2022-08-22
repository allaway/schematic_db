"""MYSQL
"""
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert
from db_object_config import DBObjectConfig, DBDatatype
from .rdb import RelationalDatabase

MYSQL_DATATYPES = {
    DBDatatype.TEXT: sa.String(100),
    DBDatatype.DATE: sa.Date,
    DBDatatype.INT: sa.Integer,
    DBDatatype.FLOAT: sa.Float,
    DBDatatype.BOOLEAN: sa.Boolean,
}

PANDAS_DATATYPES = {DBDatatype.INT: "Int64", DBDatatype.BOOLEAN: "boolean"}


class MySQLDatabase(RelationalDatabase):
    """MYSQL
    - Represents a mysql database.
    - Implements the RDBType interface.
    - Handles MYSQL specific functionality.
    """

    def __init__(self, config_dict: dict):
        """Init
        obj = MySQL({
            username: "root"
            password: "root"
            host: "localhost"
            schema: "test_schema"
        })

        An initial connection is created to the database without the schema.
        The schema will be created if it doesn't exist.
        A second connection is created with the schema.
        The second connection is used to create the sqlalchemy connection and metadata.

        Args:
            config_dict (dict): A dict with mysql specific fields
        """
        username = config_dict.get("username")
        password = config_dict.get("password")
        host = config_dict.get("host")
        schema = config_dict.get("schema")

        url = f"mysql://{username}:{password}@{host}/"
        engine = sa.create_engine(url, encoding="utf-8", echo=True)
        create_statement = f"CREATE DATABASE IF NOT EXISTS {schema};"
        engine.execute(create_statement)

        url2 = f"mysql://{username}:{password}@{host}/{schema}"
        engine2 = sa.create_engine(url2, encoding="utf-8", echo=True)
        self.engine = engine2
        self.metadata = sa.MetaData()

    def execute_sql_query(self, query: str) -> pd.DataFrame:
        result = self.execute_sql_statement(query).fetchall()
        table = pd.DataFrame(result)
        return table

    def execute_sql_statement(self, statement: str):
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def update_table(self, data: pd.DataFrame, table_config: DBObjectConfig):
        table_names = self.get_table_names()
        table_name = table_config.name
        if table_name not in table_names:
            self.add_table(table_name, table_config)
        self.upsert_table_rows(table_name, data)

    def get_table_names(self) -> list[str]:
        inspector = sa.inspect(self.engine)
        return inspector.get_table_names()

    def add_table(self, table_name: str, table_config: DBObjectConfig):
        columns = self._create_columns(table_config)
        sa.Table(table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine)

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame):
        data = data.replace({np.nan: None})
        rows = data.to_dict("records")
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        for row in rows:
            statement = insert(table).values(row).on_duplicate_key_update(**row)
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(statement)

    def drop_table(self, table_name: str):
        self.execute_sql_statement(f"DROP TABLE IF EXISTS {table_name};")
        self.metadata.clear()

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ):
        primary_keys = table_config.primary_keys
        for col in primary_keys:
            if col not in list(data.columns):
                raise ValueError(f"primary key: {col} missing from data")
        data = data[primary_keys]
        tuples = list(data.itertuples(index=False, name=None))
        tuples = [(f"'{i}'" for i in tup) for tup in tuples]
        tuple_strings = ["(" + ",".join(tup) + ")" for tup in tuples]
        tuple_string = ",".join(tuple_strings)
        statement = f"DELETE FROM {table_name} WHERE ({','.join(primary_keys)}) IN ({tuple_string})"
        self.execute_sql_statement(statement)

    def query_table(
        self, table_name: str, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name};"
        table = self.execute_sql_query(query)
        for att in table_config.attributes:
            pandas_value = PANDAS_DATATYPES.get(att.datatype, None)
            if pandas_value is not None:
                table = table.astype({att.name: pandas_value})
        return table

    def _create_columns(self, table_config: DBObjectConfig) -> list[sa.Column]:
        columns = []
        for att in table_config.attributes:
            att_name = att.name
            att_datatype = att.datatype
            sql_datatype = MYSQL_DATATYPES.get(att_datatype)
            if att_name in table_config.get_foreign_key_names():
                key = table_config.get_foreign_key_by_name(att_name)
                col = sa.Column(
                    att_name,
                    sql_datatype,
                    sa.ForeignKey(
                        f"{key.foreign_object_name}.{key.foreign_attribute_name}"
                    ),
                    nullable=False,
                )
            else:
                col = sa.Column(att_name, sql_datatype)
            columns.append(col)

        if table_config.primary_keys != []:
            columns.append(sa.PrimaryKeyConstraint(*table_config.primary_keys))
        return columns
