from .rdb_type import RDBType
from typing import List, Dict
from yaml import safe_load
import sqlalchemy as sa
from db_object_config import DBObjectConfig, DBDatatype
import logging

class MySQL(RDBType):

    def __init__(self, config_yaml_path: str, schema_name: str):

        with open(config_yaml_path, mode="rt", encoding="utf-8") as file:
            config_dict = safe_load(file)
        username = config_dict.get("username")
        password = config_dict.get("password")
        host = config_dict.get("host")

        url = str("mysql://{0}:{1}@{2}/".format(username, password, host))
        engine = sa.create_engine(url, encoding = 'utf-8', echo = True)
        create_statement = str("CREATE DATABASE IF NOT EXISTS {0};".format(schema_name))
        engine.execute(create_statement)

        url2 = str("mysql://{0}:{1}@{2}/{3}".format(username, password, host, schema_name))
        engine2 = sa.create_engine(url2, encoding = 'utf-8', echo = True)
        self.engine = engine2
        self.metadata = sa.MetaData()

    def execute_sql_statement(self, statement: str):
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(sa.text(statement))
        return(result)

    def execute_sql_query(self, query: str):
        result = self.execute_sql_statement(query).fetchall()
        return(result)

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
        statement = str('DROP TABLE IF EXISTS {0};'.format(table_id))
        self.execute_sql_statement(statement)

    def add_table_column(self, table_id: str, column_name: str, datatype: str):
        statement = "ALTER TABLE {0} ADD {1} {2};".format(table_id, column_name, datatype)
        self.execute_sql_statement(statement)

    def drop_table_column(self, table_id: str, column_name: str):
        statement = "ALTER TABLE {0} DROP {1};".format(table_id, column_name)
        self.execute_sql_statement(statement)

    def insert_table_rows(self, table_id: str, rows: List[Dict]):
        table = sa.Table(table_id, self.metadata, autoload_with=self.engine)
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(sa.insert(table), rows)

    def delete_table_rows(self, table_id: str, column: str, values: List[str]):
        table = sa.Table(table_id, self.metadata, autoload_with=self.engine)
        statement = sa.delete(table).where(table.c[column].in_(values))
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(statement)

    def update_table_rows(self, table_id: str):
        pass

    def get_tables(self) -> List[str]:
        inspector = sa.inspect(self.engine)
        return(inspector.get_table_names())

    def get_columns_from_table(self, table_id: str) -> List[str]:
        inspector = sa.inspect(self.engine)
        return(inspector.get_columns(table_id))

    def get_column_names_from_table(self, table_id: str) -> List[str]:
        columns = self.get_columns_from_table(table_id)
        names = [col.get("name") for col in columns]
        return(names)

    def _get_schemas(self) -> List[str]:
        inspector = sa.inspect(self.engine)
        return(inspector.get_schema_names())

    def _get_current_schema(self) -> str:
        return(self.execute_sql_query("SELECT DATABASE();")[0][0])

    def _change_current_schema(self, schema_name):
        self.execute_sql_statement("USE {0};".format(schema_name))

    def _create_schema(self, schema_name):
        statement = str("CREATE DATABASE IF NOT EXISTS {0};".format(schema_name))
        self.execute_sql_statement(statement)

    def _drop_schema(self, schema_name):
        statement = str("DROP DATABASE {0};".format(schema_name))
        self.execute_sql_statement(statement)