"""MySQLDatabase"""
from typing import Any
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert
from schematic_db.db_schema.db_schema import (
    ColumnDatatype,
    ColumnSchema,
)
from .sql_alchemy_database import SQLAlchemyDatabase, SQLConfig


class MySQLDatabase(SQLAlchemyDatabase):
    """MySQLDatabase
    - Represents a mysql database.
    - Implements the RelationalDatabase interface.
    - Handles MYSQL specific functionality.
    """

    def __init__(
        self,
        config: SQLConfig,
        verbose: bool = False,
    ):
        """Init

        Args:
            config (MySQLConfig): A MySQL config
            verbose (bool): Sends much more to logging.info
        """
        super().__init__(config, verbose, "mysql")

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

    def _get_datatype(
        self, attribute: ColumnSchema, primary_key: str, foreign_keys: list[str]
    ) -> Any:
        datatypes = {
            ColumnDatatype.TEXT: sa.VARCHAR(5000),
            ColumnDatatype.DATE: sa.Date,
            ColumnDatatype.INT: sa.Integer,
            ColumnDatatype.FLOAT: sa.Float,
            ColumnDatatype.BOOLEAN: sa.Boolean,
        }
        # Keys need to be max 100 chars
        if attribute.datatype == ColumnDatatype.TEXT and (
            attribute.name == primary_key or attribute.name in foreign_keys
        ):
            return sa.VARCHAR(100)
        # Strings that need to be indexed need to be max 1000 chars
        if attribute.index and attribute.datatype == ColumnDatatype.TEXT:
            return sa.VARCHAR(1000)

        # Otherwise use datatypes dict
        return datatypes[attribute.datatype]
