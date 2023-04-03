"""MySQLDatabase"""
from typing import Any
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert
from schematic_db.db_config.db_config import (
    DBDatatype,
    DBAttributeConfig,
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

    def _upsert_table_row(
        self, row: dict[str, Any], table: sa.table, table_name: str
    ) -> None:  # pylint: disable=unused-argument
        statement = insert(table).values(row).on_duplicate_key_update(**row)
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(statement)

    def _get_datatype(
        self, attribute: DBAttributeConfig, primary_key: str, foreign_keys: list[str]
    ) -> Any:
        datatypes = {
            DBDatatype.TEXT: sa.VARCHAR(5000),
            DBDatatype.DATE: sa.Date,
            DBDatatype.INT: sa.Integer,
            DBDatatype.FLOAT: sa.Float,
            DBDatatype.BOOLEAN: sa.Boolean,
        }
        # Keys need to be max 100 chars
        if attribute.datatype == DBDatatype.TEXT and (
            attribute.name == primary_key or attribute.name in foreign_keys
        ):
            return sa.VARCHAR(100)
        # Strings that need to be indexed need to be max 1000 chars
        if attribute.index and attribute.datatype == DBDatatype.TEXT:
            return sa.VARCHAR(1000)

        # Otherwise use datatypes dict
        return datatypes[attribute.datatype]
