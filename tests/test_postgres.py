import sqlalchemy as sa
import pandas as pd
import numpy as np
import pytest
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.db_schema.db_schema import TableSchema


@pytest.mark.fast
class TestPostgresDatabase:
    def test_upsert_table_rows(
        self,
        postgres_database: PostgresDatabase,
        table_one: pd.DataFrame,
        table_one_schema: TableSchema,
    ) -> None:
        postgres_database.add_table("table_one", table_one_schema)
        data = table_one.replace({np.nan: None})
        rows = data.to_dict("records")
        table = sa.Table(
            "table_one",
            postgres_database.metadata,
            autoload_with=postgres_database.engine,
        )

        query_result1 = postgres_database.query_table("table_one")
        assert len(query_result1.index) == 0

        postgres_database._upsert_table_rows(rows, table, "table_one", "pk_one_col")
        query_result2 = postgres_database.query_table("table_one")
        assert query_result2["pk_one_col"].values.tolist() == ["key1", "key2", "key3"]
        assert query_result2["string_one_col"].values.tolist() == ["a", "b", None]

        table_one_copy = table_one.copy()
        table_one_copy["string_one_col"] = ["a", "b", "c"]
        data_copy = table_one_copy.replace({np.nan: None})
        rows_copy = data_copy.to_dict("records")
        postgres_database._upsert_table_rows(rows_copy, table, "table_one", "pk_one_col")
        query_result3 = postgres_database.query_table("table_one")
        assert query_result3["pk_one_col"].values.tolist() == ["key1", "key2", "key3"]
        assert query_result3["string_one_col"].values.tolist() == ["a", "b", "c"]
