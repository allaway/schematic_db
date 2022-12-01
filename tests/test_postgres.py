"""
Testing for  Postgres.

For testing locally there should be a postgres server running.
A config yaml should exist at 'tests/data/local_postgres_config.yml'.
This config should look like:
username: "root"
password: "root"
host: "localhost"
schema: "test_schema"
The schema should not exist on the database at the beginning of the test.
This file is ignored by git.

If the the config doesn't exist, the file at 'tests/data/postgres_config.yml'
will be used.
"""
from datetime import datetime
import pytest
import pandas as pd
from schematic_db.db_config.db_config import DBObjectConfig
from schematic_db.rdb.postgres import PostgresDatabase


@pytest.mark.fast
class TestPostgresUpdateTables:  # pylint: disable=too-few-public-methods
    """Testing for Postgres methods that update tables"""

    def test_add_drop_table(
        self,
        postgres: PostgresDatabase,
        table_one_config: DBObjectConfig,
        table_two_config: DBObjectConfig,
        table_three_config: DBObjectConfig,
    ) -> None:
        """
        Testing for Postgres.add_table() and and Postgres.drop_table()
        """
        assert postgres.get_table_names() == []
        postgres.add_table("table_one", table_one_config)
        assert postgres.get_table_names() == ["table_one"]
        postgres.add_table("table_two", table_two_config)
        assert postgres.get_table_names() == ["table_one", "table_two"]
        postgres.add_table("table_three", table_three_config)
        assert postgres.get_table_names() == ["table_one", "table_three", "table_two"]
        postgres.drop_table("table_three")
        assert postgres.get_table_names() == ["table_one", "table_two"]
        postgres.drop_table("table_two")
        assert postgres.get_table_names() == ["table_one"]
        postgres.drop_table("table_one")
        assert postgres.get_table_names() == []


@pytest.mark.fast
class TestPostgresGetters:  # pylint: disable=too-few-public-methods
    """
    Testing for Postgres
    """

    def test_get_table_names(
        self, postgres: PostgresDatabase, table_one_config: DBObjectConfig
    ) -> None:
        """
        Testing for Postgres.get_table_names()
        """
        assert postgres.get_table_names() == []
        postgres.add_table("table_one", table_one_config)
        assert postgres.get_table_names() == ["table_one"]
        postgres.drop_table("table_one")
        assert postgres.get_table_names() == []


@pytest.mark.fast
class TestPostgresUpdateRows:
    """Testing for Postgres methods that update rows"""

    def test_upsert_table_rows1(
        self,
        postgres: PostgresDatabase,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for Postgres.upsert_table_rows()
        Whole table at once
        """
        assert postgres.get_table_names() == []
        postgres.add_table("table_one", table_one_config)
        assert postgres.get_table_names() == ["table_one"]

        postgres.upsert_table_rows("table_one", table_one)
        query_result1 = postgres.query_table("table_one", table_one_config)

        postgres.upsert_table_rows("table_one", table_one)
        query_result2 = postgres.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(query_result1, query_result2)

        table_one_copy = table_one.copy()
        table_one_copy["string_one_col"] = ["a", "b", "c"]
        postgres.upsert_table_rows("table_one", table_one_copy)
        query_result3 = postgres.query_table("table_one", table_one_config)
        assert query_result3["string_one_col"].values.tolist() == ["a", "b", "c"]

        postgres.drop_table("table_one")
        assert postgres.get_table_names() == []

    def test_upsert_table_rows2(
        self,
        postgres: PostgresDatabase,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for Postgres.upsert_table_rows()
        Whole table at once
        """
        assert postgres.get_table_names() == []
        postgres.add_table("table_one", table_one_config)
        assert postgres.get_table_names() == ["table_one"]

        postgres.upsert_table_rows("table_one", table_one.iloc[0:1, :])
        query_result = postgres.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one.iloc[0:1, :], query_result)

        postgres.upsert_table_rows("table_one", table_one.iloc[1:2, :])
        query_result = postgres.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one.iloc[0:2, :], query_result)

        postgres.upsert_table_rows("table_one", table_one.iloc[2:3, :])
        query_result = postgres.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, query_result)

        postgres.drop_table("table_one")
        assert postgres.get_table_names() == []

    def test_upsert_table_rows3(
        self,
        postgres: PostgresDatabase,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for Postgres.upsert_table_rows()
        Updating a row
        """
        assert postgres.get_table_names() == []
        postgres.add_table("table_one", table_one_config)
        assert postgres.get_table_names() == ["table_one"]

        postgres.upsert_table_rows("table_one", table_one)
        query_result = postgres.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, query_result)

        postgres.upsert_table_rows(
            "table_one", pd.DataFrame({"pk_one_col": ["key3"], "string_one_col": ["x"]})
        )
        postgres.upsert_table_rows(
            "table_one",
            pd.DataFrame(
                {
                    "pk_one_col": ["key3"],
                    "double_one_col": [3.3],
                    "bool_one_col": [True],
                }
            ),
        )
        query_result = postgres.query_table("table_one", table_one_config)
        new_table = pd.concat(
            [
                table_one.iloc[0:2, :],
                pd.DataFrame(
                    {
                        "pk_one_col": ["key3"],
                        "string_one_col": ["x"],
                        "int_one_col": [3],
                        "double_one_col": [3.3],
                        "date_one_col": [datetime(2022, 8, 2)],
                        "bool_one_col": [True],
                    }
                ),
            ],
            ignore_index=True,
        )
        new_table = new_table.astype(
            {"int_one_col": "Int64", "bool_one_col": "boolean"}
        )
        new_table["date_one_col"] = pd.to_datetime(new_table["date_one_col"]).dt.date
        pd.testing.assert_frame_equal(query_result, new_table)

        postgres.drop_table("table_one")
        assert postgres.get_table_names() == []

    def test_delete_table_rows1(
        self,
        postgres: PostgresDatabase,
        table_one_config: DBObjectConfig,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for Postgres.delete_table_rows()"""
        assert postgres.get_table_names() == []
        postgres.add_table("table_one", table_one_config)
        assert postgres.get_table_names() == ["table_one"]
        postgres.upsert_table_rows("table_one", table_one)
        result1 = postgres.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, result1)
        postgres.delete_table_rows("table_one", table_one.iloc[0:2, :])
        result_keys = postgres.query_table("table_one", table_one_config)[
            "pk_one_col"
        ].to_list()
        correct_keys = table_one.iloc[2:, :]["pk_one_col"].to_list()
        assert result_keys == correct_keys

        postgres.drop_table("table_one")
        assert postgres.get_table_names() == []
