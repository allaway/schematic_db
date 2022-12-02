"""
Testing for MySQLDatabase and PostgresDatabase

For testing locally there should be a mysql server and postgres server running.
A config yaml should exist at 'tests/data/local_secrets.yml'.
This config should look like:

mysql:
  username: "user1"
  password: "xxx"
  host: "localhost"
postgres:
  username: "user2"
  password: "xxx"
  host: "localhost"

"""
from typing import Generator, Any
import pytest
import pandas as pd
from schematic_db.db_config.db_config import DBObjectConfig
from schematic_db.rdb.mysql import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase


@pytest.fixture(name="sql_databases", scope="module")
def fixture_sql_databases(
    mysql: MySQLDatabase,
    postgres: PostgresDatabase,
) -> Generator:
    """Yields a list of databases to test"""
    yield [mysql, postgres]


@pytest.mark.fast
class TestSQLGetters:  # pylint: disable=too-few-public-methods
    """Testing for RelationalDatabase getters"""

    def test_get_table_names(
        self, sql_databases: Any, table_one_config: DBObjectConfig
    ) -> None:
        """Tests RelationalDatabase.get_table_names()"""
        for obj in sql_databases:
            assert obj.get_table_names() == []
            obj.add_table("table_one", table_one_config)
            assert obj.get_table_names() == ["table_one"]
            obj.drop_table("table_one")
            assert obj.get_table_names() == []


@pytest.mark.fast
class TestSQLUpdateTables:  # pylint: disable=too-few-public-methods
    """Testing for RelationalDatabase methods that update tables"""

    def test_add_drop_table(
        self,
        sql_databases: Any,
        table_one_config: DBObjectConfig,
        table_two_config: DBObjectConfig,
        table_three_config: DBObjectConfig,
    ) -> None:
        """Testing for RelationalDatabase.add_table() and and RelationalDatabase.drop_table()"""
        for obj in sql_databases:
            assert obj.get_table_names() == []
            obj.add_table("table_one", table_one_config)
            assert obj.get_table_names() == ["table_one"]
            obj.add_table("table_two", table_two_config)
            assert obj.get_table_names() == ["table_one", "table_two"]
            obj.add_table("table_three", table_three_config)
            assert obj.get_table_names() == ["table_one", "table_three", "table_two"]
            obj.drop_table("table_three")
            assert obj.get_table_names() == ["table_one", "table_two"]
            obj.drop_table("table_two")
            assert obj.get_table_names() == ["table_one"]
            obj.drop_table("table_one")
            assert obj.get_table_names() == []


@pytest.mark.fast
class TestSQLUpdateRows:
    """Testing for SQL methods that update rows"""

    def test_upsert_table_rows1(
        self,
        sql_databases: Any,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for RelationalDatabase.upsert_table_rows()
        Adding to an empty table and then upserting the same data
        """
        for obj in sql_databases:
            assert obj.get_table_names() == []
            obj.add_table("table_one", table_one_config)
            assert obj.get_table_names() == ["table_one"]

            obj.upsert_table_rows("table_one", table_one)
            query_result1 = obj.query_table("table_one", table_one_config)

            obj.upsert_table_rows("table_one", table_one)
            query_result2 = obj.query_table("table_one", table_one_config)
            pd.testing.assert_frame_equal(query_result1, query_result2)

            table_one_copy = table_one.copy()
            table_one_copy["string_one_col"] = ["a", "b", "c"]
            obj.upsert_table_rows("table_one", table_one_copy)
            query_result3 = obj.query_table("table_one", table_one_config)
            assert query_result3["string_one_col"].values.tolist() == ["a", "b", "c"]

            obj.drop_table("table_one")
            assert obj.get_table_names() == []

    def test_upsert_table_rows2(
        self,
        sql_databases: Any,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for RelationalDatabase.upsert_table_rows()
        Adding to an empty table and then updating one row
        """
        for obj in sql_databases:
            assert obj.get_table_names() == []
            obj.add_table("table_one", table_one_config)
            assert obj.get_table_names() == ["table_one"]

            obj.upsert_table_rows("table_one", table_one)
            query_result1 = obj.query_table("table_one", table_one_config)
            assert query_result1["string_one_col"].values.tolist() == ["a", "b", None]

            table_one_copy = table_one.copy()
            table_one_copy["string_one_col"] = ["a", "b", "c"]
            obj.upsert_table_rows("table_one", table_one_copy)
            query_result2 = obj.query_table("table_one", table_one_config)
            assert query_result2["string_one_col"].values.tolist() == ["a", "b", "c"]

            obj.drop_table("table_one")
            assert obj.get_table_names() == []

    def test_upsert_table_rows3(
        self,
        sql_databases: Any,
        table_two: pd.DataFrame,
        table_two_config: DBObjectConfig,
    ) -> None:
        """
        Testing for RelationalDatabase.upsert_table_rows()
        Adding to an empty table and then updating one row, and adding one row
        """
        for obj in sql_databases:
            assert obj.get_table_names() == []
            obj.add_table("table_two", table_two_config)
            assert obj.get_table_names() == ["table_two"]

            obj.upsert_table_rows("table_two", table_two)
            query_result1 = obj.query_table("table_two", table_two_config)
            assert query_result1["string_two_col"].values.tolist() == [
                "a",
                "b",
                "c",
                "d",
            ]

            table_two_copy = table_two.copy()
            table_two_copy["string_two_col"] = ["a", "b", "c", "X"]
            table_two_copy = pd.concat(
                [
                    table_two_copy,
                    pd.DataFrame(
                        {
                            "pk_two_col": ["key5"],
                            "string_two_col": ["Y"],
                        }
                    ),
                ],
                ignore_index=True,
            )
            obj.upsert_table_rows("table_two", table_two_copy)
            query_result2 = obj.query_table("table_two", table_two_config)
            assert query_result2["string_two_col"].values.tolist() == [
                "a",
                "b",
                "c",
                "X",
                "Y",
            ]

            obj.drop_table("table_two")
            assert obj.get_table_names() == []

    def test_delete_table_rows1(
        self,
        sql_databases: Any,
        table_one_config: DBObjectConfig,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for RelationalDatabase.delete_table_rows()"""
        for obj in sql_databases:
            assert obj.get_table_names() == []
            obj.add_table("table_one", table_one_config)
            assert obj.get_table_names() == ["table_one"]
            obj.upsert_table_rows("table_one", table_one)
            result1 = obj.query_table("table_one", table_one_config)
            pd.testing.assert_frame_equal(table_one, result1)
            obj.delete_table_rows("table_one", table_one.iloc[0:2, :])
            result_keys = obj.query_table("table_one", table_one_config)[
                "pk_one_col"
            ].to_list()
            correct_keys = table_one.iloc[2:, :]["pk_one_col"].to_list()
            assert result_keys == correct_keys

            obj.drop_table("table_one")
            assert obj.get_table_names() == []
