"""Testing for RDB.
"""
import pytest
import pandas as pd
from rdb import RDB, UpdateDatabaseError
from rdb import normalize_table


class TestUtils:
    """Testing for rdb utils"""

    def test_normalize_table1(self, table_one, table_one_config):
        """Tests an already normalized table"""
        result = normalize_table(table_one, table_one_config)
        pd.testing.assert_frame_equal(result, table_one, check_like=True)

    def test_normalize_table2(self, table_123_unormalized, table_one_config):
        """Tests a denormalized table"""
        result = normalize_table(table_123_unormalized, table_one_config)
        correct_result = pd.DataFrame(
            {"pk_one_col": ["key1", "key2"], "string_one_col": ["a", "b"]}
        )
        pd.testing.assert_frame_equal(result, correct_result, check_like=True)

    def test_normalize_table3(self, table_123_unormalized, table_two_config):
        """Tests a denormalized table"""
        result = normalize_table(table_123_unormalized, table_two_config)
        correct_result = pd.DataFrame({"pk_two_col": ["key1", "key2"]})
        pd.testing.assert_frame_equal(result, correct_result, check_like=True)

    def test_normalize_table4(self, table_123_unormalized, table_three_config):
        """Tests a denormalized table"""
        result = normalize_table(table_123_unormalized, table_three_config)
        correct_result = pd.DataFrame(
            {
                "pk_one_col": ["key1", "key1", "key2", "key2"],
                "pk_two_col": ["key1", "key2", "key1", "key2"],
                "string_three_col": ["a", "b", "c", "d"],
            }
        )
        pd.testing.assert_frame_equal(result, correct_result, check_like=True)


class TestRDBMySQLUpdate:
    """Testing for RDB with MySQL database"""

    def test_init(self, rdb_mysql):
        """Testing for RDB.init()"""
        assert isinstance(rdb_mysql, RDB)

    def test_update_database_table1(self, rdb_mysql, table_one, table_one_config):
        """Testing for RDB.update_database_table()"""
        assert rdb_mysql.rdb_type.get_table_names() == []

        rdb_mysql.update_database_table(["table_one"], table_one_config)
        result = rdb_mysql.rdb_type.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result, table_one)

        rdb_mysql.drop_table("table_one")
        assert rdb_mysql.rdb_type.get_table_names() == []

    def test_update_database_table2(
        self, rdb_mysql, table_two, table_two_b, table_two_config
    ):
        """Testing for RDB.update_database_table()"""
        assert rdb_mysql.rdb_type.get_table_names() == []

        rdb_mysql.update_database_table(["table_two", "table_two_b"], table_two_config)
        result = rdb_mysql.rdb_type.query_table("table_two", table_two_config)
        test_table = pd.concat([table_two, table_two_b]).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, test_table)

        rdb_mysql.drop_table("table_two")
        assert rdb_mysql.rdb_type.get_table_names() == []

    def test_update_all_database_tables1(
        self, rdb_mysql, table_one, table_two, table_three, table_configs
    ):
        """Testing for update_all_database_tables()"""
        assert rdb_mysql.rdb_type.get_table_names() == []
        rdb_mysql.update_all_database_tables(
            [["table_one"], ["table_two"], ["table_three"]], table_configs
        )
        assert rdb_mysql.rdb_type.get_table_names() == [
            "table_one",
            "table_three",
            "table_two",
        ]
        result1 = rdb_mysql.rdb_type.query_table(
            "table_one", table_configs.get_config_by_name("table_one")
        )
        pd.testing.assert_frame_equal(result1, table_one)
        result2 = rdb_mysql.rdb_type.query_table(
            "table_two", table_configs.get_config_by_name("table_two")
        )
        pd.testing.assert_frame_equal(result2, table_two)
        result3 = rdb_mysql.rdb_type.query_table(
            "table_three", table_configs.get_config_by_name("table_three")
        )
        pd.testing.assert_frame_equal(result3, table_three)
        assert rdb_mysql.rdb_type.get_table_names() == [
            "table_one",
            "table_three",
            "table_two",
        ]

        rdb_mysql.drop_table("table_three")
        rdb_mysql.drop_table("table_two")
        rdb_mysql.drop_table("table_one")
        assert rdb_mysql.rdb_type.get_table_names() == []

    def test_update_all_database_tables2(self, rdb_mysql, table_configs):
        """Testing for update_all_database_tables() exceptions"""
        with pytest.raises(
            UpdateDatabaseError, match="Length of param manifest_table_names"
        ):
            rdb_mysql.update_all_database_tables([["table_one"]], table_configs)


class TestRDBMySQLQueries:
    """Testing for RDB with MySQL database"""

    def test_store_query_result(self, rdb_mysql, table_configs):
        """Testing for RDB.store_query_result()"""

        assert rdb_mysql.rdb_type.get_table_names() == []
        assert rdb_mysql.query_store.get_table_names() == []

        rdb_mysql.update_all_database_tables(
            [["table_one"], ["table_two"], ["table_three"]], table_configs
        )
        assert rdb_mysql.rdb_type.get_table_names() == [
            "table_one",
            "table_three",
            "table_two",
        ]

        query = (
            "SELECT one.pk_one_col, one.int_one_col, three.string_three_col FROM "
            + "(SELECT pk_one_col, int_one_col FROM table_one) AS one "
            + "INNER JOIN "
            + "(SELECT pk_one_col, string_three_col FROM table_three) AS three "
            + "ON one.pk_one_col = three.pk_one_col;"
        )
        rdb_mysql.store_query_result(query, "result_zero")
        assert rdb_mysql.query_store.get_table_names() == ["result_zero"]

        rdb_mysql.query_store.drop_table("result_zero")
        rdb_mysql.drop_table("table_three")
        rdb_mysql.drop_table("table_two")
        rdb_mysql.drop_table("table_one")
        assert rdb_mysql.rdb_type.get_table_names() == []
        assert rdb_mysql.query_store.get_table_names() == []

    def test_store_query_results(self, rdb_mysql, table_configs, query_csv_path):
        """Testing for RDB.store_query_results()"""

        assert rdb_mysql.rdb_type.get_table_names() == []
        assert rdb_mysql.query_store.get_table_names() == []

        rdb_mysql.update_all_database_tables(
            [["table_one"], ["table_two"], ["table_three"]], table_configs
        )
        assert rdb_mysql.rdb_type.get_table_names() == [
            "table_one",
            "table_three",
            "table_two",
        ]

        rdb_mysql.store_query_results(query_csv_path)
        assert rdb_mysql.query_store.get_table_names() == [
            "result_one",
            "result_two",
        ]

        rdb_mysql.query_store.drop_table("result_one")
        rdb_mysql.query_store.drop_table("result_two")
        rdb_mysql.drop_table("table_three")
        rdb_mysql.drop_table("table_two")
        rdb_mysql.drop_table("table_one")
        assert rdb_mysql.rdb_type.get_table_names() == []
        assert rdb_mysql.query_store.get_table_names() == []
