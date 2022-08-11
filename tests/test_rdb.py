"""Testing for RDB.
"""
# pylint: disable=redefined-outer-name
# pylint: disable=too-many-arguments
# pylint: disable=W0212
# pylint: disable=E0401
import os
import pytest # type: ignore
import pandas as pd # type: ignore
from rdb import RDB

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_mysql_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "mysql_config.yml")

@pytest.fixture(scope = 'module')
def rdb_mysql():
    """Yields a RDB object
    """
    rdb = RDB(config_yaml_path = CONFIG_PATH)
    assert rdb.rdb_type.get_table_names() == []
    yield rdb
    test_table_names = ["table_three", "table_one", "table_two"]
    for table_name in test_table_names:
        if table_name in rdb.rdb_type.get_table_names():
            rdb.rdb_type.drop_table(table_name)
    for table_name in rdb.query_result_store.get_table_names():
        rdb.query_result_store.drop_table(table_name)

class TestRDBMySQL:
    """Testing for RDB with MySQL database
    """
    def test_init(self, rdb_mysql):
        """Testing for RDB.init()
        """
        assert isinstance(rdb_mysql, RDB)

    def test_update_database_table(
        self, rdb_mysql,
        table_one, table_one_config,
        table_two, table_two_config,
        table_three, table_three_config
    ):
        """Testing for RDB.update_database_table()
        """
        assert rdb_mysql.rdb_type.get_table_names() == []

        rdb_mysql.update_database_table("table_one", table_one_config)
        result = rdb_mysql.rdb_type.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result, table_one)

        rdb_mysql.update_database_table("table_two", table_two_config)
        result = rdb_mysql.rdb_type.query_table("table_two", table_two_config)
        pd.testing.assert_frame_equal(result, table_two)

        rdb_mysql.update_database_table("table_three", table_three_config)
        result = rdb_mysql.rdb_type.query_table("table_three", table_three_config)
        pd.testing.assert_frame_equal(result, table_three)

    def test_store_query_result(self, rdb_mysql):
        """Testing for RDB.store_query_result()
        """
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_three", "table_two"]
        assert rdb_mysql.query_result_store.get_table_names() == []

        query = (
            "SELECT * FROM " +\
            "(SELECT pk_col, int_col FROM table_one) AS one " +\
            "INNER JOIN " +\
            "(SELECT fk1_col, string_col FROM table_three) AS three "+\
            "ON one.pk_col = three.fk1_col;"
        )

        rdb_mysql.store_query_result(query, "result_zero")
        assert rdb_mysql.query_result_store.get_table_names() == ["result_zero"]

    def test_store_query_results(self, rdb_mysql):
        """Testing for RDB.store_query_results()
        """
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_three", "table_two"]
        assert rdb_mysql.query_result_store.get_table_names() == ["result_zero"]

        csv_path = os.path.join(DATA_DIR, "queries.csv")
        rdb_mysql.store_query_results(csv_path)
        assert rdb_mysql.query_result_store.get_table_names() == [
            "result_one", "result_two", "result_zero"
        ]
