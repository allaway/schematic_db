"""Testing for RDB.
"""


class TestRDBQueryer:
    """Testing for RDBQueryer"""

    def test_store_query_results(
        self, rdb_updater_mysql, rdb_queryer_mysql, table_configs, query_csv_path
    ):
        """Testing for RDBQueryer.store_query_results()"""

        assert rdb_queryer_mysql.rdb.get_table_names() == []
        assert rdb_queryer_mysql.query_store.synapse.get_table_names() == []

        rdb_updater_mysql.update_all_database_tables(
            [["table_one"], ["table_two"], ["table_three"]], table_configs
        )
        assert rdb_queryer_mysql.rdb.get_table_names() == [
            "table_one",
            "table_three",
            "table_two",
        ]

        rdb_queryer_mysql.store_query_results(query_csv_path)
        assert rdb_queryer_mysql.query_store.synapse.get_table_names() == [
            "result_one",
            "result_two",
        ]

        rdb_queryer_mysql.query_store.synapse.drop_table("result_one")
        rdb_queryer_mysql.query_store.synapse.drop_table("result_two")
        rdb_queryer_mysql.rdb.drop_table("table_three")
        rdb_queryer_mysql.rdb.drop_table("table_two")
        rdb_queryer_mysql.rdb.drop_table("table_one")
        assert rdb_queryer_mysql.rdb.get_table_names() == []
        assert rdb_queryer_mysql.query_store.synapse.get_table_names() == []
