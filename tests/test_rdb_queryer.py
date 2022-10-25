"""Testing for Testing for RDBQueryer."""


class FutureTestRDBQueryer:  # pylint: disable=too-few-public-methods
    """Testing for RDBQueryer"""

    def test_store_query_results(self, rdb_queryer_mysql_gff, gff_query_csv_path):
        """Testing for RDBQueryer.store_query_results()"""
        rdb_queryer_mysql_gff.store_query_results(gff_query_csv_path)
