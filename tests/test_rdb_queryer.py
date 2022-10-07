"""Testing for Testing for RDBQueryer."""

# TODO: Turn on tests when api is public
class FutureTestRDBQueryer:
    """Testing for RDBQueryer"""

    def test_store_query_results(self, rdb_queryer_mysql_gff, gff_query_csv_path):
        """Testing for RDBQueryer.store_query_results()"""
        rdb_queryer_mysql_gff.store_query_results(gff_query_csv_path)
