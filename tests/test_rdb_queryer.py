"""Testing for Testing for RDBQueryer."""
from rdb_queryer import RDBQueryer


class FutureTestRDBQueryer:  # pylint: disable=too-few-public-methods
    """Testing for RDBQueryer"""

    def test_store_query_results(
        self, rdb_queryer_mysql_gff: RDBQueryer, gff_query_csv_path: str
    ) -> None:
        """Testing for RDBQueryer.store_query_results()"""
        rdb_queryer_mysql_gff.store_query_results(gff_query_csv_path)
