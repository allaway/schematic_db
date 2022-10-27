"""Testing for RDBUpdater."""
import pytest
from schematic_db.rdb_updater import RDBUpdater


@pytest.mark.schematic
class TestRDBUpdater:
    """Testing for RDB with MySQL database"""

    def test_init(self, rdb_updater_mysql_gff: RDBUpdater) -> None:
        """Testing for RDB.init()"""
        assert isinstance(rdb_updater_mysql_gff, RDBUpdater)

    def test_update_all_database_tables(
        self, rdb_updater_mysql_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Testing for update_all_database_tables()"""
        assert rdb_updater_mysql_gff.rdb.get_table_names() == gff_database_table_names

    def test_update_all_database_tables2(
        self, rdb_updater_mysql_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Testing for update_all_database_tables()"""
        rdb_updater_mysql_gff.update_all_database_tables(replace_tables=True)
        assert rdb_updater_mysql_gff.rdb.get_table_names() == gff_database_table_names
