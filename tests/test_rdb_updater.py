"""Testing for RDBUpdater."""
import pytest
from schematic_db.rdb_updater import RDBUpdater


@pytest.mark.schematic
class TestRDBUpdaterTestSchema:
    """Testing for RDBUpdater and test schema"""

    def test_mysql_update_all_database_tables(
        self, rdb_updater_mysql_test: RDBUpdater, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in MySQL"""
        obj = rdb_updater_mysql_test
        assert obj.rdb.get_table_names() == []
        obj.update_all_database_tables()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.update_all_database_tables(replace_tables=True)
        assert obj.rdb.get_table_names() == test_schema_table_names

    def test_postgres_update_all_database_tables(
        self, rdb_updater_postgres_test: RDBUpdater, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in Postgres"""
        obj = rdb_updater_postgres_test
        assert obj.rdb.get_table_names() == []
        obj.update_all_database_tables()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.update_all_database_tables(replace_tables=True)
        assert obj.rdb.get_table_names() == test_schema_table_names

    def test_synapse_update_all_database_tables(
        self, rdb_updater_synapse_test: RDBUpdater, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in Postgres"""
        obj = rdb_updater_synapse_test
        assert obj.rdb.get_table_names() == []
        obj.update_all_database_tables()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.update_all_database_tables(replace_tables=True)
        assert obj.rdb.get_table_names() == test_schema_table_names


@pytest.mark.gff
@pytest.mark.schematic
class TestRDBUpdaterMySQL:
    """Testing for RDB with MySQL database"""

    def test_update_all_database_tables(
        self, rdb_updater_mysql_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Creates the gff database in MySQL"""
        assert rdb_updater_mysql_gff.rdb.get_table_names() == gff_database_table_names

    def test_update_all_database_tables2(
        self, rdb_updater_mysql_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Recreates the gff database in MySQL"""
        obj = rdb_updater_mysql_gff
        obj.update_all_database_tables(replace_tables=True)
        assert obj.rdb.get_table_names() == gff_database_table_names


@pytest.mark.gff
@pytest.mark.schematic
@pytest.mark.synapse
class TestRDBUpdaterSynapse:
    """Testing for RDB with Synapse database"""

    def test_update_all_database_tables(
        self, rdb_updater_synapse_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Creates the gff database in MySQL"""
        assert rdb_updater_synapse_gff.rdb.get_table_names() == gff_database_table_names

    def test_update_all_database_tables2(
        self, rdb_updater_synapse_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Creates the gff database in MySQL"""
        obj = rdb_updater_synapse_gff
        obj.update_all_database_tables(replace_tables=True)
        assert obj.rdb.get_table_names() == gff_database_table_names
