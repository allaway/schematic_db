"""Testing for RDBUpdater."""
from typing import Generator
import pytest
from schematic_db.rdb import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.schema import Schema


@pytest.fixture(scope="module", name="rdb_updater_mysql_test")
def fixture_rdb_updater_mysql_test(
    mysql: MySQLDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=mysql, schema=test_schema)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_updater_postgres_test")
def fixture_rdb_updater_postgres_test(
    postgres: PostgresDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=postgres, schema=test_schema)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_updater_synapse_test")
def fixture_rdb_updater_synapse_test(
    synapse_database: SynapseDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a synapse database and test schema"""
    obj = RDBUpdater(rdb=synapse_database, schema=test_schema)
    yield obj
    table_names = obj.rdb.get_table_names()
    for name in table_names:
        obj.rdb.delete_table(name)  # type: ignore


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
        """Creates the test database in Synapse"""
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
