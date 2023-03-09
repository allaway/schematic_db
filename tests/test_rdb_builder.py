"""Testing for RDBBuilder."""
from typing import Generator
import pytest
from schematic_db.rdb import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.rdb_builder import RDBBuilder
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.schema import Schema


@pytest.fixture(scope="module", name="rdb_builder_mysql_test")
def fixture_rdb_builder_mysql_test(
    mysql_database: MySQLDatabase, test_schema2: Schema
) -> Generator:
    """Yields a RDBBuilder with a mysql database and test schema"""
    obj = RDBBuilder(rdb=mysql_database, schema=test_schema2)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_builder_postgres_test")
def fixture_rdb_builder_postgres_test(
    postgres_database: PostgresDatabase, test_schema2: Schema
) -> Generator:
    """Yields a RDBBuilder with a mysql database and test schema"""
    obj = RDBBuilder(rdb=postgres_database, schema=test_schema2)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_builder_synapse_test")
def fixture_rdb_builder_synapse_test(
    synapse_database: SynapseDatabase, test_schema2: Schema
) -> Generator:
    """Yields a RDBBuilder with a synapse database and test schema"""
    obj = RDBBuilder(rdb=synapse_database, schema=test_schema2)
    yield obj
    synapse_database.delete_all_tables()


@pytest.mark.schematic
class TestRDBBuilderTestSchema:
    """Testing for RDBBuilder and test schema"""

    def test_mysql_update_all_database_tables(
        self, rdb_builder_mysql_test: RDBBuilder, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in MySQL"""
        obj = rdb_builder_mysql_test
        assert obj.rdb.get_table_names() == []
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names

    def test_postgres_update_all_database_tables(
        self, rdb_builder_postgres_test: RDBBuilder, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in Postgres"""
        obj = rdb_builder_postgres_test
        assert obj.rdb.get_table_names() == []
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names

    def test_synapse_update_all_database_tables(
        self, rdb_builder_synapse_test: RDBBuilder, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in Synapse"""
        obj = rdb_builder_synapse_test
        assert obj.rdb.get_table_names() == []
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
