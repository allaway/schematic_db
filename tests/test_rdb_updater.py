"""Testing for RDBUpdater."""
from typing import Generator
import pytest
from schematic_db.rdb import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.rdb_builder import RDBBuilder
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.schema import Schema
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.manifest_store import ManifestStore, ManifestStoreConfig


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


@pytest.fixture(scope="module", name="manifest_store")
def fixture_manifest_store(
    test_synapse_project_id: str,
    test_synapse_asset_view_id: str,
    secrets_dict: dict,
    test_schema_json_url: str,
) -> Generator:
    """Yields a ManifestStore object"""
    yield ManifestStore(
        ManifestStoreConfig(
            test_schema_json_url,
            test_synapse_project_id,
            test_synapse_asset_view_id,
            secrets_dict["synapse"]["auth_token"],
        )
    )


@pytest.fixture(scope="module", name="rdb_updater_mysql_test")
def fixture_rdb_updater_mysql_test(
    mysql_database: MySQLDatabase, manifest_store: ManifestStore
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=mysql_database, manifest_store=manifest_store)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_updater_postgres_test")
def fixture_rdb_updater_postgres_test(
    postgres_database: PostgresDatabase, manifest_store: ManifestStore
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=postgres_database, manifest_store=manifest_store)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_updater_synapse_test")
def fixture_rdb_updater_synapse_test(
    synapse_database: SynapseDatabase, manifest_store: ManifestStore
) -> Generator:
    """Yields a RDBUpdater with a synapse database and test schema"""
    obj = RDBUpdater(rdb=synapse_database, manifest_store=manifest_store)
    yield obj
    synapse_database.delete_all_tables()


@pytest.mark.schematic
class TestRDBBuilderTestSchema:
    """Testing for RDBBuilder and test schema"""

    def test_mysql_update_all_database_tables(
        self,
        rdb_updater_mysql_test: RDBUpdater,
        rdb_builder_mysql_test: RDBBuilder,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in MySQL"""
        rdb_builder = rdb_builder_mysql_test
        assert rdb_builder.rdb.get_table_names() == []
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_mysql_test
        rdb_updater.update_database()
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0

    def test_postgres_update_all_database_tables(
        self,
        rdb_updater_postgres_test: RDBUpdater,
        rdb_builder_postgres_test: RDBBuilder,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Postgres"""
        rdb_builder = rdb_builder_postgres_test
        assert rdb_builder.rdb.get_table_names() == []
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_postgres_test
        rdb_updater.update_database()
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0

    def test_synapse_update_all_database_tables(
        self,
        rdb_updater_synapse_test: RDBUpdater,
        rdb_builder_synapse_test: RDBBuilder,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Synapse"""
        rdb_builder = rdb_builder_synapse_test
        assert rdb_builder.rdb.get_table_names() == []
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_synapse_test
        rdb_updater.update_database()
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0
