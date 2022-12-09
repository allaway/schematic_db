"""Testing for Testing for RDBQueryer."""
from typing import Generator
import pytest
from schematic_db.rdb_queryer import RDBQueryer
from schematic_db.query_store import QueryStore
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.rdb import MySQLDatabase
from schematic_db.schema import Schema


@pytest.fixture(scope="module", name="rdb_updater_mysql_test")
def fixture_rdb_updater_mysql_test(
    mysql: MySQLDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=mysql, schema=test_schema)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="mysql_test_rdb_queryer")
def fixture_mysql_test_rdb_queryer(
    synapse_test_query_store: QueryStore,
    mysql: MySQLDatabase,
    test_schema: Schema,
    test_schema_table_names: list[str],
) -> Generator:
    """Yields a RDBQueryer with a mysql database with test schema tables added"""
    updater = RDBUpdater(rdb=mysql, schema=test_schema)
    updater.update_all_database_tables()
    assert updater.rdb.get_table_names() == test_schema_table_names

    store = synapse_test_query_store
    assert store.synapse.get_table_names() == []  # type: ignore

    obj = RDBQueryer(
        rdb=updater.rdb,
        query_store=store,
    )
    yield obj

    updater.rdb.drop_all_tables()
    assert updater.rdb.get_table_names() == []
    for table_name in store.synapse.get_table_names():  # type: ignore
        table_id = store.synapse.get_synapse_id_from_table_name(  # type: ignore
            table_name
        )
        store.synapse.delete_table(table_id)  # type: ignore
    assert store.synapse.get_table_names() == []  # type: ignore


@pytest.mark.synapse
@pytest.mark.schematic
class TestRDBQueryerTest:  # pylint: disable=too-few-public-methods
    """Testing for RDBQueryer using the test schema database"""

    def test_store_query_results(
        self,
        mysql_test_rdb_queryer: RDBQueryer,
        test_query_csv_path: str,
        test_schema_table_names: list[str],
    ) -> None:
        """Testing for RDBQueryer.store_query_results()"""
        obj = mysql_test_rdb_queryer
        assert obj.query_store.synapse.get_table_names() == []  # type: ignore
        obj.store_query_results(test_query_csv_path)
        assert obj.query_store.synapse.get_table_names() == test_schema_table_names  # type: ignore
