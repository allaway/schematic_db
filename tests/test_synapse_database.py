"""Fixtures for all tests"""
from __future__ import annotations
from typing import Generator
import pytest
import pandas as pd
from schematic_db.rdb.synapse import SynapseDatabase, SynapseDatabaseDropTableError
from schematic_db.db_config.db_config import DBObjectConfig


@pytest.fixture(name="synapse_database")
def fixture_synapse_no_extra_tables(synapse_database: SynapseDatabase) -> Generator:
    """Yields a SynapseDatabase object"""
    obj = synapse_database
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        obj.synapse.delete_table(name)


@pytest.fixture(name="synapse_with_empty_tables")
def fixture_synapse_with_empty_tables(
    synapse_database: SynapseDatabase,
    table_one_config: DBObjectConfig,
    table_two_config: DBObjectConfig,
    table_three_config: DBObjectConfig,
) -> Generator:
    """Yields a SynapseDatabase object with tables added"""
    obj = synapse_database
    obj.synapse.add_table("table_one", table_one_config)
    obj.synapse.add_table("table_two", table_two_config)
    obj.synapse.add_table("table_three", table_three_config)
    obj.annotate_table("table_one", table_one_config)
    obj.annotate_table("table_two", table_two_config)
    obj.annotate_table("table_three", table_three_config)
    yield obj


@pytest.fixture(name="synapse_with_filled_tables")
def fixture_synapse_with_filled_table_one(
    synapse_with_empty_tables: SynapseDatabase,
    table_one: pd.DataFrame,
    table_two: pd.DataFrame,
    table_three: pd.DataFrame,
) -> Generator:
    """Yields a SynapseDatabase object with tables added and filled"""
    obj = synapse_with_empty_tables
    obj.synapse.insert_table_rows("table_one", table_one)
    obj.synapse.insert_table_rows("table_two", table_two)
    obj.synapse.insert_table_rows("table_three", table_three)
    yield obj


@pytest.mark.synapse
class TestSynapseDatabase:
    """Testing for SynapseDatabase"""

    def test_drop_all_tables(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.drop_all_tables()"""
        obj = synapse_with_empty_tables
        synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
        synapse_id2 = obj.synapse.get_synapse_id_from_table_name("table_two")
        synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")

        annos1a = obj.synapse.get_entity_annotations(synapse_id1)
        annos2a = obj.synapse.get_entity_annotations(synapse_id2)
        annos3a = obj.synapse.get_entity_annotations(synapse_id3)
        assert list(annos1a.keys()) == ["attributes", "primary_key"]
        assert list(annos2a.keys()) == ["attributes", "primary_key"]
        assert list(annos3a.keys()) == [
            "attributes",
            "primary_key",
            "foreign_keys",
        ]

        obj.drop_all_tables()
        annos1b = obj.synapse.get_entity_annotations(synapse_id1)
        annos2b = obj.synapse.get_entity_annotations(synapse_id2)
        annos3b = obj.synapse.get_entity_annotations(synapse_id3)
        assert not list(annos1b.keys())
        assert not list(annos2b.keys())
        assert not list(annos3b.keys())

    def test_drop_table_and_dependencies(
        self, synapse_with_empty_tables: SynapseDatabase
    ) -> None:
        """Testing for SynapseDatabase.drop_table_and_dependencies()"""
        obj = synapse_with_empty_tables
        synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
        synapse_id2 = obj.synapse.get_synapse_id_from_table_name("table_two")
        synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")

        annos1a = obj.synapse.get_entity_annotations(synapse_id1)
        annos2a = obj.synapse.get_entity_annotations(synapse_id2)
        annos3a = obj.synapse.get_entity_annotations(synapse_id3)
        assert list(annos1a.keys()) == ["attributes", "primary_key"]
        assert list(annos2a.keys()) == ["attributes", "primary_key"]
        assert list(annos3a.keys()) == [
            "attributes",
            "primary_key",
            "foreign_keys",
        ]

        obj.drop_table_and_dependencies("table_one")
        annos1b = obj.synapse.get_entity_annotations(synapse_id1)
        annos2b = obj.synapse.get_entity_annotations(synapse_id2)
        annos3b = obj.synapse.get_entity_annotations(synapse_id3)
        assert not list(annos1b.keys())
        assert list(annos2b.keys()) == ["attributes", "primary_key"]
        assert not list(annos3b.keys())

    def test_drop_table(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.drop_table()"""
        obj = synapse_with_empty_tables
        with pytest.raises(
            SynapseDatabaseDropTableError,
            match="Can not drop database table, other tables exists that depend on it",
        ):
            obj.drop_table("table_one")

        synapse_id = obj.synapse.get_synapse_id_from_table_name("table_three")
        annos1 = obj.synapse.get_entity_annotations(synapse_id)
        assert list(annos1.keys()) == [
            "attributes",
            "primary_key",
            "foreign_keys",
        ]
        obj.drop_table("table_three")
        annos2 = obj.synapse.get_entity_annotations(synapse_id)
        assert not list(annos2.keys())

    def test_annotate_table(
        self,
        synapse_database: SynapseDatabase,
        table_one_config: DBObjectConfig,
        table_three_config: DBObjectConfig,
    ) -> None:
        """Testing for SynapseDatabase.annotate_table()"""
        obj = synapse_database
        assert obj.get_table_names() == []

        obj.synapse.add_table("table_one", table_one_config)
        obj.synapse.add_table("table_three", table_three_config)

        synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
        annotations = obj.synapse.get_entity_annotations(synapse_id1)
        assert annotations == {}

        obj.annotate_table("table_one", table_one_config)
        annotations2 = obj.synapse.get_entity_annotations(synapse_id1)
        assert list(annotations2.keys()) == ["attributes", "primary_key"]

        synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")
        annotations3 = obj.synapse.get_entity_annotations(synapse_id3)
        assert annotations3 == {}

        obj.annotate_table("table_three", table_three_config)
        annotations4 = obj.synapse.get_entity_annotations(synapse_id3)
        assert list(annotations4.keys()) == [
            "attributes",
            "primary_key",
            "foreign_keys",
        ]

    def get_db_config(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.get_db_config()"""
        obj = synapse_with_empty_tables
        db_config = obj.get_db_config()
        assert db_config.get_config_names == [
            "table_one",
            "table_three",
            "table_two",
            "test_table_one",
        ]

    def test_get_table_config(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.get_table_config()"""
        obj = synapse_with_empty_tables
        table_config1 = obj.get_table_config("table_one")
        assert table_config1.name == "table_one"
        assert table_config1.primary_key == "pk_one_col"
        assert table_config1.foreign_keys == []

        table_config3 = obj.get_table_config("table_three")
        assert table_config3.name == "table_three"
        assert table_config3.primary_key == "pk_zero_col"
        assert table_config3.foreign_keys != []

    def test_delete_table_rows(
        self,
        synapse_with_filled_tables: SynapseDatabase,
        table_three_config: DBObjectConfig,
    ) -> None:
        """Testing for SynapseDatabase.delete_table_rows()"""
        obj = synapse_with_filled_tables
        synapse_id = obj.synapse.get_synapse_id_from_table_name("table_three")
        query = f"SELECT {table_three_config.primary_key} FROM {synapse_id}"
        table = obj.execute_sql_query(query)
        assert table["pk_zero_col"].tolist() == ["keyA", "keyB", "keyC", "keyD"]

        obj.delete_table_rows("table_three", table.iloc[[0]], table_three_config)
        table2 = obj.execute_sql_query(query)
        assert table2["pk_zero_col"].tolist() == ["keyB", "keyC", "keyD"]

    def test_delete_table_rows2(
        self,
        synapse_with_filled_tables: SynapseDatabase,
        table_one_config: DBObjectConfig,
        table_three_config: DBObjectConfig,
    ) -> None:
        """Testing for SynapseDatabase.delete_table_rows()"""
        obj = synapse_with_filled_tables
        table1a = obj.synapse.query_table("table_one", table_one_config)
        table3a = obj.synapse.query_table("table_three", table_three_config)
        assert table1a["pk_one_col"].tolist() == ["key1", "key2", "key3"]
        assert table3a["pk_zero_col"].tolist() == ["keyA", "keyB", "keyC", "keyD"]

        obj.delete_table_rows("table_one", table1a.iloc[[2]], table_one_config)
        table1b = obj.synapse.query_table("table_one", table_one_config)
        table3b = obj.synapse.query_table("table_three", table_three_config)
        assert table1b["pk_one_col"].tolist() == ["key1", "key2"]
        assert table3b["pk_zero_col"].tolist() == ["keyA", "keyB", "keyC", "keyD"]

        obj.delete_table_rows("table_one", table1a.iloc[[0]], table_one_config)
        table1b = obj.synapse.query_table("table_one", table_one_config)
        table3b = obj.synapse.query_table("table_three", table_three_config)
        assert table1b["pk_one_col"].tolist() == ["key2"]
        assert table3b["pk_zero_col"].tolist() == ["keyC", "keyD"]

    def test_create_primary_key_table(
        self,
        synapse_with_filled_tables: SynapseDatabase,
    ) -> None:
        """Testing for SynapseDatabase._create_primary_key_table()"""
        obj = synapse_with_filled_tables
        synapse_id = obj.synapse.get_synapse_id_from_table_name("table_one")
        table = obj._create_primary_key_table(  # pylint: disable=protected-access
            synapse_id, "pk_one_col"
        )
        assert list(table.columns) == ["ROW_ID", "ROW_VERSION", "pk_one_col"]
