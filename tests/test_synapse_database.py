"""Fixtures for all tests"""
from __future__ import annotations
from typing import Generator
import pytest
from schematic_db.rdb.synapse import SynapseDatabase
from schematic_db.db_config.db_config import DBObjectConfig


@pytest.fixture(name="synapse_no_extra_tables")
def fixture_synapse_no_extra_tables(
    synapse_database: SynapseDatabase, synapse_database_table_names: list[str]
) -> Generator:
    """
    Yields a SynapseDatabase object
    """
    obj = synapse_database
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        if name not in synapse_database_table_names:
            obj.synapse.delete_table(name)


@pytest.fixture(name="synapse_with_empty_table_one")
def fixture_synapse_with_empty_table_one(
    synapse_no_extra_tables: SynapseDatabase, table_one_config: DBObjectConfig
) -> Generator:
    """
    Yields a SynapseDatabase object with table one added
    """
    obj = synapse_no_extra_tables
    obj.synapse.add_table("table_one", table_one_config)
    yield obj


@pytest.mark.synapse
class TestSynapseDatabase:  # pylint: disable=too-few-public-methods
    """Testing for SynapseDatabase"""

    def test_annotate_table(
        self,
        synapse_with_empty_table_one: SynapseDatabase,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for SynapseDatabase.annotate_table()"""
        obj = synapse_with_empty_table_one
        synapse_id = obj.synapse.get_synapse_id_from_table_name("table_one")
        annotations = obj.synapse.get_entity_annotations(synapse_id)
        assert annotations == {}

        obj.annotate_table("table_one", table_one_config)
        annotations2 = obj.synapse.get_entity_annotations(synapse_id)
        assert annotations2 == {"primary_key": ["pk_one_col"]}
