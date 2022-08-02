"""
Testing for Synapse.

For testing locally there should be a synapse server running.
A config yaml should exist at 'tests/data/local_synapse_config.yml'.
This config should look like:
username: "root"
password: "root"
host: "localhost"
schema: "test_schema"
The schema should not exist on the database at the beginning of the test.
This file is ignored by git.

If the the config doesn't exist, the file at 'tests/data/synapse_config.yml'
will be used.
"""
# pylint: disable=W0621
# pylint: disable=E0401
# pylint: disable=W0212
# pylint: disable=E0611
import os
import pytest # type: ignore
import pandas as pd # type: ignore
from rdb_type import Synapse
from db_object_config import DBObjectConfig, DBAttributeConfig, DBDatatype


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_synapse_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "synapse_config.yml")


@pytest.fixture
def synapse():
    """
    Yields a Synapse object
    """
    obj = Synapse(config_yaml_path=CONFIG_PATH)
    yield obj

@pytest.fixture
def table_config_no_keys():
    """
    Yields a DBObjectConfig object with no primary or foreign keys
    """
    table_config = DBObjectConfig(
        name = "test_table1",
        attributes = [
            DBAttributeConfig(name="string_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="int_col", datatype=DBDatatype.Int)
        ],
        primary_keys = [],
        foreign_keys = []
    )
    yield table_config

@pytest.fixture
def table_config_one_primary_key():
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name = "test_table2",
        attributes = [
            DBAttributeConfig(name="p_key_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="string_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="int_col", datatype=DBDatatype.Int)
        ],
        primary_keys = ["p_key_col"],
        foreign_keys = []
    )
    yield table_config

@pytest.fixture
def rows_df():
    """
    Yields a pd.Dataframe.
    These are used as inputs to update and upsert methods.
    """
    dataframe = pd.DataFrame({
        "string_col": ["a","b","c"],
        "int_col": [1,2,3]
    })
    yield dataframe

class TestSynapse:
    """
    Testing for synapse
    """
    def test_init(self, synapse):
        """Testing for Synapse.init()
        """
        assert synapse.project.properties.name == "Schematic DB Test Database"

    def test_get_add_drop_table(self, synapse, table_config_no_keys):
        """Testing for Synapse.add_table(), and Synapse.drop_table()
        """
        assert synapse.get_table_names() == []
        synapse.add_table("test_table1", table_config_no_keys)
        assert synapse.get_table_names() == ['test_table1']
        synapse.drop_table('test_table1')
        assert synapse.get_table_names() == []

    def test_add_table_column(self, synapse, table_config_no_keys):
        """Testing for synapse.add_table_column()
        """
        assert synapse.get_table_names() == []
        synapse.add_table("test_table1", table_config_no_keys)
        synapse.get_column_names_from_table("test_table1")
        assert synapse.get_column_names_from_table("test_table1") == ["string_col", "int_col"]
        synapse.add_table_column("test_table1", "float_col", DBDatatype.Float)
        assert synapse.get_column_names_from_table("test_table1") == \
            ["string_col", "int_col", "float_col"]
        synapse.drop_table('test_table1')
        assert synapse.get_table_names() == []

    def test_drop_table_column(self, synapse, table_config_no_keys):
        """Testing for synapse.drop_table_column()
        """
        assert synapse.get_table_names() == []
        synapse.add_table("test_table1", table_config_no_keys)
        synapse.get_column_names_from_table("test_table1")
        assert synapse.get_column_names_from_table("test_table1") == ["string_col", "int_col"]
        synapse.drop_table_column("test_table1", "string_col")
        assert synapse.get_column_names_from_table("test_table1") == ["int_col"]
        synapse.drop_table('test_table1')
        assert synapse.get_table_names() == []

    def test_execute_sql_query(self, synapse, table_config_no_keys):
        """Testing for synapse.execute_sql_query()
        """
        synapse.add_table("test_table1", table_config_no_keys)
        table_id = synapse._get_table_synapse_id("test_table1")
        result = synapse.execute_sql_query("SELECT * FROM " + table_id)
        assert len(result.index) == 0
        synapse.drop_table('test_table1')

    def test_insert_delete_table_rows(self, synapse, table_config_no_keys, rows_df):
        """
        Testing for synapse.insert_table_rows() and synapse.delete_table_rows()
        """
        assert synapse.get_table_names() == []
        synapse.add_table("test_table1", table_config_no_keys)
        synapse.insert_table_rows("test_table1", rows_df)
        table_id = synapse._get_table_synapse_id("test_table1")
        result1 = synapse.execute_sql_query("SELECT * FROM " + table_id, False)
        pd.testing.assert_frame_equal(result1, rows_df)
        synapse.drop_table('test_table1')
        assert synapse.get_table_names() == []
