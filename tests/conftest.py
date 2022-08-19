"""Fixtures for all testsDBDatatype.TEXT
"""
import os
from datetime import datetime
import pytest
import pandas as pd
import numpy as np
from yaml import safe_load
from db_object_config import (
    DBObjectConfigList,
    DBObjectConfig,
    DBAttributeConfig,
    DBDatatype,
    DBForeignKey,
)
from rdb_type import MySQL, Synapse
from rdb import RDB

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_mysql_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "mysql_config.yml")


@pytest.fixture(scope="session", name="config_dict")
def fixture_config_dict():
    """
    Yields a MYSQL config dict
    """
    with open(CONFIG_PATH, mode="rt", encoding="utf-8") as file:
        config = safe_load(file)
    yield config


@pytest.fixture(scope="session", name="query_csv_path")
def fixture_query_csv_path():
    """
    Yields a MYSQL config dict
    """
    path = os.path.join(DATA_DIR, "queries.csv")
    yield path


@pytest.fixture(scope="session", name="mysql")
def fixture_mysql(config_dict):
    """
    Yields a MYSQL object
    """
    obj = MySQL(config_dict["database"])
    yield obj
    test_table_names = ["table_three", "table_one", "table_two"]
    for table_name in test_table_names:
        if table_name in obj.get_table_names():
            obj.drop_table(table_name)
    assert obj.get_table_names() == []


@pytest.fixture(scope="session", name="synapse_manifest_store")
def fixture_synapse_manifest_store(config_dict):
    """
    Yields a Synapse Manifest Store
    """
    obj = Synapse(config_dict["manifest_store"])
    yield obj


@pytest.fixture(scope="session", name="synapse_query_store")
def fixture_synapse_query_store(config_dict):
    """
    Yields a Synapse Query Store
    """
    obj = Synapse(config_dict["query_store"])
    assert obj.get_table_names() == []
    yield obj
    for name in obj.get_table_names():
        obj.drop_table(name)
    assert obj.get_table_names() == []


@pytest.fixture(scope="module", name="rdb_mysql")
def fixture_rdb_mysql(config_dict, synapse_manifest_store, synapse_query_store):
    """Yields a RDB object"""
    mysql = MySQL(config_dict["database"])
    rdb = RDB(
        rdb_type=mysql,
        manifest_store=synapse_manifest_store,
        query_store=synapse_query_store,
    )
    yield rdb


@pytest.fixture(scope="session")
def table_one():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_one_col": ["key1", "key2", "key3"],
            "string_one_col": ["a", "b", np.nan],
            "int_one_col": [1, pd.NA, 3],
            "double_one_col": [1.1, 2.2, np.nan],
            "date_one_col": [datetime(2022, 8, 2), np.nan, datetime(2022, 8, 2)],
            "bool_one_col": [pd.NA, True, False],
        }
    )
    dataframe = dataframe.astype({"int_one_col": "Int64", "bool_one_col": "boolean"})
    dataframe["date_one_col"] = pd.to_datetime(dataframe["date_one_col"]).dt.date
    yield dataframe


@pytest.fixture(scope="session", name="table_one_config")
def fixture_table_one_config():
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name="table_one",
        attributes=[
            DBAttributeConfig(name="pk_one_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="string_one_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="int_one_col", datatype=DBDatatype.INT),
            DBAttributeConfig(name="double_one_col", datatype=DBDatatype.FLOAT),
            DBAttributeConfig(name="date_one_col", datatype=DBDatatype.DATE),
            DBAttributeConfig(name="bool_one_col", datatype=DBDatatype.BOOLEAN),
        ],
        primary_keys=["pk_one_col"],
        foreign_keys=[],
    )
    yield table_config


@pytest.fixture(scope="session")
def table_two():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_two_col": ["key1", "key2", "key3", "key4"],
            "string_two_col": ["a", "b", "c", "d"],
        }
    )
    yield dataframe


@pytest.fixture(scope="session")
def table_two_b():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_two_col": ["key5", "key6", "key7", "key8"],
            "string_two_col": ["a", "b", "c", "d"],
        }
    )
    yield dataframe


@pytest.fixture(scope="session", name="table_two_config")
def fixture_table_two_config():
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name="table_two",
        attributes=[
            DBAttributeConfig(name="pk_two_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="string_two_col", datatype=DBDatatype.TEXT),
        ],
        primary_keys=["pk_two_col"],
        foreign_keys=[],
    )
    yield table_config


@pytest.fixture(scope="session")
def table_three():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_one_col": ["key1", "key1", "key2", "key2"],
            "pk_two_col": ["key1", "key2", "key1", "key2"],
            "string_three_col": ["a", "b", "c", "d"],
        }
    )
    yield dataframe


@pytest.fixture(scope="session", name="table_three_config")
def fixture_table_three_config():
    """
    Yields a DBObjectConfig object with two keys that are both primary and foreign
    """
    table_config = DBObjectConfig(
        name="table_three",
        attributes=[
            DBAttributeConfig(name="pk_one_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="pk_two_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="string_three_col", datatype=DBDatatype.TEXT),
        ],
        primary_keys=["pk_one_col", "pk_two_col"],
        foreign_keys=[
            DBForeignKey(
                name="pk_one_col",
                foreign_object_name="table_one",
                foreign_attribute_name="pk_one_col",
            ),
            DBForeignKey(
                name="pk_two_col",
                foreign_object_name="table_two",
                foreign_attribute_name="pk_two_col",
            ),
        ],
    )
    yield table_config


@pytest.fixture(scope="session")
def table_123_unormalized():
    """
    Yields a pd.Dataframe. This is what a merged table might look like.
    """
    dataframe = pd.DataFrame(
        {
            "pk_one_col": ["key1", "key1", "key2", "key2"],
            "pk_two_col": ["key1", "key2", "key1", "key2"],
            "string_three_col": ["a", "b", "c", "d"],
            "string_one_col": ["a", "a", "b", "a"],
        }
    )

    yield dataframe


@pytest.fixture(scope="session")
def table_configs(table_one_config, table_two_config, table_three_config):
    """Yields a DBObjectConfigList"""
    yield DBObjectConfigList([table_one_config, table_two_config, table_three_config])
