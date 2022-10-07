"""Fixtures for all testsDBDatatype.TEXT
"""
import os
from datetime import datetime
import pytest
import pandas as pd
import numpy as np
from yaml import safe_load
from db_object_config import (
    DBConfig,
    DBObjectConfig,
    DBAttributeConfig,
    DBDatatype,
    DBForeignKey,
)
from rdb import MySQLDatabase, SynapseDatabase
from rdb_updater import RDBUpdater
from rbd_queryer import RDBQueryer
from query_store import SynapseQueryStore
from synapse import Synapse
from schema import Schema

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_mysql_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "mysql_config.yml")
SYNAPSE_CONFIG_PATH = os.path.join(DATA_DIR, "local_synapse_config.yml")
if not os.path.exists(SYNAPSE_CONFIG_PATH):
    SYNAPSE_CONFIG_PATH = os.path.join(DATA_DIR, "synapse_config.yml")
SECRETS_PATH = os.path.join(DATA_DIR, "local_secrets.yml")
if not os.path.exists(SECRETS_PATH):
    SECRETS_PATH = os.path.join(DATA_DIR, "secrets.yml")

# files -----------------------------------------------------------------------
@pytest.fixture(scope="session", name="secrets_dict")
def fixture_secrets_dict():
    """
    Yields a dict with various secrets, either locally or from a github action
    """
    with open(SECRETS_PATH, mode="rt", encoding="utf-8") as file:
        config = safe_load(file)
    yield config


@pytest.fixture(scope="session", name="config_dict")
def fixture_config_dict():
    """
    Yields a MYSQL config dict
    """
    with open(CONFIG_PATH, mode="rt", encoding="utf-8") as file:
        config = safe_load(file)
    yield config


@pytest.fixture(scope="session", name="synapse_config_dict")
def fixture_synapse_config_dict():
    """
    Yields a Synapse config dict
    """
    with open(SYNAPSE_CONFIG_PATH, mode="rt", encoding="utf-8") as file:
        config = safe_load(file)
    yield config


@pytest.fixture(scope="session", name="query_csv_path")
def fixture_query_csv_path():
    """Yields a path to a file of test SQL queries"""
    path = os.path.join(DATA_DIR, "queries.csv")
    yield path


@pytest.fixture(scope="session", name="gff_query_csv_path")
def fixture_gff_query_csv_path():
    """Yields a path to a file of test SQL queries for gff"""
    path = os.path.join(DATA_DIR, "gff_queries.csv")
    yield path


# schema objects --------------------------------------------------------------


@pytest.fixture(scope="session", name="synapse_input_token")
def fixture_synapse_input_token(secrets_dict):
    """Yields a synapse token"""
    yield secrets_dict["synapse"]["auth_token"]


@pytest.fixture(scope="session", name="gff_synapse_project_id")
def fixture_gff_synapse_project_id():
    """Yields the synapse id for the gff schema project id"""
    yield "syn38296792"


@pytest.fixture(scope="session", name="gff_synapse_asset_view_id")
def fixture_gff_synapse_asset_view_id():
    """Yields the synapse id for the gff schema project id"""
    yield "syn38308526"


# gff database objects --------------------------------------------------------


@pytest.fixture(scope="session", name="gff_mysql")
def fixture_gff_mysql():
    """
    Yields a MYSQL object with database named 'gff_test_schema'
    """
    obj = MySQLDatabase(
        {
            "username": "root",
            "password": "pass",
            "host": "localhost",
            "schema": "gff_test_schema",
        }
    )
    yield obj
    table_names = [
        "Usage",
        "Biobank",
        "VendorItem",
        "Observation",
        "ResourceApplication",
        "Mutation",
        "Development",
        "Vendor",
        "MutationDetails",
        "Resource",
        "Investigator",
        "Publication",
        "Funder",
        "GeneticReagent",
        "Antibody",
        "CellLine",
        "AnimalModel",
        "Donor",
    ]
    for table_name in table_names:
        if table_name in obj.get_table_names():
            obj.drop_table(table_name)
    assert obj.get_table_names() == []


@pytest.fixture(scope="session", name="gff_schema")
def fixture_gff_schema(
    gff_synapse_project_id, gff_synapse_asset_view_id, synapse_input_token
):
    """Yields a Schema using the GFF tools schema"""
    schema_url = (
        "https://raw.githubusercontent.com/nf-osi/"
        "nf-research-tools-schema/main/nf-research-tools.jsonld"
    )
    obj = Schema(
        schema_url,
        gff_synapse_project_id,
        gff_synapse_asset_view_id,
        synapse_input_token,
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_updater_mysql_gff")
def fixture_rdb_updater_mysql_gff(gff_mysql, gff_schema):
    """Yields a RDBUpdater with a mysql database and gff schema with tables added"""
    obj = RDBUpdater(rdb=gff_mysql, schema=gff_schema)
    obj.update_all_database_tables()
    yield obj


@pytest.fixture(scope="session", name="synapse_gff_query_store")
def fixture_synapse_gff_query_store(secrets_dict):
    """
    Yields a Synapse Query Store for gff
    """
    obj = SynapseQueryStore(
        {
            "project_id": "syn39024404",
            "username": secrets_dict.get("synapse").get("username"),
            "auth_token": secrets_dict.get("synapse").get("auth_token"),
        }
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_queryer_mysql_gff")
def fixture_rdb_queryer_mysql_gff(rdb_updater_mysql_gff, synapse_gff_query_store):
    """Yields a RDBQueryer with a mysql database with gff tables added"""
    obj = RDBQueryer(
        rdb=rdb_updater_mysql_gff.rdb,
        query_store=synapse_gff_query_store,
    )
    yield obj


# database objects ------------------------------------------------------------
@pytest.fixture(scope="session", name="mysql")
def fixture_mysql():
    """
    Yields a MYSQL object
    """
    obj = MySQLDatabase(
        {
            "username": "root",
            "password": "pass",
            "host": "localhost",
            "schema": "test_schema",
        }
    )
    yield obj
    test_table_names = ["table_three", "table_one", "table_two"]
    for table_name in test_table_names:
        if table_name in obj.get_table_names():
            obj.drop_table(table_name)
    assert obj.get_table_names() == []


@pytest.fixture(scope="session", name="synapse_database_table_names")
def fixture_synapse_database_table_names():
    """
    Yields a list of table names the database testing project should start out with.
    """
    yield ["test_table_one"]


@pytest.fixture(scope="session", name="synapse_database_project")
def fixture_synapse(synapse_config_dict, synapse_database_table_names):
    """
    Yields a Synapse object used for testing databases
    """
    obj = Synapse(synapse_config_dict)
    if obj.get_table_names() != synapse_database_table_names:
        raise ValueError("synapse_database_project has incorrect table names.")
    yield obj
    if obj.get_table_names() != synapse_database_table_names:
        raise ValueError("synapse_database_project has incorrect table names.")


@pytest.fixture(scope="session", name="synapse_query_store")
def fixture_synapse_query_store(config_dict):
    """
    Yields a Synapse Query Store
    """
    obj = SynapseQueryStore(config_dict["query_store"])
    assert obj.synapse.get_table_names() == []
    yield obj
    for name in obj.synapse.get_table_names():
        obj.synapse.drop_table(name)
    assert obj.synapse.get_table_names() == []


@pytest.fixture(scope="module", name="rdb_queryer_mysql")
def fixture_rdb_queryer_mysql(mysql, synapse_query_store):
    """Yields a RDBQueryer"""
    obj = RDBQueryer(
        rdb=mysql,
        query_store=synapse_query_store,
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_queryer_synapse")
def fixture_rdb_queryer_synapse(synapse_config_dict, synapse_query_store):
    """Yields a RDBQueryer"""
    obj = RDBQueryer(
        rdb=SynapseDatabase(synapse_config_dict),
        query_store=synapse_query_store,
    )
    yield obj


# config objects --------------------------------------------------------------
@pytest.fixture(scope="session", name="gff_db_config")
def fixture_gff_db_config(gff_schema):
    """Yields a config from the GFF tools schema"""
    yield gff_schema.create_db_config()


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


@pytest.fixture(scope="session", name="table_two_config_combined")
def fixture_table_two_config_combined():
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


@pytest.fixture(scope="session", name="table_configs")
def fixture_table_configs(table_one_config, table_two_config, table_three_config):
    """Yields a DBObjectConfigList"""
    yield DBConfig([table_one_config, table_two_config, table_three_config])
