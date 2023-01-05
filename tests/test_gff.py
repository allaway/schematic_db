"""Testing for GFF"""
from typing import Generator
from copy import copy
import os
import pytest
from sqlalchemy import exc
from schematic_db.rdb import MySQLDatabase, MySQLConfig
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.synapse import SynapseConfig
from schematic_db.schema import Schema
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.query_store import QueryStore, SynapseQueryStore
from schematic_db.rdb_queryer import RDBQueryer
from schematic_db.db_config import DBConfig


@pytest.fixture(scope="session", name="gff_database_table_names")
def fixture_gff_database_table_names() -> Generator:
    """
    Yields a list of table names the gff database should have.
    """
    table_names = [
        "AnimalModel",
        "Antibody",
        "Biobank",
        "CellLine",
        "Development",
        "Donor",
        "Funder",
        "GeneticReagent",
        "Investigator",
        "Mutation",
        "MutationDetails",
        "Observation",
        "Publication",
        "Resource",
        "ResourceApplication",
        "Usage",
        "Vendor",
        "VendorItem",
    ]
    yield table_names


@pytest.fixture(scope="session", name="gff_mysql_database")
def fixture_gff_mysql(mysql_config: MySQLConfig) -> Generator:
    """
    Yields a MYSQL object with database named 'gff_test_schema'
    """
    config = copy(mysql_config)
    config.name = "gff_test_schema"
    obj = MySQLDatabase(config)
    yield obj
    obj.drop_database()


@pytest.fixture(scope="session", name="gff_synapse_database")
def fixture_gff_synapse(secrets_dict: dict) -> Generator:
    """
    Yields a Synapse object used for testing databases
    """
    obj = SynapseDatabase(
        SynapseConfig(
            project_id="syn42838499",
            username=secrets_dict["synapse"]["username"],
            auth_token=secrets_dict["synapse"]["auth_token"],
        )
    )
    yield obj


@pytest.fixture(scope="session", name="gff_synapse_project_id")
def fixture_gff_synapse_project_id() -> Generator:
    """Yields the synapse id for the gff schema project id"""
    yield "syn38296792"


@pytest.fixture(scope="session", name="gff_synapse_asset_view_id")
def fixture_gff_synapse_asset_view_id() -> Generator:
    """Yields the synapse id for the gff schema project id"""
    yield "syn38308526"


@pytest.fixture(scope="session", name="gff_schema")
def fixture_gff_schema(
    gff_synapse_project_id: str, gff_synapse_asset_view_id: str, secrets_dict: dict
) -> Generator:
    """Yields a Schema using the GFF tools schema"""
    schema_url = (
        "https://raw.githubusercontent.com/nf-osi/"
        "nf-research-tools-schema/main/nf-research-tools.jsonld"
    )
    obj = Schema(
        schema_url,
        gff_synapse_project_id,
        gff_synapse_asset_view_id,
        secrets_dict["synapse"]["auth_token"],
    )
    yield obj


@pytest.fixture(scope="session", name="gff_db_config")
def fixture_gff_db_config(gff_schema: Schema) -> Generator:
    """Yields a config from the GFF tools schema"""
    yield gff_schema.db_config


@pytest.fixture(scope="module", name="rdb_updater_mysql_gff")
def fixture_rdb_updater_mysql_gff(
    gff_mysql: MySQLDatabase, gff_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and gff schema with tables added"""
    obj = RDBUpdater(rdb=gff_mysql, schema=gff_schema)
    yield obj


@pytest.fixture(scope="module", name="rdb_updater_synapse_gff")
def fixture_rdb_updater_synapse_gff(
    gff_synapse: SynapseDatabase, gff_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a synapse database and gff schema with tables added"""
    obj = RDBUpdater(rdb=gff_synapse, schema=gff_schema)
    yield obj


@pytest.fixture(scope="session", name="gff_query_csv_path")
def fixture_gff_query_csv_path(data_dir: str = "data_directory") -> Generator:
    """Yields a path to a file of test SQL queries for gff"""
    path = os.path.join(data_dir, "gff_queries.csv")
    yield path


@pytest.mark.gff
@pytest.mark.synapse
@pytest.mark.schematic
class TestSynapseDatabase:  # pylint: disable=too-few-public-methods
    """Testing for gff synapse database"""

    def test_drop_all_tables(self, gff_synapse: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.drop_all_tables()"""
        obj = gff_synapse
        obj.drop_all_tables()
        assert False


@pytest.mark.gff
@pytest.mark.schematic
@pytest.mark.synapse
class TestRDBUpdater:
    """Testing for RDB with Synapse database"""

    def test_update_all_database_tables_mysql(
        self, rdb_updater_mysql_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Creates the gff database in MySQL"""
        with pytest.raises(exc.OperationalError, match="Row size too large"):
            obj = rdb_updater_mysql_gff
            assert obj.rdb.get_table_names() == []
            obj.build_database()
            assert obj.rdb.get_table_names() == gff_database_table_names
            obj.update_database()
            assert obj.rdb.get_table_names() == gff_database_table_names

    def test_update_all_database_tables_synapse(
        self, rdb_updater_synapse_gff: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Creates the gff database in Synapse"""
        obj = rdb_updater_synapse_gff
        obj.build_database()
        assert obj.rdb.get_table_names() == gff_database_table_names
        obj.update_database()
        assert obj.rdb.get_table_names() == gff_database_table_names


# RDBQueryer ------------------------------------------------------------------


@pytest.fixture(scope="session", name="synapse_gff_query_store")
def fixture_synapse_gff_query_store(secrets_dict: dict) -> Generator:
    """
    Yields a Synapse Query Store for gff
    """
    obj = SynapseQueryStore(
        SynapseConfig(
            project_id="syn39024404",
            username=secrets_dict["synapse"]["username"],
            auth_token=secrets_dict["synapse"]["auth_token"],
        )
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_queryer_mysql")
def fixture_rdb_queryer_mysql(
    rdb_updater_mysql_gff: RDBUpdater, synapse_gff_query_store: QueryStore
) -> Generator:
    """Yields a RDBQueryer with a mysql database with gff tables added"""
    obj = RDBQueryer(
        rdb=rdb_updater_mysql_gff.rdb,
        query_store=synapse_gff_query_store,
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_queryer_synapse")
def fixture_rdb_queryer_synapse(
    gff_synapse_database: SynapseDatabase, synapse_gff_query_store: QueryStore
) -> Generator:
    """Yields a RDBQueryer with a mysql database with gff tables added"""
    obj = RDBQueryer(
        rdb=gff_synapse_database,
        query_store=synapse_gff_query_store,
    )
    yield obj


@pytest.mark.gff
@pytest.mark.synapse
@pytest.mark.schematic
class TestRDBQueryer:
    """Testing for RDBQueryer using the gff database"""

    def test_store_query_results_mysql(
        self,
        rdb_queryer_mysql: RDBQueryer,
    ) -> None:
        """Testing for RDBQueryer.store_query_results()"""
        rdb_queryer_mysql.store_query_results("tests/data/gff_queries.csv")

    def test_store_query_results_synapse(
        self,
        rdb_queryer_synapse: RDBQueryer,
    ) -> None:
        """Testing for RDBQueryer.store_query_results()"""
        rdb_queryer_synapse.store_query_results("tests/data/gff_queries.csv")


# Schema ----------------------------------------------------------------------


@pytest.mark.gff
@pytest.mark.schematic
class TestGFFSchema:
    """Testing for GFF Schema"""

    def test_create_db_config(self, gff_db_config: DBConfig) -> None:
        """Testing for Schema.create_db_config()"""
        assert gff_db_config.get_config_names() == [
            "Donor",
            "AnimalModel",
            "CellLine",
            "Antibody",
            "GeneticReagent",
            "Funder",
            "Publication",
            "Investigator",
            "Resource",
            "MutationDetails",
            "Vendor",
            "Development",
            "Mutation",
            "ResourceApplication",
            "Observation",
            "VendorItem",
            "Biobank",
            "Usage",
        ]

    def test_get_manifests(self, gff_schema: Schema, gff_db_config: DBConfig) -> None:
        """Testing for Schema.get_manifests()"""
        manifests1 = gff_schema.get_manifests(gff_db_config.configs[0])
        assert len(manifests1) == 2
        assert list(manifests1[0].columns) == [
            "age",
            "donorId",
            "parentDonorId",
            "race",
            "sex",
            "species",
        ]

        manifests2 = gff_schema.get_manifests(gff_db_config.configs[1])
        assert len(manifests2) == 1
        assert list(manifests2[0].columns) == [
            "animalModelDisease",
            "animalModelofManifestation",
            "animalModelId",
            "animalState",
            "backgroundStrain",
            "backgroundSubstrain",
            "donorId",
            "generation",
            "strainNomenclature",
            "transplantationDonorId",
            "transplantationType",
        ]
