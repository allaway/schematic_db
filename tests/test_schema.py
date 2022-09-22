"""Testing for Schema."""
import pytest
from schema import Schema, get_project_manifests, get_manifest_ids_for_object
from db_object_config import DBForeignKey


@pytest.fixture(name="test_synapse_project_id")
def fixture_test_synapse_project_id():
    """Yields the synapse id for the test schema project id"""
    yield "syn23643250"


@pytest.fixture(name="test_synapse_folder_id")
def fixture_test_synapse_folder_id():
    """Yields a synapse id for a folder in the test project"""
    yield "syn30988314"


@pytest.fixture(name="test_synapse_asset_view_id")
def fixture_test_synapse_asset_view_id():
    """Yields the synapse id for the test schema project id"""
    yield "syn23643253"


@pytest.fixture(name="synapse_input_token")
def fixture_synapse_input_token(secrets_dict):
    """Yields a synapse token"""
    yield secrets_dict["synapse"]["auth_token"]


@pytest.fixture(name="test_schema")
def fixture_test_schema(
    test_synapse_project_id, test_synapse_asset_view_id, synapse_input_token
):
    """Yields a Schema  using the database specific test schema"""
    schema_url = (
        "https://raw.githubusercontent.com/Sage-Bionetworks/"
        "schematic/develop-rdb-export-joins/tests/data/example.rdb.model.jsonld"
    )
    obj = Schema(
        schema_url,
        test_synapse_project_id,
        test_synapse_asset_view_id,
        synapse_input_token,
    )
    yield obj


@pytest.fixture(name="test_manifests")
def fixture_test_manifests():
    """Yields a test set of manifests"""
    yield [
        {"manifest_id": "syn1", "component_name": "C1"},
        {"manifest_id": "syn2", "component_name": "C2"},
        {"manifest_id": "syn3", "component_name": ""},
        {"manifest_id": "", "component_name": "C3"},
        {"manifest_id": "syn5", "component_name": "C1"},
    ]


class TestUtils:
    """Testing for Schema utils"""

    def test_get_manifest_ids_for_object(self, test_manifests):
        """Testing for get_manifest_ids_for_object"""
        assert get_manifest_ids_for_object("C1", test_manifests) == ["syn1", "syn5"]
        assert get_manifest_ids_for_object("C2", test_manifests) == ["syn2"]
        assert get_manifest_ids_for_object("C3", test_manifests) == []

    def test_get_project_manifests(
        self, synapse_input_token, test_synapse_folder_id, test_synapse_asset_view_id
    ):
        "Testing for get_project_manifests"
        manifests = get_project_manifests(
            input_token=synapse_input_token,
            project_id=test_synapse_folder_id,
            asset_view=test_synapse_asset_view_id,
        )
        assert manifests == [
            {
                "dataset_id": "syn30988361",
                "dataset_name": "TestFolder",
                "manifest_id": "syn30988380",
                "manifest_name": "synapse_storage_manifest (2).csv",
                "component_name": "Biospecimen",
            }
        ]


class TestSchema:
    """Testing for Schema"""

    def test_create_foreign_keys(self, test_schema):
        """Testing for Schema.create_foreign_keys()"""
        obj = test_schema
        assert obj.create_foreign_keys("Patient") == []
        assert obj.create_foreign_keys("Biospecimen") == [
            DBForeignKey(
                name="patientId",
                foreign_object_name="Patient",
                foreign_attribute_name="patientId",
            )
        ]
        assert obj.create_foreign_keys("BulkRNA-seqAssay") == [
            DBForeignKey(
                name="biospecimenId",
                foreign_object_name="Biospecimen",
                foreign_attribute_name="biospecimenId",
            )
        ]

    def test_create_db_config(self, test_schema):
        """Testing for Schema.test_create_db_config()"""
        obj = test_schema
        config = obj.create_db_config()
        assert config.get_config_names() == [
            "Patient",
            "Biospecimen",
            "BulkRNA-seqAssay",
        ]
