"""Testing for Schema."""
import pytest
import pandas as pd
from schema import (
    get_project_manifests,
    get_manifest_ids_for_object,
    get_manifest,
)
from db_object_config import DBForeignKey, DBAttributeConfig, DBDatatype


@pytest.fixture(name="test_synapse_folder_id")
def fixture_test_synapse_folder_id():
    """Yields a synapse id for a folder in the test project"""
    yield "syn30988314"


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


class FutureTestUtils:
    """Testing for Schema utils"""

    def test_get_manifest_ids_for_object(self, test_manifests):
        """Testing for get_manifest_ids_for_object"""
        assert get_manifest_ids_for_object("C1", test_manifests) == ["syn1", "syn5"]
        assert get_manifest_ids_for_object("C2", test_manifests) == ["syn2"]
        assert get_manifest_ids_for_object("C3", test_manifests) == []

    def test_get_project_manifests(
        self, secrets_dict, test_synapse_folder_id, test_synapse_asset_view_id
    ):
        "Testing for get_project_manifests"
        manifests = get_project_manifests(
            input_token=secrets_dict.get("synapse").get("auth_token"),
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

    def test_get_project_manifests2(
        self, secrets_dict, gff_synapse_project_id, gff_synapse_asset_view_id
    ):
        "Testing for get_project_manifests"
        manifests = get_project_manifests(
            input_token=secrets_dict.get("synapse").get("auth_token"),
            project_id=gff_synapse_project_id,
            asset_view=gff_synapse_asset_view_id,
        )
        assert len(manifests) == 31

    def test_get_manifest(self, secrets_dict, gff_synapse_asset_view_id):
        "Testing for get_manifest"
        manifest = get_manifest(
            secrets_dict.get("synapse").get("auth_token"),
            "syn38306654",
            gff_synapse_asset_view_id,
        )
        assert isinstance(manifest, pd.DataFrame)


class FutureTestSchema:
    """Testing for Schema"""

    def test_create_attributes(self, test_schema):
        """Testing for Schema.attributes()"""
        obj = test_schema
        assert obj.create_attributes("Patient") == [
            DBAttributeConfig(name="sex", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="yearofBirth", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="diagnosis", datatype=DBDatatype.TEXT),
        ]
        assert obj.create_attributes("Biospecimen") == [
            DBAttributeConfig(name="patientId", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="tissueStatus", datatype=DBDatatype.TEXT),
        ]

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
        """Testing for Schema.create_db_config()"""
        obj = test_schema
        config = obj.create_db_config()
        assert config.get_config_names() == [
            "Patient",
            "Biospecimen",
            "BulkRNA-seqAssay",
        ]


class FutureTestGFFSchema:
    """Testing for GFF Schema"""

    def test_create_db_config(self, gff_db_config):
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

    def test_get_manifests(self, gff_schema, gff_db_config):
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
