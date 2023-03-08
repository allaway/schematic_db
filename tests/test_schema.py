"""Testing for Schema."""
from typing import Generator
import pytest
from schematic_db.db_config import (
    DBConfig,
    DBForeignKey,
    DBAttributeConfig,
    DBDatatype,
)
from schematic_db.schema import (
    Schema,
    ManifestSynapseConfig,
    DatabaseConfig,
    DatabaseObjectConfig,
    get_manifest_ids_for_object,
    get_dataset_ids_for_object,
)


@pytest.fixture(name="database_config")
def fixture_database_config() -> Generator:
    """Yields a DatabaseConfig"""
    data = [
        {
            "name": "object1",
            "primary_key": "att1",
            "foreign_keys": [
                {
                    "attribute_name": "att2",
                    "foreign_object_name": "object2",
                    "foreign_attribute_name": "att1",
                },
                {
                    "attribute_name": "att3",
                    "foreign_object_name": "object3",
                    "foreign_attribute_name": "att1",
                },
            ],
            "attributes": [
                {
                    "attribute_name": "att2",
                    "datatype": "str",
                    "required": True,
                    "index": True,
                },
                {
                    "attribute_name": "att3",
                    "datatype": "int",
                    "required": False,
                    "index": False,
                },
            ],
        },
        {"name": "object2", "primary_key": "att1"},
        {"name": "object3", "primary_key": "att1"},
    ]
    obj = DatabaseConfig(data)  # type: ignore
    yield obj


@pytest.fixture(name="database_object_config")
def fixture_database_object_config() -> Generator:
    """Yields a DatabaseObjectConfig"""
    data = {
        "name": "object1",
        "primary_key": "att1",
        "foreign_keys": [
            {
                "attribute_name": "att2",
                "foreign_object_name": "object2",
                "foreign_attribute_name": "att1",
            },
            {
                "attribute_name": "att3",
                "foreign_object_name": "object3",
                "foreign_attribute_name": "att1",
            },
        ],
    }
    obj = DatabaseObjectConfig(**data)  # type: ignore
    yield obj


@pytest.mark.fast
class TestDatabaseConfig:
    """Testing for DatabaseConfig"""

    def test_init1(self, database_object_config: DatabaseObjectConfig) -> None:
        """Testing for init"""
        obj1 = database_object_config
        assert obj1

    def test_get_primary_key(self, database_config: DatabaseConfig) -> None:
        """Testing for get_primary_key"""
        obj = database_config
        assert obj.get_primary_key("object1") == "att1"

    def test_get_foreign_keys(self, database_config: DatabaseConfig) -> None:
        """Testing for get_foreign_keys"""
        obj = database_config
        assert obj.get_foreign_keys("object1") is not None
        assert obj.get_foreign_keys("object2") is None
        assert obj.get_foreign_keys("object3") is None

    def test_get_attributes(self, database_config: DatabaseConfig) -> None:
        """Testing for get_attributes"""
        obj = database_config
        assert obj.get_attributes("object1") is not None
        assert obj.get_attributes("object2") is None
        assert obj.get_attributes("object3") is None


@pytest.fixture(name="test_manifests")
def fixture_test_manifests() -> Generator:
    """Yields a test set of manifests"""
    yield [
        ManifestSynapseConfig(
            manifest_id="syn1",
            dataset_id="syn6",
            component_name="C1",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="syn2",
            dataset_id="syn7",
            component_name="C2",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="syn3",
            dataset_id="syn8",
            component_name="",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="",
            dataset_id="syn9",
            component_name="C3",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="syn5",
            dataset_id="syn10",
            component_name="C1",
            dataset_name="",
            manifest_name="",
        ),
    ]


@pytest.mark.fast
class TestUtils:
    """Testing for Schema utils"""

    def test_get_manifest_ids_for_object(
        self, test_manifests: list[ManifestSynapseConfig]
    ) -> None:
        """Testing for get_manifest_ids_for_object"""
        assert get_manifest_ids_for_object("C1", test_manifests) == ["syn1", "syn5"]
        assert get_manifest_ids_for_object("C2", test_manifests) == ["syn2"]
        assert get_manifest_ids_for_object("C3", test_manifests) == []

    def test_get_dataset_ids_for_object(
        self, test_manifests: list[ManifestSynapseConfig]
    ) -> None:
        """Testing for get_manifest_ids_for_object"""
        assert get_dataset_ids_for_object("C1", test_manifests) == ["syn6", "syn10"]
        assert get_dataset_ids_for_object("C2", test_manifests) == ["syn7"]
        assert get_dataset_ids_for_object("C3", test_manifests) == []


@pytest.mark.schematic
class TestSchema:
    """Testing for Schema"""

    def test_init(self, test_schema1: Schema) -> None:
        """Testing for Schema.__init__"""
        obj = test_schema1
        for item in obj.manifest_configs:
            assert isinstance(item, ManifestSynapseConfig)
        config = obj.get_db_config()
        assert isinstance(config, DBConfig)
        assert config.get_config_names() == [
            "Patient",
            "Biospecimen",
            "BulkRnaSeq",
        ]

    def test_create_attributes(self, test_schema1: Schema) -> None:
        """Testing for Schema.attributes()"""
        obj = test_schema1
        assert obj.create_attributes("Patient") == [
            DBAttributeConfig(
                name="id", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(
                name="sex", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(
                name="yearofBirth", datatype=DBDatatype.INT, required=False, index=False
            ),
            DBAttributeConfig(name="weight", datatype=DBDatatype.FLOAT, required=False),
            DBAttributeConfig(
                name="diagnosis", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(name="date", datatype=DBDatatype.DATE, required=False),
        ]
        assert obj.create_attributes("Biospecimen") == [
            DBAttributeConfig(
                name="id", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(
                name="patientId", datatype=DBDatatype.TEXT, required=False, index=False
            ),
            DBAttributeConfig(
                name="tissueStatus",
                datatype=DBDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]
        assert obj.create_attributes("BulkRnaSeq") == [
            DBAttributeConfig(
                name="id", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(
                name="biospecimenId",
                datatype=DBDatatype.TEXT,
                required=False,
                index=False,
            ),
            DBAttributeConfig(name="filename", datatype=DBDatatype.TEXT, required=True),
            DBAttributeConfig(
                name="fileFormat", datatype=DBDatatype.TEXT, required=True, index=False
            ),
        ]

    def test_create_foreign_keys(self, test_schema1: Schema) -> None:
        """Testing for Schema.create_foreign_keys()"""
        obj = test_schema1
        assert obj.create_foreign_keys("Patient") == []
        assert obj.create_foreign_keys("Biospecimen") == [
            DBForeignKey(
                name="patientId",
                foreign_object_name="Patient",
                foreign_attribute_name="id",
            )
        ]
        assert obj.create_foreign_keys("BulkRnaSeq") == [
            DBForeignKey(
                name="biospecimenId",
                foreign_object_name="Biospecimen",
                foreign_attribute_name="id",
            )
        ]

    def test_get_manifests(self, test_schema1: Schema) -> None:
        """Testing for Schema.get_manifests()"""
        obj = test_schema1
        db_config = obj.get_db_config()
        patient_config = db_config.get_config_by_name("Patient")
        manifests = obj.get_manifests(patient_config)
        assert len(manifests) == 2


@pytest.mark.schematic
class TestSchema2:
    """Testing for Schema"""

    def test_init(self, test_schema2: Schema) -> None:
        """Testing for Schema.__init__"""
        obj = test_schema2
        for item in obj.manifest_configs:
            assert isinstance(item, ManifestSynapseConfig)
        config = obj.get_db_config()
        assert isinstance(config, DBConfig)
        assert config.get_config_names() == [
            "Patient",
            "Biospecimen",
            "BulkRnaSeq",
        ]

    def test_create_attributes(self, test_schema2: Schema) -> None:
        """Testing for Schema.attributes()"""
        obj = test_schema2
        assert obj.create_attributes("Patient") == [
            DBAttributeConfig(
                name="id", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(
                name="sex", datatype=DBDatatype.TEXT, required=True, index=True
            ),
            DBAttributeConfig(
                name="yearofBirth", datatype=DBDatatype.INT, required=False, index=False
            ),
            DBAttributeConfig(name="weight", datatype=DBDatatype.FLOAT, required=False),
            DBAttributeConfig(
                name="diagnosis", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(name="date", datatype=DBDatatype.DATE, required=False),
        ]
        assert obj.create_attributes("Biospecimen") == [
            DBAttributeConfig(
                name="id", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(
                name="patientId", datatype=DBDatatype.TEXT, required=False, index=False
            ),
            DBAttributeConfig(
                name="tissueStatus",
                datatype=DBDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]
        assert obj.create_attributes("BulkRnaSeq") == [
            DBAttributeConfig(
                name="id", datatype=DBDatatype.TEXT, required=True, index=False
            ),
            DBAttributeConfig(
                name="biospecimenId",
                datatype=DBDatatype.TEXT,
                required=False,
                index=False,
            ),
            DBAttributeConfig(name="filename", datatype=DBDatatype.TEXT, required=True),
            DBAttributeConfig(
                name="fileFormat", datatype=DBDatatype.TEXT, required=True, index=False
            ),
        ]

    def test_create_foreign_keys(self, test_schema2: Schema) -> None:
        """Testing for Schema.create_foreign_keys()"""
        obj = test_schema2
        assert obj.create_foreign_keys("Patient") == []
        assert obj.create_foreign_keys("Biospecimen") == [
            DBForeignKey(
                name="patientId",
                foreign_object_name="Patient",
                foreign_attribute_name="id",
            )
        ]
        assert obj.create_foreign_keys("BulkRnaSeq") == [
            DBForeignKey(
                name="biospecimenId",
                foreign_object_name="Biospecimen",
                foreign_attribute_name="id",
            )
        ]

    def test_get_manifests(self, test_schema2: Schema) -> None:
        """Testing for Schema.get_manifests()"""
        obj = test_schema2
        db_config = obj.get_db_config()
        patient_config = db_config.get_config_by_name("Patient")
        manifests = obj.get_manifests(patient_config)
        assert len(manifests) == 2
