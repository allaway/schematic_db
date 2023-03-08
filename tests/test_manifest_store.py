"""Testing for ManifestStore."""
from typing import Generator
import pytest
from schematic_db.manifest_store import (
    ManifestStore,
    ManifestStoreConfig,
)

from schematic_db.api_utils import ManifestSynapseConfig


@pytest.fixture(name="manifest_store")
def fixture_manifest_store(
    test_synapse_project_id: str,
    test_synapse_asset_view_id: str,
    secrets_dict: dict,
    test_schema_json_url: str,
) -> Generator:
    """Yields a ManifestStore object"""
    yield ManifestStore(
        ManifestStoreConfig(
            test_schema_json_url,
            test_synapse_project_id,
            test_synapse_asset_view_id,
            secrets_dict["synapse"]["auth_token"],
        )
    )


@pytest.mark.schematic
class TestSchema:
    """Testing for ManifestStore"""

    def test_init(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.__init__"""
        obj = manifest_store
        for item in obj.manifest_configs:
            assert isinstance(item, ManifestSynapseConfig)

    def test_get_manifests(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.get_manifests()"""
        obj = manifest_store
        manifests = obj.get_manifests("Patient")
        assert len(manifests) == 2
