"""Testing for Synapse."""
from typing import Any, Generator
import pytest
import pandas as pd
import synapseclient as sc  # type: ignore
from schematic_db.db_config.db_config import (
    DBAttributeConfig,
    DBDatatype,
    DBObjectConfig,
)
from schematic_db.synapse import Synapse, SynapseConfig

@pytest.fixture(name="synapse_with_test_table_one", scope="class")
def fixture_synapse_with_test_table_one(
    synapse_database_project: Synapse, table_one_config: DBObjectConfig, table_one: pd.DataFrame
) -> Generator:
    """
    Yields a Synapse object with "test_table_one" added, used only for tests that
     don't alter the state
    """
    obj = synapse_database_project
    obj.add_table("test_table_one", table_one_config)
    synapse_id = obj.get_synapse_id_from_table_name("test_table_one")
    obj.insert_table_rows("test_table_one", table_one)
    obj.set_entity_annotations(synapse_id, {"test_annotation": "test_value"})
    yield obj
    obj.delete_table("test_table_one")


@pytest.fixture(name="synapse_with_no_tables")
def fixture_synapse_with_no_tables(synapse_database_project) -> Generator:
    """
    Yields a Synapse object
    """
    obj = synapse_database_project
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        obj.delete_table(name)


@pytest.fixture(name="synapse_with_empty_table_one")
def fixture_synapse_with_empty_table_one(
    synapse_with_no_tables: Synapse, table_one_config: DBObjectConfig
) -> Generator:
    """
    Yields a Synapse object with table one added
    """
    obj = synapse_with_no_tables
    obj.add_table("table_one", table_one_config)
    yield obj


@pytest.fixture(name="synapse_with_filled_table_one")
def fixture_synapse_with_filled_table_one(
    synapse_with_empty_table_one: Synapse, table_one: pd.DataFrame
) -> Generator:
    """
    Yields a Synapse object with table one filled
    """
    obj = synapse_with_empty_table_one
    obj.insert_table_rows("table_one", table_one)
    yield obj


@pytest.fixture(name="mock_synapse_config")
def fixture_mock_synapse_config() -> Generator:
    """
    Yields a Synapse object with table one filled
    """
    yield SynapseConfig(username="", auth_token="", project_id="")


@pytest.mark.fast
class TestMockSynapse:
    """Testing for Synapse class with mocked methods"""

    def test_get_table_names(
        self, mocker: Any, mock_synapse_config: SynapseConfig
    ) -> None:
        """Testing for Synapse.get_table_names"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        obj = Synapse(mock_synapse_config)
        assert obj.get_table_names() == ["table1", "table2"]

    def test_get_synapse_id_from_table_name(
        self, mocker: Any, mock_synapse_config: SynapseConfig
    ) -> None:
        """Testing for Synapse.get_synapse_id_from_table_name"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        obj = Synapse(mock_synapse_config)
        assert obj.get_synapse_id_from_table_name("table1") == "syn1"
        assert obj.get_synapse_id_from_table_name("table2") == "syn2"

    def test_get_table_name_from_synapse_id(
        self, mocker: Any, mock_synapse_config: SynapseConfig
    ) -> None:
        """Testing for Synapse.get_table_name_from_synapse_id"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        obj = Synapse(mock_synapse_config)
        assert obj.get_table_name_from_synapse_id("syn1") == "table1"
        assert obj.get_table_name_from_synapse_id("syn2") == "table2"

    def test_query_table(self, mocker: Any, mock_synapse_config: SynapseConfig) -> None:
        """Testing for Synapse.query_table"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        query_result = pd.DataFrame({"col1": ["a", "b"], "col2": [1, 2]})
        config = DBObjectConfig(
            name="tbl1",
            attributes=[
                DBAttributeConfig("col1", DBDatatype.TEXT),
                DBAttributeConfig("col2", DBDatatype.INT),
            ],
            primary_key="col1",
            foreign_keys=[],
        )
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse.execute_sql_query",
            return_value=query_result,
        )
        obj = Synapse(mock_synapse_config)
        assert isinstance(obj.query_table("table1", config), pd.DataFrame)


@pytest.mark.synapse
class TestSynapseGetters:
    """Testing for Synapse class getters"""

    def test_get_table_names(
        self, synapse_with_test_table_one: Synapse
    ) -> None:
        """Testing for Synapse.get_table_names()"""
        assert (
            synapse_with_test_table_one.get_table_names() == ["test_table_one"]
        )

    def test_get_column_table_names(
        self, synapse_with_test_table_one: Synapse, table_one_config: DBObjectConfig
    ) -> None:
        """Testing for Synapse.get_table_column_names()"""
        assert (
            synapse_with_test_table_one.get_table_column_names("test_table_one")
            == table_one_config.get_attribute_names()
        )

    def test_get_table_id_and_name(self, synapse_with_test_table_one: Synapse) -> None:
        """Testing for Synapse.get_table_id_from_name()"""
        synapse_id = synapse_with_test_table_one.get_synapse_id_from_table_name("test_table_one")
        table_name = synapse_with_test_table_one.get_table_name_from_synapse_id(synapse_id)
        assert table_name == "test_table_one"

    def test_query_table(
        self,
        synapse_with_test_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for synapse.query_table()"""
        result = synapse_with_test_table_one.query_table(
            "test_table_one", table_one_config
        )
        pd.testing.assert_frame_equal(result, table_one)

    def test_get_entity_annotations(self, synapse_with_test_table_one: Synapse) -> None:
        """Testing for Synapse.get_entity_annotations"""
        obj = synapse_with_test_table_one
        synapse_id = obj.get_synapse_id_from_table_name("test_table_one")
        annotations = obj.get_entity_annotations(synapse_id)
        assert annotations.id == synapse_id
        assert annotations == {"test_annotation": ["test_value"]}


@pytest.mark.synapse
class TestSynapseModifyTables:
    """
    Testing for methods that add or drop tables
    """

    def test_build_table(
        self,
        synapse_with_no_tables: Synapse,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for Synapse.add_table()"""
        obj = synapse_with_no_tables
        assert obj.get_table_names() == []
        obj.build_table("table_one", table_one)
        assert obj.get_table_names() == ["table_one"]

    def test_add_table(
        self,
        synapse_with_no_tables: Synapse,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for Synapse.add_table()"""
        obj = synapse_with_no_tables
        assert obj.get_table_names() == []
        obj.add_table("table_one", table_one_config)
        assert obj.get_table_names() == ["table_one"]

    def test_delete_table(
        self,
        synapse_with_empty_table_one: Synapse,
    ) -> None:
        """Testing for Synapse.delete_table()"""
        obj = synapse_with_empty_table_one
        assert obj.get_table_names() == ["table_one"]
        obj.delete_table("table_one")
        assert obj.get_table_names() == []

    def test_replace_table(
        self,
        synapse_with_filled_table_one: Synapse,
        table_two: pd.DataFrame,
        table_two_config: DBObjectConfig,
    ) -> None:
        """Testing for synapse.replace_table()"""
        obj = synapse_with_filled_table_one
        table_id1 = obj.get_synapse_id_from_table_name("table_one")
        obj.replace_table("table_one", table_two)
        result1 = obj.query_table("table_one", table_two_config)
        pd.testing.assert_frame_equal(result1, table_two)
        table_id2 = obj.get_synapse_id_from_table_name("table_one")
        assert table_id1 == table_id2


@pytest.mark.synapse
class TestSynapseModifyRows:
    """
    Testing for synapse methods that modify row data
    """

    def test_insert_table_rows(
        self,
        synapse_with_empty_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for synapse.insert_table_rows()
        """
        obj = synapse_with_empty_table_one
        obj.insert_table_rows("table_one", table_one)
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.insert_table_rows("table_one", table_one)
        result2 = obj.query_table("table_one", table_one_config)
        test_table = pd.concat(objs=[table_one, table_one])
        test_table.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(result2, test_table)

    def test_delete_table_rows(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for Synapse.delete_table_rows()"""
        obj = synapse_with_filled_table_one
        table_id = obj.get_synapse_id_from_table_name("table_one")
        query = f"SELECT {table_one_config.primary_key} FROM {table_id}"
        table = obj.execute_sql_query(query, include_row_data=True)
        assert table["ROW_ID"].tolist() == [1, 2, 3]

        obj.delete_table_rows(table_id, table.iloc[[0]])
        table2 = obj.execute_sql_query(query, include_row_data=True)
        assert table2["ROW_ID"].tolist() == [2, 3]

    def test_delete_all_table_rows(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for synapse.delete_all_table_rows()
        """
        obj = synapse_with_filled_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.delete_all_table_rows(synapse_id)
        result2 = obj.query_table("table_one", table_one_config)
        assert result2.empty

    def test_update_table_rows(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for synapse.update_table_rows()
        """
        obj = synapse_with_filled_table_one
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.update_table_rows("table_one", table_one, table_one_config)
        result2 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result2, table_one)

        update_table = pd.DataFrame({"pk_one_col": ["key1"], "string_one_col": ["x"]})
        test_table = table_one
        test_table.at[0, "string_one_col"] = "x"
        obj.update_table_rows("table_one", update_table, table_one_config)
        result3 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result3, test_table)

    def test_upsert_table_rows(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for synapse.upsert_table_rows()
        """
        obj = synapse_with_filled_table_one
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.upsert_table_rows("table_one", table_one, table_one_config)
        result2 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result2, table_one)

        upsert_table = table_one
        upsert_table.at[0, "string_one_col"] = "x"
        obj.update_table_rows("table_one", upsert_table, table_one_config)
        result3 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result3, upsert_table)


@pytest.mark.synapse
class TestSynapseModifyColumns:
    """
    Testing for synapse methods that modify table columns
    """

    def test_delete_all_table_columns(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for synapse.delete_all_table_columns()
        """
        obj = synapse_with_filled_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.delete_all_table_columns(synapse_id)
        with pytest.raises(
            sc.core.exceptions.SynapseHTTPError,
            match="400 Client Error",
        ):
            obj.query_table("table_one", table_one_config)

    def test_add_table_columns(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """
        Testing for synapse.add_table_columns()
        """
        obj = synapse_with_filled_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        with pytest.raises(
            sc.core.exceptions.SynapseHTTPError,
            match="400 Client Error",
        ):
            obj.add_table_columns("table_one", table_one)

        obj.delete_all_table_columns(synapse_id)
        obj.add_table_columns("table_one", table_one)
        assert obj.get_table_column_names("table_one") == [
            "pk_one_col",
            "string_one_col",
            "int_one_col",
            "double_one_col",
            "date_one_col",
            "bool_one_col",
        ]


@pytest.mark.synapse
class TestSynapseAnnotations:
    """Testing for annotation methods"""

    def test_set_entity_annotations(
        self, synapse_with_empty_table_one: Synapse
    ) -> None:
        """Testing for Synapse.set_entity_annotations"""
        obj = synapse_with_empty_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        annotations = obj.get_entity_annotations(synapse_id)
        assert annotations.id == synapse_id
        assert annotations == {}

        obj.set_entity_annotations(synapse_id, {"test_annotation": "test_value"})
        annotations2 = obj.get_entity_annotations(synapse_id)
        assert annotations2.id == synapse_id
        assert annotations2 == {"test_annotation": ["test_value"]}

    def test_clear_entity_annotations(
        self, synapse_with_empty_table_one: Synapse
    ) -> None:
        """Testing for Synapse.clear_entity_annotations"""
        obj = synapse_with_empty_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        annotations = obj.get_entity_annotations(synapse_id)
        assert annotations.id == synapse_id
        assert annotations == {}

        obj.set_entity_annotations(synapse_id, {"test_annotation": "test_value"})
        annotations2 = obj.get_entity_annotations(synapse_id)
        assert annotations2.id == synapse_id
        assert annotations2 == {"test_annotation": ["test_value"]}

        obj.clear_entity_annotations(synapse_id)
        annotations3 = obj.get_entity_annotations(synapse_id)
        assert annotations3.id == synapse_id
        assert annotations3 == {}
