"""Testing for Synapse."""
import pytest
import pandas as pd


@pytest.fixture(scope="module", name="synapse")
def fixture_synapse(
    synapse_database_project, synapse_database_table_names, table_one_config
):
    """
    Yields a Synapse object
    """
    obj = synapse_database_project
    obj.add_table("table_one", table_one_config)
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        if name not in synapse_database_table_names:
            obj.drop_table(name)


class TestSynapseIDs:
    """Testing for  id methods"""

    def test_get_table_id_from_name(self, synapse):
        """Testing for Synapse.get_table_id_from_name()"""
        assert synapse.get_synapse_id_from_table_name("test_table_one") == "syn34532191"

    def test_get_table_name_from_id(self, synapse):
        """Testing for Synapse.get_table_name_from_id()"""
        assert synapse.get_table_name_from_synapse_id("syn34532191") == "test_table_one"


class TestSynapseQueries:
    """Testing for query methods"""

    def test_query_table(self, synapse, table_one, table_one_config):
        """Testing for synapse.query_table()"""
        result = synapse.query_table("test_table_one", table_one_config)
        pd.testing.assert_frame_equal(result, table_one)


class TestSynapse:
    """
    Testing for synapse
    """

    def test_add_drop_table(
        self, synapse, table_one_config, synapse_database_table_names
    ):
        """Testing for Synapse.add_table(), and Synapse.drop_table()"""
        assert synapse.get_table_names() == ["table_one"] + synapse_database_table_names
        synapse.drop_table("table_one")
        assert synapse.get_table_names() == synapse_database_table_names
        synapse.add_table("table_one", table_one_config)
        assert synapse.get_table_names() == ["table_one"] + synapse_database_table_names


class TestSynapseModifyRows:
    """
    Testing for synapse
    """

    def test_insert_table_rows(
        self, synapse, table_one, table_one_config, synapse_database_table_names
    ):
        """
        Testing for synapse.insert_table_rows()
        """
        assert synapse.get_table_names() == ["table_one"] + synapse_database_table_names

        synapse.insert_table_rows("table_one", table_one)
        result1 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        synapse.insert_table_rows("table_one", table_one)
        result2 = synapse.query_table("table_one", table_one_config)
        test_table = pd.concat(objs=[table_one, table_one])
        test_table.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(result2, test_table)

        synapse.drop_table("table_one")
        synapse.add_table("table_one", table_one_config)

    def test_delete_table_rows(
        self, synapse, table_one, table_one_config, synapse_database_table_names
    ):
        """
        Testing for synapse.delete_table_rows()
        """
        assert synapse.get_table_names() == ["table_one"] + synapse_database_table_names
        synapse.insert_table_rows("table_one", table_one)
        result1 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        synapse.delete_table_rows("table_one", table_one.iloc[[0]], table_one_config)
        result2 = synapse.query_table("table_one", table_one_config)
        test_table = table_one.iloc[1:]
        test_table.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(result2, test_table)

        synapse.delete_table_rows("table_one", table_one.iloc[[0]], table_one_config)
        result3 = synapse.query_table("table_one", table_one_config)
        test_table2 = table_one.iloc[1:]
        test_table2.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(result3, test_table2)

        synapse.drop_table("table_one")
        synapse.add_table("table_one", table_one_config)

    def test_update_table_rows(
        self, synapse, table_one, table_one_config, synapse_database_table_names
    ):
        """
        Testing for synapse.update_table_rows()
        """
        assert synapse.get_table_names() == ["table_one"] + synapse_database_table_names
        synapse.insert_table_rows("table_one", table_one)
        result1 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        synapse.update_table_rows("table_one", table_one, table_one_config)
        result2 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result2, table_one)

        update_table = pd.DataFrame({"pk_one_col": ["key1"], "string_one_col": ["x"]})
        test_table = table_one
        test_table.at[0, "string_one_col"] = "x"
        synapse.update_table_rows("table_one", update_table, table_one_config)
        result3 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result3, test_table)

        synapse.drop_table("table_one")
        synapse.add_table("table_one", table_one_config)

    def test_upsert_table_rows(
        self, synapse, table_one, table_one_config, synapse_database_table_names
    ):
        """
        Testing for synapse.upsert_table_rows()
        """
        assert synapse.get_table_names() == ["table_one"] + synapse_database_table_names
        synapse.insert_table_rows("table_one", table_one)
        result1 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        synapse.upsert_table_rows("table_one", table_one, table_one_config)
        result2 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result2, table_one)

        upsert_table = table_one
        upsert_table.at[0, "string_one_col"] = "x"
        synapse.update_table_rows("table_one", upsert_table, table_one_config)
        result3 = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result3, upsert_table)

        synapse.drop_table("table_one")
        synapse.add_table("table_one", table_one_config)
