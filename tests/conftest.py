"""Fixtures for all tests
"""
# pylint: disable=E0401
from datetime import date
import pytest # type: ignore
import pandas as pd # type: ignore
import numpy as np # type: ignore
from db_object_config import DBObjectConfig, DBAttributeConfig, DBDatatype


@pytest.fixture
def table_one():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame({
        "pk_col": ["key1", "key2", "key3"],
        "string_col": ["a","b", np.nan],
        "int_col": [1,pd.NA,3],
        "double_col":[1.1,2.2,np.nan],
        "date_col":[date.today(), np.nan, date.today()],
        "bool_col":[pd.NA, True, False]
    })
    dataframe = dataframe.astype({"int_col": "Int64", "bool_col": "boolean"})
    yield dataframe

@pytest.fixture
def table_one_config():
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name = "table_one",
        attributes = [
            DBAttributeConfig(name="pk_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="string_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="int_col", datatype=DBDatatype.Int),
            DBAttributeConfig(name="double_col", datatype=DBDatatype.Float),
            DBAttributeConfig(name="date_col", datatype=DBDatatype.Date),
            DBAttributeConfig(name="bool_col", datatype=DBDatatype.Boolean)
        ],
        primary_keys = ["pk_col"],
        foreign_keys = []
    )
    yield table_config
