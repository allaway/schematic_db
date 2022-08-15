"""Creates tables in Synapse test projects
"""
from datetime import datetime
import pandas as pd
import numpy as np
import synapseclient as sc

MANIFEST_STORE_ID = "syn33937311"

syn = sc.Synapse()
syn.login()

syn.store(
    sc.table.build_table(
        "table_one",
        MANIFEST_STORE_ID,
        pd.DataFrame({
            "pk_one_col": ["key1", "key2", "key3"],
            "string_one_col": ["a","b", np.nan],
            "int_one_col": [1,pd.NA,3],
            "double_one_col": [1.1,2.2,np.nan],
            "date_one_col": [datetime(2022, 8, 2), np.nan, datetime(2022, 8, 2)],
            "bool_one_col": [pd.NA, True, False]
        })
    )
)

syn.store(
    sc.table.build_table(
        "table_two",
        MANIFEST_STORE_ID,
        pd.DataFrame({
            "pk_two_col": ["key1", "key2", "key3", "key4"],
            "string_two_col": ["a", "b", "c", "d"]
        })
    )
)

syn.store(
    sc.table.build_table(
        "table_two_b",
        MANIFEST_STORE_ID,
        pd.DataFrame({
            "pk_two_col": ["key5", "key6", "key7", "key8"],
            "string_two_col": ["a", "b", "c", "d"]
        })
    )
)

syn.store(
    sc.table.build_table(
        "table_three",
        MANIFEST_STORE_ID,
        pd.DataFrame({
            "pk_one_col": ["key1", "key1", "key2", "key2"],
            "pk_two_col": ["key1", "key2", "key1", "key2"],
            "string_three_col": ["a", "b", "c", "d"]
        })
    )
)
