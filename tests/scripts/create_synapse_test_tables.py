"""Creates tables in Synapse test projects
"""
from datetime import datetime
import pandas as pd
import numpy as np
import synapseclient as sc

MANIFEST_STORE_ID = "syn33937311"
DATABASE_ID = "syn33832432"

syn = sc.Synapse()
syn.login()

df_one = pd.DataFrame(
    {
        "pk_one_col": ["key1", "key2", "key3"],
        "string_one_col": ["a", "b", np.nan],
        "int_one_col": [1, pd.NA, 3],
        "double_one_col": [1.1, 2.2, np.nan],
        "date_one_col": [datetime(2022, 8, 2), np.nan, datetime(2022, 8, 2)],
        "bool_one_col": [pd.NA, True, False],
    }
)
df_one.to_csv("table_one.csv")
# syn.store(sc.File("table_one.csv", parent=MANIFEST_STORE_ID))
db_table_one = sc.table.build_table("test_table_one", DATABASE_ID, df_one)
# syn.store(db_table_one)

df_two = pd.DataFrame(
    {
        "pk_one_col": ["key1", "key2", "key3"],
        "string_one_col": ["a", "b", np.nan],
        "int_one_col": [1, pd.NA, 3],
        "double_one_col": [1.1, 2.2, np.nan],
        "date_one_col": [datetime(2022, 8, 2), np.nan, datetime(2022, 8, 2)],
        "bool_one_col": [pd.NA, True, False],
    }
)

df_two.to_csv("table_two.csv")
syn.store(sc.File("table_two.csv", parent=MANIFEST_STORE_ID))

df_two_b = pd.DataFrame(
    {
        "pk_two_col": ["key5", "key6", "key7", "key8"],
        "string_two_col": ["a", "b", "c", "d"],
    }
)

df_two_b.to_csv("table_two_b.csv")
syn.store(sc.File("table_two_b.csv", parent=MANIFEST_STORE_ID))

df_three = pd.DataFrame(
    {
        "pk_one_col": ["key1", "key1", "key2", "key2"],
        "pk_two_col": ["key1", "key2", "key1", "key2"],
        "string_three_col": ["a", "b", "c", "d"],
    }
)

df_three.to_csv("table_three.csv")
syn.store(sc.File("table_three.csv", parent=MANIFEST_STORE_ID))

df_123_unormalized = pd.DataFrame(
    {
        "pk_one_col": ["key1", "key1", "key2", "key2"],
        "pk_two_col": ["key1", "key2", "key1", "key2"],
        "string_three_col": ["a", "b", "c", "d"],
        "string_one_col": ["a", "a", "b", "a"],
    }
)

df_123_unormalized.to_csv("table_123_unormalized.csv")
syn.store(sc.File("table_123_unormalized.csv", parent=MANIFEST_STORE_ID))
