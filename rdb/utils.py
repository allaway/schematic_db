"""Utils for the RDB class
"""

import pandas as pd
from db_object_config import DBObjectConfig

def normalize_table(table: pd.DataFrame, table_config: DBObjectConfig) -> pd.DataFrame:
    """_summary_
    """
    column_names_set = {att.name for att in table_config.attributes}
    table_column_names_set = set(list(table.columns))
    normalized_column_names = list(column_names_set.intersection(table_column_names_set))
    missing_primary_keys = []
    for key in table_config.primary_keys:
        if key not in normalized_column_names:
            missing_primary_keys.append(key)
    if len(missing_primary_keys) > 0:
        raise ValueError(f"Manifest missing primary keys: [{', '.join(missing_primary_keys)}]")

    table = table[normalized_column_names]
    table = table.drop_duplicates(subset=table_config.primary_keys)
    table.reset_index(inplace=True, drop=True)

    return table
