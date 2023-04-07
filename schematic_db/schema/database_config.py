"""
A config for database specific items
"""
from typing import Optional, Any
from schematic_db.db_schema.db_schema import (
    ForeignKeySchema,
    ColumnSchema,
    ColumnDatatype,
)


DATATYPES = {
    "str": ColumnDatatype.TEXT,
    "float": ColumnDatatype.FLOAT,
    "int": ColumnDatatype.INT,
    "date": ColumnDatatype.DATE,
}


class DatabaseObjectConfig:  # pylint: disable=too-few-public-methods
    """A config for database specific items for one object"""

    def __init__(
        self,
        name: str,
        primary_key: Optional[str] = None,
        foreign_keys: Optional[list[dict[str, str]]] = None,
        columns: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        """
        Init
        """
        self.name = name
        self.primary_key = primary_key
        if foreign_keys is None:
            self.foreign_keys = None
        else:
            self.foreign_keys = [
                ForeignKeySchema(
                    name=key["column_name"],
                    foreign_table_name=key["foreign_table_name"],
                    foreign_column_name=key["foreign_column_name"],
                )
                for key in foreign_keys
            ]
        if columns is None:
            self.columns = None
        else:
            self.columns = [
                ColumnSchema(
                    name=column["column_name"],
                    datatype=DATATYPES[column["datatype"]],
                    required=column["required"],
                    index=column["index"],
                )
                for column in columns
            ]


class DatabaseConfig:
    """A config for database specific items"""

    def __init__(self, objects: list[dict[str, Any]]) -> None:
        """
        Init
        """
        self.objects: list[DatabaseObjectConfig] = [
            DatabaseObjectConfig(**obj) for obj in objects
        ]

    def get_primary_key(self, object_name: str) -> Optional[str]:
        """Gets the primary key for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[str]: The primary key
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.primary_key

    def get_foreign_keys(self, object_name: str) -> Optional[list[ForeignKeySchema]]:
        """Gets the foreign keys for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[ForeignKeySchema]]: The foreign keys
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.foreign_keys

    def get_attributes(self, object_name: str) -> Optional[list[ColumnSchema]]:
        """Gets the attributes for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[ColumnSchema]]: The list of attributes
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.columns

    def get_attribute(
        self, object_name: str, attribute_name: str
    ) -> Optional[ColumnSchema]:
        """Gets the attributes for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[ColumnSchema]]: The list of attributes
        """
        attributes = self.get_attributes(object_name)
        if attributes is None:
            return None
        attributes = [
            attribute for attribute in attributes if attribute.name == attribute_name
        ]
        if len(attributes) == 0:
            return None
        return attributes[0]

    def _get_object_by_name(self, object_name: str) -> Optional[DatabaseObjectConfig]:
        objects = [obj for obj in self.objects if obj.name == object_name]
        if len(objects) == 0:
            return None
        return objects[0]
