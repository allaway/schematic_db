"""
A config for database specific items
"""
from typing import Optional, Any
from schematic_db.db_config import DBForeignKey


class DatabaseObjectConfig:  # pylint: disable=too-few-public-methods
    """A config for database specific items for one object"""

    def __init__(
        self,
        name: str,
        primary_key: Optional[str] = None,
        foreign_keys: Optional[list[dict[str, str]]] = None,
        indices: Optional[list[str]] = None,
    ) -> None:
        """
        Init
        """
        self.name = name
        self.primary_key = primary_key
        self.indices = indices
        if foreign_keys is None:
            self.foreign_keys = None
        else:
            self.foreign_keys = [
                DBForeignKey(
                    name=key["attribute_name"],
                    foreign_object_name=key["foreign_object_name"],
                    foreign_attribute_name=key["foreign_attribute_name"],
                )
                for key in foreign_keys
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

    def get_foreign_keys(self, object_name: str) -> Optional[list[DBForeignKey]]:
        """Gets the foreign keys for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[DBForeignKey]]: The foreign keys
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.foreign_keys

    def get_indices(self, object_name: str) -> Optional[list[str]]:
        """Gets the attributes to be indexed for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[str]]: The list of attributes
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.indices

    def _get_object_by_name(self, object_name: str) -> Optional[DatabaseObjectConfig]:
        objects = [obj for obj in self.objects if obj.name == object_name]
        if len(objects) == 0:
            return None
        return objects[0]
