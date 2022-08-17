"""Table config objects
These are a set of object for defining a database table in a dialect agnostic way.
"""
from dataclasses import dataclass
from typing import List
from enum import Enum


class DBDatatype(Enum):
    """A generic datatype that should be supported by all database types."""

    TEXT = "text"
    DATE = "date"
    INT = "int"
    FLOAT = "float"
    BOOLEAN = "boolean"


@dataclass
class DBAttributeConfig:
    """A config for a table attribute(column)."""

    name: str
    datatype: DBDatatype

    def __post_init__(self):
        if not isinstance(self.datatype, DBDatatype):
            raise TypeError(f"Param datatype is not of type DBDatatype:{self.datatype}")


@dataclass
class DBForeignKey:
    """A foreign key in a database object attribute."""

    name: str
    foreign_object_name: str
    foreign_attribute_name: str


@dataclass
class DBObjectConfig:
    """A config for a generic database object."""

    name: str
    attributes: List[DBAttributeConfig]
    primary_keys: List[str]
    foreign_keys: List[DBForeignKey]

    def __post_init__(self):
        self._check_attributes()
        self._check_primary_keys()
        self._check_foreign_keys()

    def get_attribute_names(self) -> List[str]:
        """Returns a list of names of the attributes

        Returns:
            List[str]: A list of names of the attributes
        """
        return [att.name for att in self.attributes]

    def get_foreign_key_names(self) -> List[str]:
        """Returns a list of names of the foreign keys

        Returns:
            List[str]: A list of names of the foreign keys
        """
        return [key.name for key in self.foreign_keys]

    def get_foreign_key_by_name(self, name: str) -> DBForeignKey:
        """Returns foreign key

        Args:
            name (str): name of the foreign key

        Returns:
            DBForeignKey: The foreign key asked for
        """
        return [key for key in self.foreign_keys if key.name == name][0]

    def _check_attributes(self):
        if not isinstance(self.attributes, List):
            raise TypeError(f"Param attributes is not of type List:{self.attributes}")
        if len(self.attributes) == 0:
            raise ValueError("Param attributes is empty.")
        for item in self.attributes:
            if not isinstance(item, DBAttributeConfig):
                raise TypeError(
                    f"Item in param attributes is not of type DBAttributeConfig:{item}"
                )

    def _check_primary_keys(self):
        if not isinstance(self.primary_keys, List):
            raise TypeError(
                f"Param primary_keys is not of type List:{self.primary_keys}"
            )
        if len(self.primary_keys) == 0:
            raise ValueError("Param primary_keys is empty.")
        for key in self.primary_keys:
            self._check_primary_key(key)

    def _check_primary_key(self, key):
        if key not in self.get_attribute_names():
            raise ValueError(
                f"Item in param primary_keys is missing from param attributes:{key}"
            )

    def _check_foreign_keys(self):
        if not isinstance(self.foreign_keys, List):
            raise TypeError(
                f"Param foreign_keys is not of type List:{self.foreign_keys}"
            )
        for key in self.foreign_keys:
            self._check_foreign_key(key)

    def _check_foreign_key(self, key):
        if not isinstance(key, DBForeignKey):
            raise TypeError(
                f"Key in param foreign_keys is not of type DBForeignKey:{key}"
            )
        if key.name not in self.get_attribute_names():
            raise ValueError(
                f"Key in param foreign_keys is missing from param attributes:{key}"
            )
        if key.foreign_object_name == self.name:
            raise ValueError(
                f"Key in param foreign_keys references its own table:{key}"
            )


@dataclass
class DBObjectConfigList:
    """A group of configs for generic database tables."""

    configs: List[DBObjectConfig]

    def __post_init__(self):
        if not isinstance(self.configs, List):
            raise TypeError(f"Param configs is not of type List:{self.configs}")

        for config in self.configs:
            if not isinstance(config, DBObjectConfig):
                raise TypeError(
                    f"Item in param configs is not of type DBObjectConfig:{config}"
                )

        for config in self.configs:
            self._check_foreign_keys(config)

    def get_config_names(self) -> List[str]:
        """Returns a list of names of the configs

        Returns:
            List[str]: A list of names of the configs
        """
        return [config.name for config in self.configs]

    def get_config_by_name(self, name: str) -> DBObjectConfig:
        """Returns the config

        Args:
            name (str): name of the config

        Returns:
            DBObjectConfig: The DBObjectConfig asked for
        """
        return [config for config in self.configs if config.name == name][0]

    def _check_foreign_keys(self, config):
        for key in config.foreign_keys:
            self._check_foreign_key_object(config, key)
            self._check_foreign_key_attribute(config, key)

    def _check_foreign_key_object(self, config, key):
        if key.foreign_object_name not in self.get_config_names():
            msg = (
                f"Foreign key in config does not exist in foreign object:"
                f"foreign_key:{key}; config.name:{config.name}"
            )
            raise ValueError(msg)

    def _check_foreign_key_attribute(self, config, key):
        foreign_config = self.get_config_by_name(key.foreign_object_name)
        if key.foreign_attribute_name not in foreign_config.get_attribute_names():
            msg = (
                f"Foreign key attribute in config does not exist in foreign object:"
                f"foreign_key:{key}; config.name:{config.name};"
                f"foreign object:{foreign_config.name}."
            )
            raise ValueError(msg)
