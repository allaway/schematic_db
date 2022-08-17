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
            raise TypeError(f"datatype({self.datatype}) is not of type DBDatatype")

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
        # checking for self.attributes
        if not isinstance(self.attributes, List):
            raise TypeError(f"attributes({self.attributes}) is not of type List")
        for item in self.attributes:
            if not isinstance(item, DBAttributeConfig):
                raise TypeError(f"item in attributes({item}) is not of type DBAttributeConfig")

        # checking for self.primary_keys
        if not isinstance(self.primary_keys, List):
            raise TypeError(f"primary_keys({self.primary_keys}) is not of type List")
        for key in self.primary_keys:
            if key not in self.get_attribute_names():
                raise ValueError(f"key in primary_keys({key}) is missing from attributes")

        # checking for self.foreign_keys
        if not isinstance(self.foreign_keys, List):
            raise TypeError(f"foreign_keys({self.foreign_keys}) is not of type List")
        for key in self.foreign_keys:
            if not isinstance(key, DBForeignKey):
                raise TypeError(f"key in attributes({key}) is not of type DBForeignKey")
            if key.name not in self.get_attribute_names():
                raise ValueError(f"key in foreign_keys({key}) is missing from attributes")

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

    def get_foreign_key_by_name(self, name:str) -> DBForeignKey:
        """Returns foreign key

        Args:
            name (str): name of the foreign key

        Returns:
            DBForeignKey: The foreign key asked for
        """
        return [key for key in self.foreign_keys if key.name == name][0]

@dataclass
class DBObjectConfigList:
    """A group of configs for generic database tables."""

    configs: List[DBObjectConfig]

    def __post_init__(self):
        if not isinstance(self. configs, List):
            raise TypeError(f"configs({self.configs}) is not of type List")

        for config in self.configs:
            if not isinstance(config, DBObjectConfig):
                raise TypeError(f"config in configs({config}) is not of type DBObjectConfig")

        for config in self.configs:
            for key in config.foreign_keys:
                if key.foreign_object_name not in self.get_config_names():
                    raise ValueError(
                        f"foreign_key ({key}) in config({config.name}) \
                            does not exist in any other object."
                    )
                foreign_config = self.get_config_by_name(key.foreign_object_name)
                if key.foreign_attribute_name not in foreign_config.get_attribute_names():
                    raise ValueError(
                        f"foreign_key ({key}) in config({config.name}) \
                            does not exist in foreign object({foreign_config.name})."
                    )

    def get_config_names(self) -> List[str]:
        """Returns a list of names of the configs

        Returns:
            List[str]: A list of names of the configs
        """
        return [config.name for config in self.configs]

    def get_config_by_name(self, name:str) -> DBObjectConfig:
        """Returns the config

        Args:
            name (str): name of the config

        Returns:
            DBObjectConfig: The DBObjectConfig asked for
        """
        return [config for config in self.configs if config.name == name][0]
