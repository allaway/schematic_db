"""DB config
These are a set of classes for defining a database table in a dialect agnostic way.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, TypeVar


class ColumnDatatype(Enum):
    """A generic datatype that should be supported by all database types."""

    TEXT = "text"
    DATE = "date"
    INT = "int"
    FLOAT = "float"
    BOOLEAN = "boolean"


# mypy types so that a class can refer to its own type
X = TypeVar("X", bound="ColumnSchema")
Y = TypeVar("Y", bound="TableSchema")
T = TypeVar("T", bound="DatabaseSchema")


@dataclass
class ColumnSchema:
    """A schema for a table column (attribute)."""

    name: str
    datatype: ColumnDatatype
    required: bool = False
    index: bool = False

    def is_equivalent(self, other: X) -> bool:
        """Use instead of == when determining if schema's are equivalent

        Args:
            other (ColumnSchema): Another ColumnSchema to compare to self

        Returns:
            bool: True if both ColumnSchemas are equivalent
        """

        return all(
            [
                self.name == other.name,
                self.datatype == other.datatype,
                self.required == other.required,
            ]
        )


@dataclass
class ForeignKeySchema:
    """A foreign key in a database schema."""

    name: str
    foreign_table_name: str
    foreign_column_name: str

    def get_column_dict(self) -> dict[str, str]:
        """Returns the foreign key in dict form

        Returns:
            dict[str, str]: A dictionary of the foreign key columns
        """
        return {
            "name": self.name,
            "foreign_table_name": self.foreign_table_name,
            "foreign_column_name": self.foreign_column_name,
        }


class TableColumnError(Exception):
    """TableColumnError"""

    def __init__(self, message: str, table_name: str) -> None:
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}: {self.table_name}"


class TableKeyError(Exception):
    """TableKeyError"""

    def __init__(
        self, message: str, table_name: str, key: Optional[str] = None
    ) -> None:
        self.message = message
        self.table_name = table_name
        self.key = key
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.key is None:
            return f"{self.message}: {self.table_name}"
        return f"{self.message}: {self.table_name}; {self.key}"


@dataclass
class TableSchema:
    """A schema for a database table."""

    name: str
    columns: list[ColumnSchema]
    primary_key: str
    foreign_keys: list[ForeignKeySchema]

    def __post_init__(self) -> None:
        self.columns.sort(key=lambda x: x.name)
        self.foreign_keys.sort(key=lambda x: x.name)
        self._check_columns()
        self._check_primary_key()
        self._check_foreign_keys()

    def __eq__(self, other: Any) -> bool:
        """Overrides the default implementation"""
        return self.get_sorted_columns() == other.get_sorted_columns()

    def is_equivalent(self, other: Y) -> bool:
        """
        Use instead of == when determining if schema's are equivalent
        Args:
            other (TableSchema): Another instance of TableSchema

        Returns:
            bool
        """
        columns_equivalent = all(
            x.is_equivalent(y)
            for x, y in zip(
                self.get_sorted_columns(),
                other.get_sorted_columns(),
            )
        )

        return all(
            [
                columns_equivalent,
                self.name == other.name,
                self.primary_key == other.primary_key,
                self.foreign_keys == other.foreign_keys,
            ]
        )

    def get_sorted_columns(self) -> list[ColumnSchema]:
        """Gets the tables columns sorted by name

        Returns:
            list[ColumnSchema]: Sorted list of columns
        """
        return sorted(self.columns, key=lambda x: x.name)

    def get_column_names(self) -> list[str]:
        """Returns a list of names of the columns

        Returns:
            List[str]: A list of names of the attributes
        """
        return [column.name for column in self.columns]

    def get_foreign_key_dependencies(self) -> list[str]:
        """Returns a list of table names the current object depends on

        Returns:
            list[str]: A list of table names
        """
        return [key.foreign_table_name for key in self.foreign_keys]

    def get_foreign_key_names(self) -> list[str]:
        """Returns a list of names of the foreign keys

        Returns:
            List[str]: A list of names of the foreign keys
        """
        return [key.name for key in self.foreign_keys]

    def get_foreign_key_by_name(self, name: str) -> ForeignKeySchema:
        """Returns foreign key

        Args:
            name (str): name of the foreign key

        Returns:
            ForeignKeySchema: The foreign key asked for
        """
        return [key for key in self.foreign_keys if key.name == name][0]

    def get_column_by_name(self, name: str) -> ColumnSchema:
        """Returns the column

        Args:
            name (str): name of the column

        Returns:
            ColumnSchema: The ColumnSchema asked for
        """
        return [column for column in self.columns if column.name == name][0]

    def _check_columns(self) -> None:
        if len(self.columns) == 0:
            raise TableColumnError("There are no columns", self.name)
        if len(self.get_column_names()) != len(set(self.get_column_names())):
            raise TableColumnError("There are duplicate columns", self.name)

    def _check_primary_key(self) -> None:
        if self.primary_key not in self.get_column_names():
            raise TableKeyError(
                "Primary key is missing from columns", self.name, self.primary_key
            )

    def _check_foreign_keys(self) -> None:
        for key in self.foreign_keys:
            self._check_foreign_key(key)

    def _check_foreign_key(self, key: ForeignKeySchema) -> None:
        if key.name not in self.get_column_names():
            raise TableKeyError(
                "Foreign key is missing from columns", self.name, key.name
            )
        if key.foreign_table_name == self.name:
            raise TableKeyError(
                "Foreign key references its own table", self.name, key.name
            )


class ConfigForeignKeyMissingObjectError(Exception):
    """When a foreign key references an object that doesn't exist"""

    def __init__(
        self, foreign_key: str, object_name: str, foreign_object_name: str
    ) -> None:
        self.message = "Foreign key references object which does not exist in config."
        self.foreign_key = foreign_key
        self.object_name = object_name
        self.foreign_object_name = foreign_object_name
        super().__init__(self.message)

    def __str__(self) -> str:
        msg = (
            f"Foreign key '{self.foreign_key}' in object '{self.object_name}' references object "
            f"'{self.foreign_object_name}' which does not exist in config."
        )
        return msg


class ConfigForeignKeyMissingAttributeError(Exception):
    """When a foreign key references an object attribute the object doesn't have"""

    def __init__(
        self,
        foreign_key: str,
        object_name: str,
        foreign_object_name: str,
        foreign_object_attribute: str,
    ) -> None:
        self.message = "Foreign key references attribute which does not exist."
        self.foreign_key = foreign_key
        self.object_name = object_name
        self.foreign_object_name = foreign_object_name
        self.foreign_object_attribute = foreign_object_attribute
        super().__init__(self.message)

    def __str__(self) -> str:
        msg = (
            f"Foreign key '{self.foreign_key}' in object '{self.object_name}' references "
            f"attribute '{self.foreign_object_attribute}' which does not exist in object"
            f"'{self.foreign_object_name}'"
        )
        return msg


@dataclass
class DatabaseSchema:
    """A group of configs for generic database tables."""

    configs: list[TableSchema]

    def __post_init__(self) -> None:
        for config in self.configs:
            self._check_foreign_keys(config)

    def __eq__(self, other: Any) -> bool:
        """Overrides the default implementation"""
        return self.get_sorted_configs() == other.get_sorted_configs()

    def is_equivalent(self, other: T) -> bool:
        """
        Use instead of == when determining if schema's are equivalent
        Args:
            other (DatabaseSchema): Another instance of DatabaseSchema

        Returns:
            bool
        """
        return all(
            x.is_equivalent(y)
            for x, y in zip(
                self.get_sorted_configs(),
                other.get_sorted_configs(),
            )
        )

    def get_sorted_configs(self) -> list[TableSchema]:
        """Gets the the configs sorted by name

        Returns:
            list[TableSchema]: The list of sorted configs
        """
        return sorted(self.configs, key=lambda x: x.name)

    def get_dependencies(self, object_name: str) -> list[str]:
        """Gets the objects dependencies

        Args:
            object_name (str): The name of the object

        Returns:
            list[str]: A list of objects names the object depends on
        """
        return self.get_config_by_name(object_name).get_foreign_key_dependencies()

    def get_reverse_dependencies(self, object_name: str) -> list[str]:
        """Gets the names of Objects that depend on the input object

        Args:
            object_name (str): The name of the object

        Returns:
            list[str]: A list of object names that depend on the input object
        """
        return [
            config.name
            for config in self.configs
            if object_name in config.get_foreign_key_dependencies()
        ]

    def get_config_names(self) -> list[str]:
        """Returns a list of names of the configs

        Returns:
            List[str]: A list of names of the configs
        """
        return [config.name for config in self.configs]

    def get_config_by_name(self, name: str) -> TableSchema:
        """Returns the config

        Args:
            name (str): name of the config

        Returns:
            TableSchema: The TableSchema asked for
        """
        return [config for config in self.configs if config.name == name][0]

    def _check_foreign_keys(self, config: TableSchema) -> None:
        for key in config.foreign_keys:
            self._check_foreign_key_object(config, key)
            self._check_foreign_key_attribute(config, key)

    def _check_foreign_key_object(
        self, config: TableSchema, key: ForeignKeySchema
    ) -> None:
        if key.foreign_table_name not in self.get_config_names():
            raise ConfigForeignKeyMissingObjectError(
                foreign_key=key.name,
                object_name=config.name,
                foreign_object_name=key.foreign_table_name,
            )

    def _check_foreign_key_attribute(
        self, config: TableSchema, key: ForeignKeySchema
    ) -> None:
        foreign_config = self.get_config_by_name(key.foreign_table_name)
        if key.foreign_column_name not in foreign_config.get_column_names():
            raise ConfigForeignKeyMissingAttributeError(
                foreign_key=key.name,
                object_name=config.name,
                foreign_object_name=key.foreign_table_name,
                foreign_object_attribute=key.foreign_column_name,
            )
