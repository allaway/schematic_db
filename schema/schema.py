"""Schema class"""

from typing import Optional, Callable, Union
import warnings
import networkx
from db_object_config import (
    DBObjectConfigList,
    DBObjectConfig,
    DBForeignKey,
    DBAttributeConfig,
    DBDatatype,
)

from .api_utils import (
    get_graph_by_edge_type,
    find_class_specific_properties,
    get_property_label_from_display_name,
    get_project_manifests,
)


class NoAttributesWarning(Warning):
    """
    Occurs when a database object has no attributes returned from find_class_specific_properties().
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def get_key_attribute(object_name: str) -> str:
    """
    Standard function for getting a key's name(primary or foreign) based on the objects name.
    The Schema class uses this function to get both primary and foreign keys by default.
    Users may want to use a different function.

    Args:
        object_name (str): The name of the object in the database

    Returns:
        str: The name of the key attribute for that database
    """
    return f"{object_name}_id"


def get_manifest_ids_for_object(
    object_name: str, manifests: list[dict[str:str]]
) -> list[str]:
    """Gets the manifest ids from a list of manifests matching the object name

    Args:
        object_name (str): The name of the object to get the manifests for
        manifests (list[dict[str:str]]): A list of manifests in dictionary form

    Returns:
        list[str]: A list of synapse ids for the manifests
    """
    return [
        manifest.get("manifest_id")
        for manifest in manifests
        if manifest.get("component_name") == object_name
        and manifest.get("manifest_id") != ""
    ]


class Schema:
    """
    The Schema object is used  to interact with the Schematic API to create a DBObjectConfigList,
    which contains schemas for each object(table) in a database.
    """

    def __init__(
        self,
        schema_url: str,
        synapse_project_id: str,
        synapse_asset_view_id: str,
        synapse_input_token: str,
        primary_key_getter: Optional[Callable[[str], str]] = get_key_attribute,
        foreign_key_getter: Optional[Callable[[str], str]] = get_key_attribute,
    ) -> None:
        """Init

        The Schema class has two optional arguments primary_key_getter, and foreign_key_getter.
        These are used to determine the names of the attributes that are primary and foreign keys.
        It is assumed that all objects(tables) have one primary key that can be systematically
        determined from the objects name, and that the primary_key_getter will do that.

        By default get_key_attribute is used for primary keys. This assumes that all primary keys
        are of the form "<object_name>_id". For example if the object was named "patient" then the
        primary key would be named "patient_id".

        Also by default get_key_attribute is used for foreign keys. This assumes that all foreign
        keys match the name of the the primary key they refer to.

        If foreign keys do not match the primary key they refer to then the functions would need
        to be different to reflect that.

        Args:
            schema_url (str): A url to the jsonld schema file
            synapse_project_id (str): The synapse id to the project where the manifests are stored.
            synapse_asset_view_id (str): The synapse id to the asset view that tracks the manifests.
            synapse_input_token (str): A synapse token with download permissions for both the
                synapse_project_id and synapse_asset_view_id
            primary_key_getter (Optional[Callable[[str], str]], optional):
                Defaults to get_key_attribute.
            foreign_key_getter (Optional[Callable[[str], str]], optional):
                Defaults to get_key_attribute.
        """
        # retrieve the edges from schematic API and store in networkx.DiGraph()
        subgraph = get_graph_by_edge_type(schema_url, "requiresComponent")
        schema_graph = networkx.DiGraph()
        schema_graph.add_edges_from(subgraph)
        self.schema_graph = schema_graph

        self.schema_url = schema_url
        self.synapse_project_id = synapse_project_id
        self.synapse_asset_view_id = synapse_asset_view_id
        self.synapse_input_token = synapse_input_token
        self.primary_key_getter = primary_key_getter
        self.foreign_key_getter = foreign_key_getter

    def create_db_config(self) -> DBObjectConfigList:
        """Creates the configs for all objects in the database.

        Returns:
            DBObjectConfigList: Configs for all objects in the database.
        """
        all_manifests = get_project_manifests(
            input_token=self.synapse_input_token,
            project_id=self.synapse_project_id,
            asset_view=self.synapse_asset_view_id,
        )
        # order objects so that ones with dependencies come after they depend on
        object_names = list(
            reversed(list(networkx.topological_sort(self.schema_graph)))
        )
        object_configs = [
            self.create_db_object_config(name, all_manifests) for name in object_names
        ]
        object_configs = [config for config in object_configs if config is not None]
        return DBObjectConfigList(object_configs)

    def create_db_object_config(
        self, object_name: str, manifests: list[dict[str:str]]
    ) -> DBObjectConfig:
        """Creates the config for one object in the database.

        Args:
            object_name (str): The name of the object the config will be created for.
            manifests (list[dict[str:str]]): A list of manifests in dictionary form

        Returns:
            DBObjectConfig: The config for the object.
        """
        # Some components will not have any attributes for various reasons
        attributes = self.create_attributes(object_name)
        if not attributes:
            return None
        primary_key = get_property_label_from_display_name(
            self.schema_url, self.primary_key_getter(object_name)
        )
        # primary keys don't always appear in the attributes endpoint
        if primary_key not in [att.name for att in attributes]:
            attributes.append(
                DBAttributeConfig(name=primary_key, datatype=DBDatatype.TEXT)
            )
        # foreign keys don't always appear in the attributes endpoint
        foreign_keys = self.create_foreign_keys(object_name)
        for key in foreign_keys:
            name = key.name
            if name not in [att.name for att in attributes]:
                attributes.append(
                    DBAttributeConfig(name=name, datatype=DBDatatype.TEXT)
                )
        return DBObjectConfig(
            name=object_name,
            manifest_ids=get_manifest_ids_for_object(object_name, manifests),
            attributes=attributes,
            primary_keys=[primary_key],
            foreign_keys=foreign_keys,
        )

    def create_attributes(
        self, object_name: str
    ) -> Union[list[DBAttributeConfig], None]:
        """Create the attributes for the object

        Args:
            object_name (str): The name of the object to create the attributes for

        Returns:
            Union[list[DBAttributeConfig], None]: A list of attributes in DBAttributeConfig form
        """
        attribute_names = find_class_specific_properties(self.schema_url, object_name)
        attributes = [
            DBAttributeConfig(name=name, datatype=DBDatatype.TEXT)
            for name in attribute_names
        ]
        # Some components will not have any attributes for various reasons
        if not attributes:
            warnings.warn(
                NoAttributesWarning(
                    f"Object {object_name} has no attributes, and will be skipped."
                )
            )
            return None
        return attributes

    def create_foreign_keys(self, object_name: str) -> list[DBForeignKey]:
        """Creates a list of foreign keys for an object in the database

        Args:
            object_name (str): The name of the object the config will be created for.

        Returns:
            list[DBForeignKey]: A list of foreign keys for the object.
        """
        neighbor_object_names = list(self.schema_graph.neighbors(object_name))
        foreign_keys = [self.create_foreign_key(name) for name in neighbor_object_names]
        return foreign_keys

    def create_foreign_key(self, foreign_object_name: str) -> DBForeignKey:
        """Creates a foreign key object

        Args:
            foreign_object_name (str): The name of the object the foreign key is referring to.

        Returns:
            DBForeignKey: A foreign key object.
        """
        attribute_name = get_property_label_from_display_name(
            self.schema_url, self.foreign_key_getter(foreign_object_name)
        )
        foreign_attribute_name = get_property_label_from_display_name(
            self.schema_url, self.primary_key_getter(foreign_object_name)
        )
        return DBForeignKey(
            attribute_name,
            foreign_object_name,
            foreign_attribute_name,
        )
