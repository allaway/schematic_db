"""Functions that interact with the schematic API"""

import pickle
import requests
import networkx
import pandas

# Currently this is the url for the API when running locally.
API_URL = "http://0.0.0.0:3001"
API_SERVER = "v1"


def create_schematic_api_response(
    endpoint_path: str, params: dict
) -> requests.Response:
    """Performs a GET request on the schematic API

    Args:
        endpoint_path (str): The path for the endpoint in the schematic API
        params (dict): The parameters in dict form for the requested endpoint

    Returns:
        requests.Response: The response from the API
    """
    endpoint_url = f"{API_URL}/{API_SERVER}/{endpoint_path}"
    return requests.get(endpoint_url, params=params, timeout=30)


def find_class_specific_properties(schema_url: str, schema_class: str) -> list[str]:
    """Find properties specifically associated with a given class

    Args:
        schema_url (str): Data Model URL
        schema_class (str): _description_

    Returns:
        list[str]: _description_
    """
    params = {"schema_url": schema_url, "schema_class": schema_class}
    response = create_schematic_api_response(
        "explorer/find_class_specific_properties", params
    )
    return response.json()


def get_node_dependencies(
    schema_url: str,
    source_node: str,
    return_display_names: bool = True,
    return_schema_ordered: bool = True,
) -> list[str]:
    """Get the immediate dependencies that are related to a given source node

    Args:
        schema_url (str): Data Model URL
        source_node (str): _description_
        return_display_names (bool, optional): _description_. Defaults to True.
        return_schema_ordered (bool, optional): _description_. Defaults to True.

    Returns:
        list[str]: _description_
    """
    params = {
        "schema_url": schema_url,
        "source_node": source_node,
        "return_display_names": return_display_names,
        "return_schema_ordered": return_schema_ordered,
    }
    response = create_schematic_api_response("explorer/get_node_dependencies", params)
    return response.json()


def get_node_range(
    schema_url: str, node_label: str, return_display_names: bool = True
) -> list[str]:
    """Get all the valid values that are associated with a node label

    Args:
        schema_url (str): Data Model URL
        node_label (str): Label/display name for the node to get values for
        return_display_names (bool, optional): If true returns the display names of the nodes.
            Defaults to True.

    Returns:
        list[str]: Valid values that are associated with a node label
    """
    params = {
        "schema_url": schema_url,
        "node_label": node_label,
        "return_display_names": return_display_names,
    }
    response = create_schematic_api_response("explorer/get_node_range", params)
    return response.json()


def get_property_label_from_display_name(
    schema_url: str, display_name: str, strict_came_case: bool = True
) -> list[str]:
    """Converts a given display name string into a proper property label string

    Args:
        schema_url (str): Data Model URL
        display_name (str): The display name to be converted
        strict_came_case (bool, optional): If true the more strict way of converting
            to camel case is used. Defaults to True.

    Returns:
        list[str]: _description_
    """
    params = {
        "schema_url": schema_url,
        "display_name": display_name,
        "strict_came_case": strict_came_case,
    }
    response = create_schematic_api_response(
        "explorer/get_property_label_from_display_name", params
    )
    return response.json()


def get_graph_by_edge_type(schema_url: str, relationship: str) -> list[tuple[str, str]]:
    """Get a subgraph containing all edges of a given type (aka relationship)

    Args:
        schema_url (str): Data Model URL
        relationship (str): Relationship (i.e. parentOf, requiresDependency,
            rangeValue, domainValue)

    Returns:
        list[tuple[str, str]]: A subgraph in the form of a list of tuples.
    """
    params = {"schema_url": schema_url, "relationship": relationship}
    response = create_schematic_api_response("schemas/get/graph_by_edge_type", params)
    return response.json()


def get_schema(schema_url: str) -> networkx.MultiDiGraph:
    """Return schema as a pickle file

    Args:
        schema_url (str): Data Model URL

    Returns:
        str: The path to the downloaded pickle file.
    """
    params = {"schema_url": schema_url}
    response = create_schematic_api_response("schemas/get/schema", params)
    schema_path = response.text
    with open(schema_path, "rb") as file:
        schema = pickle.load(file)
    return schema


def get_project_manifests(
    input_token: str, project_id: str, asset_view: str
) -> list[dict[str:str]]:
    """Gets all metadata manifest files across all datasets in a specified project.

    Args:
        input_token (str): access token
        project_id (str): Project ID
        asset_view (str): ID of view listing all project data assets. For example,
            for Synapse this would be the Synapse ID of the fileview listing all
            data assets for a given project.(i.e. master_fileview in config.yml)

    Returns:
        list[dict[str:str]]: A list of dictionaries where each dict represents a manifest.
    """
    params = {
        "input_token": input_token,
        "project_id": project_id,
        "asset_view": asset_view,
    }
    response = create_schematic_api_response("storage/project/manifests", params)
    manifests = [
        {
            "dataset_id": item[0][0],
            "dataset_name": item[0][1],
            "manifest_id": item[1][0],
            "manifest_name": item[1][1],
            "component_name": item[2][0],
        }
        for item in response.json()
    ]
    return manifests


def get_manifest(
    input_token: str, dataset_id: str, asset_view: str
) -> pandas.DataFrame:
    """Downloads a manifest as a pd.dataframe

    Args:
        input_token (str): Access token
        dataset_id (str): The id of the dataset the manifest part of
        asset_view (str): The id of the view listing all project data assets. For example,
            for Synapse this would be the Synapse ID of the fileview listing all
            data assets for a given project.(i.e. master_fileview in config.yml)

    Returns:
        pd.DataFrame: The manifest in dataframe form
    """
    params = {
        "input_token": input_token,
        "dataset_id": dataset_id,
        "asset_view": asset_view,
        "as_json": True,
    }
    response = create_schematic_api_response("manifest/download", params)
    return pandas.DataFrame(response.json())
