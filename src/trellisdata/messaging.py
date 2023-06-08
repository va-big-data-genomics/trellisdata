# This module replaces the "messages.py" module
# that had a class based solution to sending messages with
# function based approach. Instead of creating objects
# to store and manipulate data that will transmitted to
# other serverless functions, data will be manipulated
# by these functions.

import json
import neo4j
import base64

from neo4j.graph import Graph

def _get_node_dict(node):
    """Convert neo4j.graph.Node object into a dictionary.

    neo4j.graph.Node source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/graph/__init__.py

    Args:
        node (neo4j.graph.Node): Object with node metadata.

    Returns:
        node_dict (dict): Dictionary of metadata stored in the Node object.
    """

    node_dict = {
    	"id_": node.id,
        "element_id": node.element_id,
        "labels": list(node.labels),
        "properties": dict(node.items())
    }
    return node_dict

def _get_relationship_dict(relationship):
    """Convert neo4j.graph.Relationship object into dictionary.

    neo4j.graph.Relationship source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/graph/__init__.py

    Args: 
        relationship (neo4j.graph.Relationship): Object with relationship metadata.

    Returns: 
        relationship_dict (dict): Dictionary of metadata stored in Relationship object.
    """

    # Requirements for relationship dict are based on the
    # _GraphHydrator.hydrate_relationship() method.
    # Source: https://github.com/neo4j/neo4j-python-driver/blob/a8a2b04da36662f0e8d3a0a2c3a9af6e7b9d2f2a/src/neo4j/_codec/hydration/v1/hydration_handler.py#L91
    relationship_dict = {
        "id_": relationship.id,
        "n0_id": relationship.start_node.id,
        "n1_id": relationship.end_node.id,
        "type_": relationship.type,
        "properties": dict(relationship.items()),
        "element_id": relationship.element_id,
        "n0_element_id": relationship.start_node.element_id,
        "n1_element_id": relationship.end_node.element_id
        #"start_node": _get_node_dict(relationship.start_node),
        #"end_node": _get_node_dict(relationship.end_node),
    }
    return relationship_dict

def translate_graph_to_json(graph):
    nodes = []
    relationships = []

    for node in graph.nodes:
        node_dict = _get_node_dict(node)
        nodes.append(node_dict)

    for rel in graph.relationships:
        rel_dict = _get_relationship_dict(rel)
        relationships.append(rel_dict)
    
    # Create a dictionary representing the graph
    graph_dict = {"nodes": nodes, "relationships": relationships}

    # Convert the graph dictionary to JSON
    graph_json = json.dumps(graph_dict)

    return graph_json