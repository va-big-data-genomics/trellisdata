# This module replaces the "messages.py" module
# that had a class based solution to sending messages with
# function based approach. Instead of creating objects
# to store and manipulate data that will transmitted to
# other serverless functions, data will be manipulated
# by these functions.

import json
import base64

from neo4j._codec.hydration.v2 import HydrationHandler

from neo4j.graph import (
    Node,
    Relationship
)

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
    graph_dict = {
        "nodes": nodes,
        "relationships": relationships}

    # Convert the graph dictionary to JSON
    graph_json = json.dumps(graph_dict)

    return graph_json

def translate_json_to_graph(graph_json):
    hydration_handler = HydrationHandler()
    hydration_scope = hydration_handler.new_hydration_scope()
    
    graph_dict = json.loads(graph_json)

    hydration_scope = hydration_handler.new_hydration_scope()
    for node in graph_dict['nodes']:
        hydration_scope._graph_hydrator.hydrate_node(**node)
    for rel in graph_dict['relationships']:
        hydration_scope._graph_hydrator.hydrate_relationship(**rel)
    return hydration_scope.get_graph()

def translate_record_to_json(record):
    """ Adapted from the RecordExporter class in neo4j.
    RecordExporter source: https://github.com/neo4j/neo4j-python-driver/blob/5.0/src/neo4j/_data.py#LL276C1-L305C21
    """

    nodes = []
    relationships = []

    for key, value in record.items():
        if isinstance(value, Node):
            node_dict = _get_node_dict(value)
            nodes.append(node_dict)
        elif isinstance(value, Relationship):
            rel_dict =  _get_relationship_dict(value)
            relationships.append(rel_dict)

    graph_dict = {
        "nodes": nodes,
        "relationships": relationships
    }

    graph_json = json.dumps(graph_dict)
    return graph_json


# Todo: Convert this to work on graph json. Instead of
# changing behavior based on "pattern", implement a fixed
# logic for splitting all nodes and relationships. If a 
# node is in a relationship it should be removed from the
# node pool.
def split_json_graph_entities(graph_json):
    """
    Todo: Convert this to work on graph json. Instead of
    changing behavior based on "pattern", implement a fixed
    logic for splitting all nodes and relationships. If a 
    node is in a relationship it should be removed from the
    node pool.
    """
    #summary_dict = self._get_result_summary_dict(self.result_summary)

    # Create a dictionary of nodes, indexed by element_id
    # Every node in a relationship will be removed from this
    # dictionary, by their element_id.
    nodes_by_id = {}
    for node in graph_json['nodes']:
        nodes_by_id[node['element_id']] = node

    for relationship in graph_json['relationships']:
        start_node = relationship['n0_element_id']
        end_node = relationship['n1_element_id']
        
        del nodes_by_id[start_node]
        del nodes_by_id[end_node]

    # Return a list of dictionaries corresponding
    if self.pattern == "node":
        for node in self.nodes:
            message = super().format_json_header()
            body = {
                    "body": {
                        "queryName": self.query_name,
                        "jobRequest": self.job_request,
                        "nodes": [self._get_node_dict(node)],
                        "relationship": {},
                        "resultSummary": summary_dict
                    }
            }
            message.update(body)
            yield message
    elif self.pattern == "relationship":
        for relationship in self.relationships:
            message = super().format_json_header()
            body = {
                    "body": {
                        "queryName": self.query_name,
                        "jobRequest": self.job_request,
                        "nodes": [],
                        "relationship": self._get_relationship_dict(relationship),
                        "resultSummary": summary_dict
                    }
            }
            message.update(body)
            yield message
    else:
        raise ValueError(f"Pattern '{self.pattern}' not in supported patterns: {self.supported_patterns}.")

def translate_result_summary_to_json(result_summary):
    # Create a copy of the dict so that metadata and server
    # elements are preserved in self.result_summary.
    # ResultSummary source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/work/summary.py
    summary_dict = dict(result_summary.__dict__)
    # The metadata field contains the dictionary of information 
    # that is parsed into attributes of the ResultSummary object.
    del summary_dict['metadata']
    # The server value is an instance of the Server class and
    # its attributes contain more classes which would require
    # parsing to text and I'm not that concerned with the
    # information anyway.
    del summary_dict['server']

    summary_dict['counters'] = summary_dict['counters'].__dict__

    return summary_dict

"""
class Message(object):

    def __init__(
                 self,
                 sender,
                 seed_id,
                 previous_event_id):

        self.message_kind = "unspecifiedWriter"
        self.sender = sender
        self.seed_id = seed_id
        self.previous_event_id = previous_event_id

    def format_json_header(self):

        header = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           }
        }
        return header

class QueryRequest(Message):

    def __init__(
                 self,
                 *, # Begin keyword-only arguments
                 sender,
                 seed_id,
                 previous_event_id,
                 query_name,
                 query_parameters,
                 custom=False,
                 cypher=None,
                 write_transaction=False,
                 aggregate_results=False,
                 publish_to=None,
                 returns={}):

        super().__init__(
                         sender,
                         seed_id,
                         previous_event_id)

        self.message_kind = "queryRequest"
        self.query_name = query_name
        self.query_parameters = query_parameters
        self.custom = custom
        self.cypher = cypher
        self.write_transaction = write_transaction
        self.aggregate_results = aggregate_results
        self.publish_to = publish_to
        self.returns = returns

    def format_json_message(self):
        message = super().format_json_header()
        body = {
                "body": {
                    "queryName": self.query_name,
                    "queryParameters": self.query_parameters,
                    # Adding support for custom queries
                    "custom": self.custom
                }
        }
        message.update(body)

        if self.custom:
            custom_fields = {
                "cypher": self.cypher,
                "writeTransaction": self.write_transaction,
                "aggregateResults": self.aggregate_results,
                "publishTo": self.publish_to,
                "returns": self.returns
            }
            message['body'].update(custom_fields)
        return message

class QueryResponse(Message):

    def __init__(
                 self,
                 *,
                 sender,
                 seed_id,
                 previous_event_id,
                 query_name,
                 result_summary,
                 graph,
                 job_request=None):

        super().__init__(
                         sender,
                         seed_id,
                         previous_event_id)

        self.message_kind = "queryResponse"
        self.query_name = query_name
        self.result_summary = result_summary
        self.graph = graph
        self.nodes = []
        self.relationships = []
        self.supported_patterns = ['node', 'relationship']
        self.job_request = job_request

        if self.graph:
            for node in self.graph.nodes:
                self.nodes.append(node)

            for relationship in self.graph.relationships:
                self.relationships.append(relationship)

        if self.relationships:
            self.pattern = "relationship"
        else:
            self.pattern = "node"

    def return_json_with_all_nodes(self):
        if self.pattern == 'relationship':
            raise ValueError("Cannot use this method to publish relationships. Use 'generate_separate_entity_jsons()' instead.")

        message = super().format_json_header()

        body = {
           "body": {
                    "queryName": self.query_name,
                    "jobRequest": self.job_request,
                    "nodes": [self._get_node_dict(node) for node in self.nodes],
                    # Note: Each relationship triple should be delivered separately using 
                    # the generate_individual_jsons() method.
                    "relationship": {},
                    #"relationships": [self._get_relationship_dict(rel) for rel in self.relationships],
                    "resultSummary": self._get_result_summary_dict(self.result_summary)
           }
        }
        message.update(body)
        return message

    def generate_separate_entity_jsons(self):
        summary_dict = self._get_result_summary_dict(self.result_summary)

        if self.pattern == "node":
            for node in self.nodes:
                message = super().format_json_header()
                body = {
                        "body": {
                            "queryName": self.query_name,
                            "jobRequest": self.job_request,
                            "nodes": [self._get_node_dict(node)],
                            "relationship": {},
                            "resultSummary": summary_dict
                        }
                }
                message.update(body)
                yield message
        elif self.pattern == "relationship":
            for relationship in self.relationships:
                message = super().format_json_header()
                body = {
                        "body": {
                            "queryName": self.query_name,
                            "jobRequest": self.job_request,
                            "nodes": [],
                            "relationship": self._get_relationship_dict(relationship),
                            "resultSummary": summary_dict
                        }
                }
                message.update(body)
                yield message
        else:
            raise ValueError(f"Pattern '{self.pattern}' not in supported patterns: {self.supported_patterns}.")
"""