#!/usr/bin/env python3

import re
import pdb
import json
import mock
import neo4j
import base64
import pytest

from neo4j._codec.hydration.v2 import HydrationHandler
from neo4j._codec.packstream import Structure

#from ..v1.test_graph_hydration import TestGraphHydration as _TestGraphHydration

from neo4j._codec.hydration.v1.hydration_handler import _GraphHydrator

from neo4j.graph import (
    Graph,
    Node,
    Relationship,
)

from neo4j import ResultSummary
from neo4j.addressing import Address

import trellisdata as trellis

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

# Tests adapted from the Neo4j v5.0 Python driver
# Source: https://github.com/neo4j/neo4j-python-driver/blob/5.0/tests/unit/common/codec/hydration/v2/test_graph_hydration.py
class TestTranslateGraphToJson:

    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    @pytest.fixture
    def hydration_scope(self, hydration_handler):
        return hydration_handler.new_hydration_scope()
    
    @pytest.fixture
    def graph_entities(self):
        return {
            "alice":  Structure(b'N', 123, ["Person"], {"name": "Alice"}, "abc"),
            "bob":  Structure(b'N', 124, ["Person"], {"name": "Bob"}, "abd"),
            "relationship": Structure(b'R', 456, 123, 124, "KNOWS", {"since": 1999},
                           "ghi", "abc", "abd",)
        }

    # Validate graph hydration method
    def test_can_hydrate_node_structure(self, hydration_scope, graph_entities):
        alice = hydration_scope.hydration_hooks[Structure](graph_entities["alice"])

        assert isinstance(alice, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert alice.id == 123
        assert alice.element_id == "abc"
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    # Validate graph hydration method
    def test_can_hydrate_relationship_structure(self, hydration_scope, graph_entities):
        rel = hydration_scope.hydration_hooks[Structure](graph_entities["relationship"])

        assert isinstance(rel, Relationship)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.id == 456
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.start_node.id == 123
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert rel.end_node.id == 124
        # for backwards compatibility, the driver should compute the element_id
        assert rel.element_id == "ghi"
        assert rel.start_node.element_id == "abc"
        assert rel.end_node.element_id == "abd"
        assert rel.type == "KNOWS"
        assert set(rel.keys()) == {"since"}
        assert rel.get("since") == 1999

    def test_translate_relationship_to_json(self, graph_entities):
        # Populate neo4j.Graph object
        hydration_handler = HydrationHandler()
        hydration_scope = hydration_handler.new_hydration_scope()
        for value in graph_entities.values():
            hydration_scope.hydration_hooks[Structure](value)
        original_graph = hydration_scope.get_graph()

        # Use Trellis to translate graph object to JSON
        graph_json = trellis.messaging.translate_graph_to_json(original_graph)

        graph_dict = json.loads(graph_json)

        hydration_scope = hydration_handler.new_hydration_scope()
        for node in graph_dict['nodes']:
            hydration_scope._graph_hydrator.hydrate_node(**node)
        for rel in graph_dict['relationships']:
            hydration_scope._graph_hydrator.hydrate_relationship(**rel)
        reconstituted_graph = hydration_scope.get_graph()
        
        # Can't use the built in equality method for comparing
        # graph entities because it also compares the graphs
        # and these entities exist in different graph objects
        original_nodes = {}
        for node in original_graph.nodes:
            original_nodes[node.element_id] = node
        reconstituted_nodes = {}
        for node in reconstituted_graph.nodes:
            reconstituted_nodes[node.element_id] = node

        for key in original_nodes.keys():
            og_node = original_nodes[key]
            new_node = reconstituted_nodes[key]

            # Compare number of node properties
            assert len(og_node) == len(new_node)

            assert og_node.labels == new_node.labels
            for property_name in iter(og_node):
                assert og_node[property_name] == new_node[property_name]

        original_rels = {}
        for rel in original_graph.relationships:
            original_rels[rel.element_id] = rel
        reconstituted_rels = {}
        for rel in reconstituted_graph.relationships:
            reconstituted_rels[rel.element_id] = rel

        assert len(original_rels) == len(reconstituted_rels)

        for key in original_rels.keys():
            og_rel = original_rels[key]
            new_rel = reconstituted_rels[key]

            # Compare number of relationship properties
            assert len(og_rel) == len(new_rel)

            assert og_rel.type == new_rel.type
            for property_name in iter(og_rel):
                assert og_rel[property_name] == new_rel[property_name]
            assert og_rel.start_node.element_id == new_rel.start_node.element_id
            assert og_rel.end_node.element_id == new_rel.end_node.element_id


        #pdb.set_trace()
        # Problem is now I can't use the same graph hydration method

        # How am I going to do this?
        # The HydrationHandler.new_hydration_scope() function returns
        # a HydrationScope object that is initialized with a 
        # GraphHydrator object. The HydrationScope object has a
        # _graph_hydrator attribute. And the graph struct_hydration_functions
        # just reference the hydration methods from the GraphHydrator object.
        # So...
        # I think I can call the GraphHydrator methods of the
        # GraphHydrator object embedded in the HydrationScope object.
        # HydrationScope

class TestTranslateJsonToGraph:
    def test_translate_json_relationship_to_graph(self, relationship_json):
        pass