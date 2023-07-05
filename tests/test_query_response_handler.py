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


#from unittest import TestCase

#from neo4j import GraphDatabase

import trellisdata as trellis
#import check_triggers

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

class TestTranslateGraphToJson:

    alice_struct = Structure(b'N', 123, ["Person"], {"name": "Alice"}, "abc")
    #bob_struct
    #alice = hydration_scope.hydration_hooks[Structure](struct)

    """
    graph_hydrator = _GraphHydrator()
    alice = graph_hydrator.hydrate_node(
                id_ = 157,
                labels = ["Person"],
                properties = {"name": "Alice"})
    bob = graph_hydrator.hydrate_node(
                id_ = 158,
                labels = ["Person"],
                properties = {"name": "Bob"})
    relationship = graph_hydrator.hydrate_relationship(
                        id_ = 78,
                        n0_id = 157,
                        n1_id = 158,
                        type_ = "KNOWS",
                        properties = {"since": 1999})
    """
  
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
            "relationship": Structure(b'R', 125, 123, 124, "KNOWS", {"since": 1999},
                           "abc", "abd", "ghi")
        }

    """
    @classmethod
    def hydration_scope(cls):
        hydrator = HydrationHandler().new_hydration_scope().hydration_hooks[Structure] 
    """

    def test_can_hydrate_node_structure(self, hydration_scope, graph_entities):
        alice = hydration_scope.hydration_hooks[Structure](graph_entities["alice"])

        assert isinstance(alice, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert alice.id == 123
        assert alice.element_id == "abc"
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"

    """
    @classmethod
    def test_translate_node_to_json(cls):
        # Translate a neo4j.Graph object to a JSON string
        graph_json = trellis.messaging.translate_graph_to_json(cls.graph_hydrator.graph)
        
        graph_dict = json.loads(graph_json)
        assert len(graph_dict['nodes']) == 1
        assert len(graph_dict['relationships']) == 0

        nodes = graph_dict['nodes']
        assert nodes[0]['properties']['name'] == 'Alice'
    """

    #@classmethod
    #def test_translate_relationship_to_json(cls):
    #   graph_json = trellis.messaging.translate_graph_to_json(cls.graph)