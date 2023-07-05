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
from neo4j._codec.hydration.v1.hydration_handler import _GraphHydrator

from neo4j._async_compat.util import Util

from neo4j.graph import (
    Graph,
    Node,
    Relationship,
)
from neo4j import Record

from neo4j import (
    Result,
    ResultSummary,
    Address,
    ServerInfo,
    Version,
)
#from neo4j.addressing import Address

import trellisdata as trellis

from trellisdata.messaging import (
    translate_record_to_json,
    translate_json_to_graph
    )

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

# Source: https://github.com/neo4j/neo4j-python-driver/blob/5.0/tests/unit/sync/work/test_result.py

def noop(*_, **__):
    pass

class ConnectionStub:
    class Message:
        def __init__(self, message, *args, **kwargs):
            self.message = message
            self.args = args
            self.kwargs = kwargs

        def _cb(self, cb_name, *args, **kwargs):
            # print(self.message, cb_name.upper(), args, kwargs)
            cb = self.kwargs.get(cb_name)
            Util.callback(cb, *args, **kwargs)

        def on_success(self, metadata):
            self._cb("on_success", metadata)

        def on_summary(self):
            self._cb("on_summary")

        def on_records(self, records):
            self._cb("on_records", records)

        def __eq__(self, other):
            return self.message == other

        def __repr__(self):
            return "Message(%s)" % self.message

    def __init__(self, records=None, run_meta=None, summary_meta=None,
                 force_qid=False):
        self._multi_result = isinstance(records, (list, tuple))
        if self._multi_result:
            self._records = records
            self._use_qid = True
        else:
            self._records = records,
            self._use_qid = force_qid
        self.fetch_idx = 0
        self._qid = -1
        self.most_recent_qid = None
        self.record_idxs = [0] * len(self._records)
        self.to_pull = [None] * len(self._records)
        self._exhausted = [False] * len(self._records)
        self.queued = []
        self.sent = []
        self.run_meta = run_meta
        self.summary_meta = summary_meta
        ConnectionStub.server_info.update({"server": "Neo4j/4.3.0"})
        self.unresolved_address = None
        self._new_hydration_scope_called = False

    def send_all(self):
        self.sent += self.queued
        self.queued = []

    def fetch_message(self):
        if self.fetch_idx >= len(self.sent):
            pytest.fail("Waits for reply to never sent message")
        msg = self.sent[self.fetch_idx]
        if msg == "RUN":
            self.fetch_idx += 1
            self._qid += 1
            meta = {"fields": self._records[self._qid].fields,
                    **(self.run_meta or {})}
            if self._use_qid:
                meta.update(qid=self._qid)
            msg.on_success(meta)
        elif msg == "DISCARD":
            self.fetch_idx += 1
            qid = msg.kwargs.get("qid", -1)
            if qid < 0:
                qid = self._qid
            self.record_idxs[qid] = len(self._records[qid])
            msg.on_success(self.summary_meta or {})
            msg.on_summary()
        elif msg == "PULL":
            qid = msg.kwargs.get("qid", -1)
            if qid < 0:
                qid = self._qid
            if self._exhausted[qid]:
                pytest.fail("PULLing exhausted result")
            if self.to_pull[qid] is None:
                n = msg.kwargs.get("n", -1)
                if n < 0:
                    n = len(self._records[qid])
                self.to_pull[qid] = \
                    min(n, len(self._records[qid]) - self.record_idxs[qid])
                # if to == len(self._records):
                #     self.fetch_idx += 1
            if self.to_pull[qid] > 0:
                record = self._records[qid][self.record_idxs[qid]]
                self.record_idxs[qid] += 1
                self.to_pull[qid] -= 1
                msg.on_records([record])
            elif self.to_pull[qid] == 0:
                self.to_pull[qid] = None
                self.fetch_idx += 1
                if self.record_idxs[qid] < len(self._records[qid]):
                    msg.on_success({"has_more": True})
                else:
                    msg.on_success(
                        {"bookmark": "foo", **(self.summary_meta or {})}
                    )
                    self._exhausted[qid] = True
                    msg.on_summary()

    def fetch_all(self):
        while self.fetch_idx < len(self.sent):
            self.fetch_message()

    def run(self, *args, **kwargs):
        self.queued.append(ConnectionStub.Message("RUN", *args, **kwargs))

    def discard(self, *args, **kwargs):
        self.queued.append(ConnectionStub.Message("DISCARD", *args, **kwargs))

    def pull(self, *args, **kwargs):
        self.queued.append(ConnectionStub.Message("PULL", *args, **kwargs))

    server_info = ServerInfo(Address(("bolt://localhost", 7687)), Version(4, 3))

    def defunct(self):
        return False

    def new_hydration_scope(self):
        class FakeHydrationScope:
            hydration_hooks = None
            dehydration_hooks = None

            def get_graph(self):
                return Graph()

        if len(self._records) > 1:
            return FakeHydrationScope()
        assert not self._new_hydration_scope_called
        assert self._records
        self._new_hydration_scope_called = True
        return self._records[0].hydration_scope

class Records:
    def __init__(self, fields, records):
        """
        fields (list)
        records (list of lists?)
        """
        self.fields = tuple(fields)
        self.hydration_scope = HydrationHandler().new_hydration_scope()
        self.records = tuple(records)
        assert all(len(self.fields) == len(r) for r in self.records)

        self._hydrate_records()

    def _hydrate_records(self):
        def _hydrate(value):
            if isinstance(value, (list, tuple)):
                value = type(value)(_hydrate(v) for v in value)
            elif isinstance(value, dict):
                value = {k: _hydrate(v) for k, v in value.items()}
            if type(value) in self.hydration_scope.hydration_hooks:
                return self.hydration_scope.hydration_hooks[type(value)](value)
            return value

        self.records = tuple(_hydrate(r) for r in self.records)

    def __len__(self):
        return self.records.__len__()

    def __iter__(self):
        return self.records.__iter__()

    def __getitem__(self, item):
        return self.records.__getitem__(item)


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
        #pdb.set_trace()

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

    @pytest.fixture
    def graph_json(self):
        return '{"nodes": [{"id_": 123, "element_id": "abc", "labels": ["Person"], "properties": {"name": "Alice"}}, {"id_": 124, "element_id": "abd", "labels": ["Person"], "properties": {"name": "Bob"}}], "relationships": [{"id_": 456, "n0_id": 123, "n1_id": 124, "type_": "KNOWS", "properties": {"since": 1999}, "element_id": "ghi", "n0_element_id": "abc", "n1_element_id": "abd"}]}'
    
    def test_translate_json_relationship_to_graph(self, graph_json):
        graph = trellis.messaging.translate_json_to_graph(graph_json)
        
        nodes = list(graph.nodes)
        assert len(nodes) == 2
        assert nodes[0].element_id == 'abc'
        assert nodes[1].element_id == 'abd'

        relationships = list(graph.relationships)
        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.element_id == 'ghi'
        assert rel.type == 'KNOWS'
        assert rel.get('since') == 1999
        assert rel.start_node.element_id == 'abc'
        assert rel.end_node.element_id == 'abd'

class TestTranslateResultSummaryToJson:

    @pytest.fixture
    def result_summary(self):
        # Need to generate a mock result summary
        address = Address(("bolt://localhost", 7687))
        version = neo4j.Version(3, 0)
        server_info = neo4j.ServerInfo(address, version)

        result_metadata = {
                           "server": server_info
        }
        result_summary = ResultSummary("localhost", **result_metadata)
        #pdb.set_trace()
        return result_summary

    # Todo: develop testing suite.
    # Neo4j reference: https://github.com/neo4j/neo4j-python-driver/blob/4.4/tests/unit/work/test_result.py#LL340C2-L340C2
    def test_translate_basic_summary(self, result_summary):
        summary = trellis.messaging.translate_result_summary_to_json(result_summary)

class TestTranslateRecordsToJson:

    @pytest.fixture
    def graph_structures(self):
        return {
            "alice":  Structure(b'N', 123, ["Person"], {"name": "Alice"}, "abc"),
            "bob":  Structure(b'N', 124, ["Person"], {"name": "Bob"}, "abd"),
            "relationship": Structure(b'R', 456, 123, 124, "KNOWS", {"since": 1999},
                           "ghi", "abc", "abd",)
        }

    @pytest.fixture
    def records(self, graph_structures):
        records = Records(
                       fields = graph_structures.keys(),
                       records = [list(graph_structures.values())])
        connection = ConnectionStub(records=records)
        result = Result(connection, 2, noop, noop)
        result._run("CYPHER", {}, None, None, "r", None, None, None)
        return result.fetch(10)

    def test_hydrate_records(self, records):
        assert len(records) == 1
        record = records[0]

        assert isinstance(record, Record)
        assert isinstance(record['alice'], Node)
        assert isinstance(record['bob'], Node)
        assert isinstance(record['relationship'], Relationship)
    
        assert record['alice']['name'] == 'Alice'
        assert record['bob']['name'] == 'Bob'
        assert record['relationship'].type == 'KNOWS'

    def test_translate_graph_record_to_json(self, records):
        record = records[0]
        graph_json = translate_record_to_json(record)

        graph = translate_json_to_graph(graph_json)
        
        nodes = list(graph.nodes)
        assert len(nodes) == 2
        assert nodes[0].element_id == 'abc'
        assert nodes[1].element_id == 'abd'

        relationships = list(graph.relationships)
        assert len(relationships) == 1
        rel = relationships[0]
        assert rel.element_id == 'ghi'
        assert rel.type == 'KNOWS'
        assert rel.get('since') == 1999
        assert rel.start_node.element_id == 'abc'
        assert rel.end_node.element_id == 'abd'
        