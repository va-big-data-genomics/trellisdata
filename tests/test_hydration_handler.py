import pdb
import pytest

from neo4j._codec.hydration.v2 import HydrationHandler
from neo4j._codec.packstream import Structure
from neo4j.graph import (
    Node,
    Relationship,
)

#from ..v1.test_graph_hydration import TestGraphHydration as _TestGraphHydration


class TestGraphHydration:
    @pytest.fixture
    def hydration_handler(self):
        return HydrationHandler()

    @pytest.fixture
    def hydration_scope(self, hydration_handler):
        return hydration_handler.new_hydration_scope()

    def test_can_hydrate_node_structure(self, hydration_scope):
        struct = Structure(b'N', 123, ["Person"], {"name": "Alice"}, "abc")
        alice = hydration_scope.hydration_hooks[Structure](struct)

        assert isinstance(alice, Node)
        with pytest.warns(DeprecationWarning, match="element_id"):
            assert alice.id == 123
        assert alice.element_id == "abc"
        assert alice.labels == {"Person"}
        assert set(alice.keys()) == {"name"}
        assert alice.get("name") == "Alice"