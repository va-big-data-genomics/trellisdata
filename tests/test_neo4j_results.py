#!/usr/bin/env python3

import re
import pdb
import json
import neo4j
import pytest

from unittest import TestCase

class TestNeo4jResults(TestCase):

	## Get query response
	bolt_port = 7687
	bolt_address = "localhost"
	bolt_uri = f"bolt://{bolt_address}:{bolt_port}"

	user = "neo4j"
	password = "test"
	auth_token = (user, password)

	# Make sure local instance of database is running
	driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

	@classmethod
	def test_get_single_node(cls):
		query = "MATCH (f:Fastq) RETURN f LIMIT 1"
		output_file = "sample_outputs/neo4j_results/single_node.txt"

		with cls.driver.session() as session:
			result = session.run(query)
			graph_result = result.graph()
			summary = result.consume()
		#pdb.set_trace()
		
		with open(output_file, 'w') as fh:
			for node in graph_result.nodes:
				node_dict = {
		            "id": node.id,
		            "labels": list(node.labels),
		            "properties": dict(node.items())
		        }
				fh.write(json.dumps(node_dict))

	@classmethod
	def test_get_multi_rel_path(cls):
		#query = "MATCH p=(:Sample)-[:WAS_USED_BY]->(:PersonalisSequencing)-[:GENERATED]->(:Fastq)-[:WAS_USED_BY]->(:Job)-[:GENERATED]->(:Ubam) RETURN p"


		with cls.driver.session() as session:
			result = session.run(query)
			graph_result = result.graph()
			summary = result.consume()
		pdb.set_trace()
		
		"""
		Output:
		<Relationship id=153 nodes=(<Node id=252 labels=frozenset({'Sample'}) properties={}>, <Node id=253 labels=frozenset({'PersonalisSequencing'}) properties={}>) type='WAS_USED_BY' properties={}>
		<Relationship id=154 nodes=(<Node id=253 labels=frozenset({'PersonalisSequencing'}) properties={}>, <Node id=254 labels=frozenset({'Fastq'}) properties={}>) type='GENERATED' properties={}>
		<Relationship id=155 nodes=(<Node id=254 labels=frozenset({'Fastq'}) properties={}>, <Node id=255 labels=frozenset({'Job'}) properties={}>) type='WAS_USED_BY' properties={}>
		<Relationship id=156 nodes=(<Node id=255 labels=frozenset({'Job'}) properties={}>, <Node id=256 labels=frozenset({'Ubam'}) properties={}>) type='GENERATED' properties={}>
		"""


