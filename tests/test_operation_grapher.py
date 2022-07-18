import pdb
import neo4j

import trellisdata as trellis

from unittest import TestCase

""" Deactivting until I can replace db dependency
class TestOperationGrapher(TestCase):

	## Get query response
	bolt_port = 7687
	bolt_address = "localhost"
	bolt_uri = f"bolt://{bolt_address}:{bolt_port}"

	user = "neo4j"
	password = "test"
	auth_token = (user, password)

	# Make sure local instance of database is running
	driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

	query_document = "sample_inputs/pilot-db-queries.yaml"
	trigger_document =  "sample_inputs/pilot-db-triggers.yaml"
	
	@classmethod
	def test_create_operation_grapher(cls):

		grapher = trellis.OperationGrapher(
					query_document = cls.query_document,
					trigger_document = cls.trigger_document,
					neo4j_driver = cls.driver)

		for query in grapher.queries:
			assert isinstance(query, trellis.DatabaseQuery)

		for trigger in grapher.triggers:
			assert isinstance(trigger, trellis.DatabaseTrigger)
		
		assert len(grapher.queries) == 8
		assert len(grapher.triggers) == 10

		for trigger in grapher.triggers:
			if trigger.pattern == 'node':
				assert hasattr(trigger, 'name') == True
				assert hasattr(trigger, 'start') == True
				assert hasattr(trigger, 'relationship') == False
				assert hasattr(trigger, 'end') == False
			elif trigger.pattern == 'relationship':
				assert hasattr(trigger, 'name') == True
				assert hasattr(trigger, 'start') == True
				assert hasattr(trigger, 'relationship') == True
				assert hasattr(trigger, 'end') == True
				assert hasattr(trigger, 'query') == True

	@classmethod
	def test_add_query_nodes_to_neo4j(cls):
		grapher = trellis.OperationGrapher(
			query_document = cls.query_document,
			trigger_document = cls.trigger_document,
			neo4j_driver = cls.driver)
		query_nodes = grapher.add_query_nodes_to_neo4j()

		assert len(query_nodes) == 8

	@classmethod
	def test_add_trigger_nodes_to_neo4j(cls):
		grapher = trellis.OperationGrapher(
			query_document = cls.query_document,
			trigger_document = cls.trigger_document,
			neo4j_driver = cls.driver)

		trigger_nodes = grapher.add_trigger_nodes_to_neo4j()

		assert len(trigger_nodes) == 10

	@classmethod
	def test_connect_relationship_based_operations(cls):
		grapher = trellis.OperationGrapher(
			query_document = cls.query_document,
			trigger_document = cls.trigger_document,
			neo4j_driver = cls.driver)

		query_nodes = grapher.add_query_nodes_to_neo4j()
		trigger_nodes = grapher.add_trigger_nodes_to_neo4j()

		with cls.driver.session() as session:
			result_summary = session.write_transaction(
								grapher._connect_relationship_operations)

	@classmethod
	def test_connect_node_based_operations(cls):
		grapher = trellis.OperationGrapher(
			query_document = cls.query_document,
			trigger_document = cls.trigger_document,
			neo4j_driver = cls.driver)

		query_nodes = grapher.add_query_nodes_to_neo4j()
		trigger_nodes = grapher.add_trigger_nodes_to_neo4j()

		with cls.driver.session() as session:
			result_summary = session.write_transaction(
								grapher._connect_node_operations)

	@classmethod
	def test_connect_triggers_to_activaed_queries(cls):
		grapher = trellis.OperationGrapher(
			query_document = cls.query_document,
			trigger_document = cls.trigger_document,
			neo4j_driver = cls.driver)

		query_nodes = grapher.add_query_nodes_to_neo4j()
		trigger_nodes = grapher.add_trigger_nodes_to_neo4j()

		with cls.driver.session() as session:
			result_summary = session.write_transaction(
								grapher._connect_triggers_to_activated_queries)
"""
