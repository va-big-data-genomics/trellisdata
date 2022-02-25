#!/usr/bin/env python3

import json
import mock
import neo4j
import base64
import pytest

from unittest import TestCase

#from neo4j import GraphDatabase

import trellisdata as trellis
#import check_triggers

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

class TestQueryRequestWriter(TestCase):

	sender = "check-triggers"
	seed_id = 123
	previous_event_id = 345
	query_name = "dummyTrigger"
	query_parameters = {"sample": "SHIP123"}

	@classmethod
	def test_create_dummy_query_request(cls):

		request = trellis.QueryRequestWriter(
									   sender = cls.sender,
									   seed_id = cls.seed_id,
									   previous_event_id = cls.previous_event_id,
									   query_name = cls.query_name,
									   query_parameters = cls.query_parameters)

	@classmethod
	def test_create_write_dummy_query_request(cls):
		query_parameters = {
							"sample": "SHIP123"
		}
		
		request = trellis.QueryRequestWriter(
									   sender = cls.sender,
									   seed_id = cls.seed_id,
									   previous_event_id = cls.previous_event_id,
									   query_name = cls.query_name,
									   query_parameters = cls.query_parameters)

		message = request.format_json_message()
		message['header']
		message['body']
		
		# Check header values
		assert message['header']['messageKind'] == "queryRequest"
		assert message['header']['sender'] == cls.sender
		assert message['header']['seedId'] == cls.seed_id
		assert message['header']['previousEventId'] == cls.previous_event_id

		# Check body values
		assert message['body']['queryName'] == cls.query_name
		assert message['body']['queryParameters'] == cls.query_parameters


class TestQueryRequestReader(TestCase):

	@classmethod
	def test_parse_dummy_query_request(cls):
		data = {
			'header': {
				'messageKind': 'queryRequest',  
				'sender': 'check-triggers', 
				'seedId': '1062325217821887', 
				'previousEventId': '1062332838591023'
			}, 
			'body': {
				'queryName': 'dummyTrigger',
				'queryParameters': {},
            }
        }
		data_str = json.dumps(data)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

        # How do you know it's a query request until you parse the message?
        # Right now, you don't.
		request = trellis.QueryRequestReader(
											 mock_context,
											 event)

        # Check header values
		assert request.message_kind == "queryRequest"
		assert request.sender == "check-triggers"
		assert request.seed_id == 1062325217821887
		assert request.previous_event_id == 1062332838591023

		# Check body values
		assert request.query_name == "dummyTrigger"
		assert request.query_parameters == {}

	@classmethod
	def test_parse_update_job_node_request(cls):
		data = {
			'header': {
				'messageKind': 'queryRequest',  
				'sender': 'trellis-log-delete-instance', 
				'seedId': '3329816530980893', 
				'previousEventId': '3329816530980893', 
			}, 
			'body': {
				'queryName': 'UpdateJobNode',
				'queryParameters': {
					"instanceId": "6433280663749256939",
					"instanceName": "google-pipelines-worker-6c3d415be62e2ebf2774924ced0fd771",
					"stopTime": "2021-11-04T23:16:53.95614Z",
					"stopTimeEpoch": "1636067813.95614",
					"stoppedBy": "genomics-api",
					"status": "STOPPED",
				}
			}
		}
		data_str = json.dumps(data)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		request = trellis.QueryRequestReader(
											 mock_context,
											 event)

		# Check header values
		assert request.message_kind == "queryRequest"
		assert request.sender == "trellis-log-delete-instance"
		assert request.seed_id == 3329816530980893
		assert request.previous_event_id == 3329816530980893

		# Check body values
		assert request.query_name == "UpdateJobNode"
		assert request.query_parameters == {
			"instanceId": "6433280663749256939",
			"instanceName": "google-pipelines-worker-6c3d415be62e2ebf2774924ced0fd771",
			"stopTime": "2021-11-04T23:16:53.95614Z",
			"stopTimeEpoch": "1636067813.95614",
			"stoppedBy": "genomics-api",
			"status": "STOPPED",
		}


class TestQueryResponseWriter(TestCase):

	bolt_port = 7687
	bolt_address = "localhost"
	bolt_uri = f"bolt://{bolt_address}:{bolt_port}"
	
	user = "neo4j"
	password = "test"
	auth_token = (user, password)

	driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

	# Trellis attributes
	sender = "test-messages"
	seed_id = 123
	previous_event_id = 456

	@classmethod
	def test_create_dummy_response(cls):

		with cls.driver.session() as session:
			result = session.run("RETURN 1")
			graph = result.graph()
			result_summary = result.consume()

		response = trellis.QueryResponseWriter(
										 sender = cls.sender,
										 seed_id = cls.seed_id,
										 previous_event_id = cls.previous_event_id,
										 query_name = "return-1",
										 graph = graph,
										 result_summary = result_summary)

		assert isinstance(response.graph, neo4j.graph.Graph)
		assert isinstance(response.result_summary, neo4j.ResultSummary)
		assert response.result_summary.query == "RETURN 1"
		assert isinstance(response.result_summary.parameters, dict)

	@classmethod
	def test_format_json_message_iter(cls):
		with cls.driver.session() as session:
			result = session.run("MERGE (p:Person {name: 'Joe'}) RETURN p")
			graph = result.graph()
			result_summary = result.consume()

		response = trellis.QueryResponseWriter(
										 sender = cls.sender,
										 seed_id = cls.seed_id,
										 previous_event_id = cls.previous_event_id,
										 query_name = "create-joe",
										 graph = graph,
										 result_summary = result_summary)

		messages = list(response.format_json_message_iter())
		assert len(messages) == 1

	@classmethod
	def test_message_nodes_and_relationship(cls):
		query_name = "relate-bobs"
		query = "CREATE (p:Person {name:'Bob'})-[r:KNOWS]->(p2:Person {name:'Bob2'}) RETURN p, p2, r"

		with cls.driver.session() as session:
			result = session.run(query)
			graph = result.graph()
			result_summary = result.consume()

		response = trellis.QueryResponseWriter(
										 sender = cls.sender,
										 seed_id = cls.seed_id,
										 previous_event_id = cls.previous_event_id,
										 query_name = query_name,
										 graph = graph,
										 result_summary = result_summary)
		message = response.format_json_message()

		assert message['header']
		assert message['header']['messageKind'] == 'queryResponse'
		assert message['header']['seedId'] == cls.seed_id
		assert message['header']['previousEventId'] == cls.previous_event_id

		assert message['body']
		assert message['body']['queryName'] == query_name
		
		nodes = message['body']['nodes']
		assert len(nodes) == 2
		for node in nodes:
			assert node['id']
			assert node['labels']
			assert node['properties']

		relationships = message['body']['relationships']
		assert len(relationships) == 1
		for rel in relationships:
			assert rel['id']
			assert rel['start_node']
			assert rel['end_node']
			assert rel['type'] == 'KNOWS'
			assert rel['properties'] == {}

		result_summary = message['body']['resultSummary']
		assert result_summary['database'] == 'neo4j'
		assert result_summary['query'] == query
		assert result_summary['parameters'] == {}
		assert result_summary['query_type'] == 'rw'
		assert result_summary['plan'] == None
		assert result_summary['profile'] == None
		assert result_summary['notifications'] == None
		assert isinstance(result_summary['result_available_after'], int)
		assert isinstance(result_summary['result_consumed_after'], int)

		counters = result_summary['counters']
		assert counters['labels_added'] == 2
		assert counters['relationships_created'] == 1
		assert counters['nodes_created'] == 2
		assert counters['properties_set'] == 2

	@classmethod
	def test_parse_pubsub_message(cls):
		query_name = "relate-bobs"
		query = "CREATE (p:Person {name:'Bob'})-[r:KNOWS]->(p2:Person {name:'Bob2'}) RETURN p, p2, r"

		with cls.driver.session() as session:
			result = session.run(query)
			graph = result.graph()
			result_summary = result.consume()

		response = trellis.QueryResponseWriter(
										 sender = cls.sender,
										 seed_id = cls.seed_id,
										 previous_event_id = cls.previous_event_id,
										 query_name = query_name,
										 graph = graph,
										 result_summary = result_summary)
		message = response.format_json_message()
		

class TestQueryResponseReader(TestCase):

	@classmethod
	def test_parse_pubsub_message(cls):
		data = {
			'header': {
				'messageKind': 'queryResponse', 
				'sender': 'test-messages', 
				'seedId': 123, 
				'previousEventId': 456
			}, 
			'body': {
				'queryName': 'relate-bobs', 
				'nodes': [
					{
						'id': 157, 
						'labels': ['Person'], 
						'properties': {'name': 'Bob'}
					}, 
					{
						'id': 158, 
						'labels': ['Person'], 
						'properties': {'name': 'Bob2'}
					}
				], 
				'relationships': [
					{
						'id': 78, 
						'start_node': {
							'id': 157, 
							'labels': ['Person'], 
							'properties': {'name': 'Bob'}
						}, 
						'end_node': {
							'id': 158, 
							'labels': ['Person'], 
							'properties': {'name': 'Bob2'}
						}, 
						'type': 'KNOWS', 
						'properties': {}
					}
				], 
				'resultSummary': {
					'database': 'neo4j', 
					'query': "CREATE (p:Person {name:'Bob'})-[r:KNOWS]->(p2:Person {name:'Bob2'}) RETURN p, p2, r", 
					'parameters': {}, 
					'query_type': 'rw', 
					'plan': None, 
					'profile': None, 
					'notifications': None, 
					'counters': {
						'labels_added': 2, 
						'_contains_updates': True, 
						'relationships_created': 1, 
						'nodes_created': 2, 
						'properties_set': 2
					}, 
					'result_available_after': 5, 
					'result_consumed_after': 1
				}
			}
		}
		data_str = json.dumps(data)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		response = trellis.QueryResponseReader(
											   mock_context,
											   event)
		assert response.result_summary['counters']['labels_added'] == 2
		assert len(response.relationships) == 1
		assert len(response.nodes) == 2
