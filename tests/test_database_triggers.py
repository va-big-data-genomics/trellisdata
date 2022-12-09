#!/usr/bin/env python3

import pdb
import mock
import json
import yaml
import neo4j
import base64
import pytest

from unittest import TestCase

from neo4j.graph import (
	Graph,
	Node,
	Relationship,
)
from neo4j.work.summary import ResultSummary
from neo4j.addressing import Address

import trellisdata as trellis
#from trellisdata import DatabaseTrigger
#from trellisdata import TriggerController
#from trellisdata import QueryResponseReader
#from trellisdata import QueryRequestWriter

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

gatk_trigger = """
--- !DatabaseTrigger 
name: LaunchGatk5Dollar
pattern: relationship
start:
    label: Job
end: 
    label: Ubam
    properties:
        sample: sample
relationship: 
    type: GENERATED
query: launchGatk5Dollar
"""

bio_trigger = """
--- !DatabaseTrigger
name: MergeBiologicalNodes
pattern: node
start: 
    label: PersonalisSequencing
    properties:
        sample: sample
query: mergeBiologicalNodes
"""

pilot_triggers = """
--- !DatabaseTrigger
name: RelateFastqToPersonalisSequencing
pattern: node
start: 
    label: Fastq
    properties:
        sample: sample
        uri: uri
query: relateFastqToPersonalisSequencing
--- !DatabaseTrigger
name: LaunchGatk5Dollar
pattern: relationship
start:
    label: Job
end: 
    label: Ubam
    properties:
        sample: sample
relationship: 
    type: GENERATED
query: launchGatk5Dollar
--- !DatabaseTrigger
name: LaunchFastqToUbam
pattern: relationship
start: 
    label: PersonalisSequencing
end: 
    label: Fastq
    properties:
        sample: sample
        readGroup: read_group
relationship: 
    type: GENERATED
query: launchFastqToUbam
--- !DatabaseTrigger
name: RelateGenomeToFastq
pattern: relationship
start: 
    label: PersonalisSequencing
end: 
    label: Fastq
    properties:
        sample: sample
relationship: 
    type: GENERATED
query: relateGenomeToFastq
--- !DatabaseTrigger
name: RelateGvcfToGenome
pattern: relationship
start: 
    label: Gvcf
    properties:
        sample: sample
        id: gvcf_id
end: 
    label: Tbi
relationship:
    type: HAS_INDEX
query: relateGvcfToGenome
--- !DatabaseTrigger
name: MergeBiologicalNodes
pattern: node
start: 
    label: PersonalisSequencing
    properties:
        sample: sample
query: mergeBiologicalNodes
"""

no_pattern = """
--- !DatabaseTrigger
name: LaunchGatk5Dollar
start:
    label: Job
relationship: 
    type: GENERATED
end: 
    label: Ubam
    properties:
        sample: sample
query: launchGatk5Dollar
"""

bad_pattern = """
--- !DatabaseTrigger
name: LaunchGatk5Dollar
pattern: nodr
start:
    label: Job
relationship: 
    type: GENERATED
end: 
    label: Ubam
    properties:
        sample: sample
query: launchGatk5Dollar
"""

no_start_label = """
--- !DatabaseTrigger
name: LaunchGatk5Dollar
pattern: node
start:
    label:
relationship: 
    type: GENERATED
end: 
    label: Ubam
    properties:
        sample: sample
query: launchGatk5Dollar
"""

no_query = """
--- !DatabaseTrigger
name: LaunchGatk5Dollar
pattern: node
start:
    label: Job
relationship: 
    type: GENERATED
end: 
    label: Ubam
    properties:
        sample: sample
"""

no_rel_type = """
--- !DatabaseTrigger
name: LaunchGatk5Dollar
pattern: node
start:
    label: Job
relationship:
end: 
    label: Ubam
    properties:
        sample: sample
query: launchGatk5Dollar
"""

no_end_label = """
--- !DatabaseTrigger
name: LaunchGatk5Dollar
pattern: node
start:
    label: Job
relationship:
    type: GENERATED
end: 
    properties:
        sample: sample
query: launchGatk5Dollar
"""

class TestDatabaseTrigger(TestCase):

	@classmethod
	def test_load_relationship_trigger(cls):
		trigger = yaml.load(gatk_trigger, Loader=yaml.FullLoader)

		assert isinstance(trigger, trellis.DatabaseTrigger)
		assert trigger.pattern == "relationship"
		assert trigger.name == "LaunchGatk5Dollar"
		assert trigger.start['label'] == "Job"
		assert trigger.end['properties'] == {"sample": "sample"}
		assert trigger.relationship['type'] == "GENERATED"

	@classmethod
	def test_load_node_trigger(cls):
		trigger = yaml.load(bio_trigger, Loader=yaml.FullLoader)

		assert isinstance(trigger, trellis.DatabaseTrigger)
		
		# Default values
		assert not hasattr(trigger, "end")
		assert not hasattr(trigger, "relationship")

	@classmethod
	def test_load_trigger_from_file(cls):
		with open("sample_inputs/create-blob-queries.yaml", "r") as file_handle:
			triggers = yaml.load_all(file_handle, Loader=yaml.FullLoader)
			# Convert generator to list
			triggers = list(triggers)

		assert len(triggers) == 1

		trigger = triggers[0]


class TestDatabaseTriggerController(TestCase):

	# Need to generate a mock result summary
	address = Address(("bolt://localhost", 7687))
	version = neo4j.Version(3, 0)
	server_info = neo4j.ServerInfo(address, version)

	result_metadata = {"server": server_info}
	result_summary = ResultSummary("localhost", **result_metadata)
	
	# Trellis attributes
	sender = "local"
	seed_id = 123
	previous_event_id = 456

	"""
	## Mock query response
	write_response = trellis.QueryResponseWriter(
										   sender=sender,
										   seed_id=seed_id,
										   previous_event_id=previous_event_id,
										   query_name="relateGvcfTbi",
										   graph=graph,
										   result_summary=result_summary)
	message = write_response.format_json_message()
	data_str = json.dumps(message)
	data_utf8 = data_str.encode('utf-8')
	event = {'data': base64.b64encode(data_utf8)}

	read_response = trellis.QueryResponseReader(
												mock_context,
												event)
	"""

	@classmethod
	def test_load_database_triggers_by_type(cls):
		controller = trellis.TriggerController(pilot_triggers)

		assert len(controller.node_triggers) == 2
		assert len(controller.relationship_triggers) == 3

		assert len(controller.relationship_triggers['Gvcf']) == 1
		assert len(controller.relationship_triggers['Job']) == 1
		assert len(controller.relationship_triggers['PersonalisSequencing']) == 2

		assert len(controller.node_triggers['PersonalisSequencing']) == 1

	@classmethod
	def test_evaluate_rel_triggers_single_label(cls):

		# Create a mock Neo4j graph database result
		# Reference: https://github.com/neo4j/neo4j-python-driver/blob/1ed96f94a7a59f49c473dadbb81715dc9651db98/tests/unit/common/test_types.py
		triple_graph = Graph()
		# Hydrate graph with node
		graph_hydrator = Graph.Hydrator(triple_graph)

		gvcf_properties = {"sample": "sample123", "id": "gs://bucket/sample123.gvcf"}
		query_parameters = {"sample": "sample123", "gvcf_id": "gs://bucket/sample123.gvcf"}

		graph_hydrator.hydrate_node(
							n_id = 1,
							n_labels = {"Gvcf"},
							properties = gvcf_properties)
		graph_hydrator.hydrate_node(
							n_id = 2,
							n_labels = {"Tbi"},
							properties = {})
		graph_hydrator.hydrate_relationship(
							r_id = 1,
							n0_id = 1,
							n1_id = 2,
							r_type = "HAS_INDEX")

		write_response = trellis.QueryResponseWriter(
			sender=cls.sender,
			seed_id=cls.seed_id,
			previous_event_id=cls.previous_event_id,
			query_name="relateGvcfTbi",
			graph=triple_graph,
			result_summary=cls.result_summary)
		generator = write_response.generate_separate_entity_jsons()
		
		# Mocking the process of posting messages
		messages = []
		for response in generator:
			data_str = json.dumps(response)
			data_utf8 = data_str.encode('utf-8')
			event = {'data': base64.b64encode(data_utf8)}
			messages.append((mock_context, event))

		assert len(messages) == 1

		# Mocking the process of receiving messages
		context, event = messages[0]
		read_response = trellis.QueryResponseReader(
													context,
													event)
		#pdb.set_trace()
		controller = trellis.TriggerController(pilot_triggers)
		triggers = controller.evaluate_trigger_conditions(read_response)

		assert len(read_response.nodes) == 0
		assert len(read_response.relationship) == 5

		assert len(triggers) == 1
		
		trigger, parameters = triggers[0]
		assert trigger.name == "RelateGvcfToGenome"
		assert parameters == query_parameters

	@classmethod
	def test_eval_node_triggers_multi_label(cls):
		node_graph = Graph()
		# Hydrate graph with node
		node_properties = {"sample": "sample123", "uri": "gs://bucket/sample123.fastq"}
		
		graph_hydrator = Graph.Hydrator(node_graph)
		graph_hydrator.hydrate_node(
							n_id = 1,
							n_labels = {"Fastq", "Blob"},
							properties = node_properties)

		write_response = trellis.QueryResponseWriter(
			sender=cls.sender,
			seed_id=cls.seed_id,
			previous_event_id=cls.previous_event_id,
			query_name="createFastq",
			graph=node_graph,
			result_summary=cls.result_summary)
		message = write_response.return_json_with_all_nodes()
		
		data_str = json.dumps(message)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		read_response = trellis.QueryResponseReader(
													mock_context,
													event)

		controller = trellis.TriggerController(pilot_triggers)
		triggers = controller.evaluate_trigger_conditions(read_response)

		assert len(read_response.nodes) == 1
		assert len(read_response.relationship) == 0

		assert len(triggers) == 1
		
		trigger, parameters = triggers[0]
		assert trigger.name == "RelateFastqToPersonalisSequencing"
		assert parameters == node_properties

	@classmethod
	def test_eval_node_trigger_no_label_matches(cls):
		node_graph = Graph()
		# Hydrate graph with node
		node_properties = {"sample": "sample123", "uri": "gs://bucket/sample123.fastq"}
		
		graph_hydrator = Graph.Hydrator(node_graph)
		graph_hydrator.hydrate_node(
							n_id = 1,
							n_labels = {"DoesNotMatchAnyTrigger"},
							properties = node_properties)

		write_response = trellis.QueryResponseWriter(
			sender=cls.sender,
			seed_id=cls.seed_id,
			previous_event_id=cls.previous_event_id,
			query_name="createFastq",
			graph=node_graph,
			result_summary=cls.result_summary)
		message = write_response.return_json_with_all_nodes()
		
		data_str = json.dumps(message)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		read_response = trellis.QueryResponseReader(
													mock_context,
													event)

		controller = trellis.TriggerController(pilot_triggers)
		triggers = controller.evaluate_trigger_conditions(read_response)

		assert not triggers

	@classmethod
	def test_eval_rel_triggers_multi_label(cls):
		pass
	
	@classmethod
	def test_eval_node_triggers_single_label(cls):

		header = {'messageKind': 'queryResponse', 'previousEventId': '4393280078988728', 'seedId': 4393288756907900, 'sender': 'trellis-db-query'}
		body = {
			'nodes': [
				{
					'id': 288453, 
					'labels': ['Fastq'], 
					'properties': {
						'basename': 'SAMPLE123_0_R1.fastq.gz', 
						'sample': 'SAMPLE123',
						'bucket': 'mvp-test-from-personalis', 
						'crc32c': '7iQ68Q==', 
						'matePair': 1, 
						'metadata': "{'node_creation_test': 'April-5-2022-1', 'trellis-uuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929'}", 
						'metageneration': '106', 
						'size': 6188179435, 
						'storageClass': 'REGIONAL', 
						'trellisUuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929', 
						'uri': 'gs://mvp-test-from-personalis/va_mvp_phase2/PLATE0/SAMPLE123/FASTQ/SAMPLE123_0_R1.fastq.gz'
					}
				}
			], 
			'queryName': 'mergeFastqNode', 
			'relationship': {}, 
			'resultSummary': {
				'counters': {'properties_set': 10}, 
				'database': None, 
				'notifications': None, 
				'query_type': 'rw', 
				'result_available_after': 15, 
				'result_consumed_after': 0
			}
		}

		message = {
				 "header": header,
				 "body": body
		}
		data_str = json.dumps(message)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		read_response = trellis.QueryResponseReader(
													mock_context,
													event)
		
		controller = trellis.TriggerController(pilot_triggers)
		activated_triggers = controller.evaluate_trigger_conditions(read_response)

		assert len(read_response.nodes) == 1
		assert len(read_response.relationship) == 0

		assert len(activated_triggers) == 1

		trigger, parameters = activated_triggers[0]
		assert trigger.name == "RelateFastqToPersonalisSequencing"
		assert parameters['sample'] == 'SAMPLE123'
		assert parameters['uri'] == 'gs://mvp-test-from-personalis/va_mvp_phase2/PLATE0/SAMPLE123/FASTQ/SAMPLE123_0_R1.fastq.gz'

class TestTriggerControllerLoadTriggers(TestCase):

	@classmethod
	def test_load_good_triggers_from_file(cls):
		with open('sample_inputs/pilot-db-triggers.yaml', 'r') as file_handle:
			doc = file_handle.read()
		controller = trellis.TriggerController(doc)

		#pdb.set_trace()

		assert len(controller.relationship_triggers) == 3

	@classmethod
	def test_load_trigger_no_pattern(cls):
		match_pattern = r"Trigger is missing an activation pattern."
		with pytest.raises(AttributeError, match=match_pattern):
			controller = trellis.TriggerController(no_pattern)

	@classmethod
	def test_load_trigger_bad_pattern(cls):
		match_pattern = r"Trigger pattern '\w+' not in supported patterns:"
		with pytest.raises(ValueError, match=match_pattern):
			controller = trellis.TriggerController(bad_pattern)

	@classmethod
	def test_load_trigger_no_start_label(cls):
		match_pattern = "Trigger start node missing label."
		with pytest.raises(ValueError, match=match_pattern):
			controller = trellis.TriggerController(no_start_label)

	@classmethod
	def test_load_trigger_no_query(cls):
		match_pattern = "Trigger is missing query."
		with pytest.raises(AttributeError, match=match_pattern):
			controller = trellis.TriggerController(no_query)

	@classmethod
	def test_load_trigger_no_rel_type(cls):
		match_pattern = "Trigger relationship type missing."
		with pytest.raises(ValueError, match=match_pattern):
			controller = trellis.TriggerController(no_rel_type)

	@classmethod
	def test_load_trigger_no_end_label(cls):
		match_pattern = "Trigger end node missing label."
		with pytest.raises(ValueError, match=match_pattern):
			controller = trellis.TriggerController(no_end_label)