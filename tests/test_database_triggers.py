#!/usr/bin/env python3

import mock
import json
import yaml
import neo4j
import base64

from unittest import TestCase

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
name: relateFastqToPersonalisSequencing
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

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

class TestDatabaseTrigger(TestCase):

	@classmethod
	def test_load_single_trigger(cls):
		trigger = yaml.load(gatk_trigger, Loader=yaml.FullLoader)

		assert isinstance(trigger, trellis.DatabaseTrigger)
		assert trigger.pattern == "relationship"
		assert trigger.name == "LaunchGatk5Dollar"
		assert trigger.start['label'] == "Job"
		assert trigger.end['properties'] == {"sample": "sample"}
		assert trigger.relationship['type'] == "GENERATED"


class TestDatabaseTriggerController(TestCase):
	## Get query response
	bolt_port = 7687
	bolt_address = "localhost"
	bolt_uri = f"bolt://{bolt_address}:{bolt_port}"

	user = "neo4j"
	password = "test"
	auth_token = (user, password)

	# Trellis attributes
	sender = "local"
	seed_id = 123
	previous_event_id = 456

	# Make sure local instance of database is running
	driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

	with driver.session() as session:
		result = session.run("MERGE (g:Gvcf {sample: 'sample123', id: 'gs://bucket/sample123.gvcf'})-[r:HAS_INDEX]->(t:Tbi) RETURN g,r,t")
		graph = result.graph()
		result_summary = result.consume()

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
	def test_evaluate_trigger_conditions(cls):

		with cls.driver.session() as session:
			result = session.run("MERGE (g:Gvcf {sample: 'sample123', id: 'gs://bucket/sample123.gvcf'})-[r:HAS_INDEX]->(t:Tbi) RETURN g,r,t")
			graph = result.graph()
			result_summary = result.consume()

		write_response = trellis.QueryResponseWriter(
			sender=cls.sender,
			seed_id=cls.seed_id,
			previous_event_id=cls.previous_event_id,
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

		controller = trellis.TriggerController(pilot_triggers)
		triggers = controller.evaluate_trigger_conditions(read_response)

		assert len(cls.read_response.nodes) == 2
		assert len(cls.read_response.relationships) == 1

		assert len(triggers) == 1
		
		trigger, parameters = triggers[0]
		assert trigger.name == "RelateGvcfToGenome"
		assert parameters == {'gvcf_id': 'gs://bucket/sample123.gvcf', 'sample': 'sample123'}

	@classmethod
	def test_evaluate_node_triggers(cls):

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
			'relationships': [], 
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
		assert len(read_response.relationships) == 0

		assert len(activated_triggers) == 1

		trigger, parameters = activated_triggers[0]
		assert trigger.name == "relateFastqToPersonalisSequencing"
		assert parameters['sample'] == 'SAMPLE123'
		assert parameters['uri'] == 'gs://mvp-test-from-personalis/va_mvp_phase2/PLATE0/SAMPLE123/FASTQ/SAMPLE123_0_R1.fastq.gz'
