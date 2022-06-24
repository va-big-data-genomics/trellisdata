#!/usr/bin/env python3

import pdb
import mock
import json
import yaml
import neo4j
import base64
import pytest

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

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

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
	"""
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

	"""
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
	"""

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
	def test_laod_trigger_no_end_label(cls):
		match_pattern = "Trigger end node missing label."
		with pytest.raises(ValueError, match=match_pattern):
			controller = trellis.TriggerController(no_end_label)

	@classmethod
	def test_bad_personalis_to_fastq_response(cls):
		with open('sample_inputs/pilot-db-triggers.yaml', 'r') as file_handle:
			doc = file_handle.read()
		controller = trellis.TriggerController(doc)

		header = {'messageKind': 'queryResponse', 'previousEventId': '4770884029805667', 'seedId': 4770836549326205, 'sender': 'trellis-db-query'}
		body = {'nodes': [{'id': 288592, 'labels': ['PersonalisSequencing'], 'properties': {'AlignmentCoverage': '33.7162', 'AssayType': 'WGS', 'BloodType': 'O', 'CellId': '0228928806', 'Concentration': '50', 'Contamination': '0.0001', 'DataQcCode': 'PASS', 'DataQcDate': '02/20/2019', 'Ethnicity': 'EUR', 'Gender': 'M', 'OD260_280': '1.93', 'PlateCoord': 'A01', 'PlateId': 'DVALABP000450', 'PredictedGender': 'M', 'ProtocolId': 'PROTOCOL_7', 'ReceiptCode': 'PASS', 'ReceiptDate': '01/19/2019', 'RequestId': 'SHIP_4719', 'RunDate': '02/19/2019', 'SampleConcordance': '0.977789', 'SampleQcCode': 'PASS', 'SampleQcDate': '02/20/2019', 'ShippingId': 'SHIP5142421', 'Volume': '60', 'basename': 'SHIP5142421.json', 'bucket': 'gbsc-gcp-project-mvp-test-from-personalis', 'contentType': 'application/json', 'crc32c': 'DJlBBw==', 'dirname': 'va_mvp_phase2/DVALABP000450/SHIP5142421', 'etag': 'CNbytYn13/YCEAc=', 'eventBasedHold': False, 'extension': 'json', 'filetype': 'json', 'generation': '1648164997003606', 'gitCommitHash': '627e754', 'gitVersionTag': '', 'id': 'gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/SHIP5142421.json/1648164997003606', 'kind': 'storage#object', 'md5Hash': '7ZhO4UpRyuOFtkAR1DvP5g==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FSHIP5142421.json?generation=1648164997003606&alt=media', 'metadata': "{'gcf-update-metadata': '1138930761694726', 'pbilling-test': 'April-20-2022-5', 'trellis-uuid': 'f1cde3e9-7735-4205-9660-78fc144d7c3a'}", 'metageneration': '7', 'name': 'SHIP5142421', 'nodeCreated': 1650494263007, 'nodeIteration': 'initial', 'path': 'va_mvp_phase2/DVALABP000450/SHIP5142421/SHIP5142421.json', 'plate': 'DVALABP000450', 'sample': 'SHIP5142421', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FSHIP5142421.json', 'size': 686, 'storageClass': 'REGIONAL', 'temporaryHold': False, 'timeCreated': '2022-03-24T23:36:37.012Z', 'timeCreatedEpoch': 1648164997.012, 'timeCreatedIso': '2022-03-24T23:36:37.012000+00:00', 'timeStorageClassUpdated': '2022-03-24T23:36:37.012Z', 'timeUpdatedEpoch': 1650494256.191, 'timeUpdatedIso': '2022-04-20T22:37:36.191000+00:00', 'trellisUuid': 'f1cde3e9-7735-4205-9660-78fc144d7c3a', 'triggerOperation': 'metadataUpdate', 'updated': '2022-04-20T22:37:36.191Z', 'uri': 'gs://gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/SHIP5142421.json'}}, {'id': 288453, 'labels': ['Fastq'], 'properties': {'basename': 'SHIP5142421_0_R1.fastq.gz', 'bucket': 'gbsc-gcp-project-mvp-test-from-personalis', 'componentCount': 1, 'crc32c': '7iQ68Q==', 'customTime': '1970-01-01T00:00:00.000Z', 'dirname': 'va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ', 'etag': 'COqL96n13/YCEGo=', 'eventBasedHold': False, 'extension': 'fastq.gz', 'filetype': 'gz', 'generation': '1648165065180650', 'gitCommitHash': '0e73bb8', 'gitVersionTag': '', 'id': 'gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz/1648165065180650', 'kind': 'storage#object', 'matePair': 1, 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FFASTQ%2FSHIP5142421_0_R1.fastq.gz?generation=1648165065180650&alt=media', 'metadata': "{'node_creation_test': 'April-5-2022-1', 'trellis-uuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929'}", 'metageneration': '106', 'name': 'SHIP5142421_0_R1', 'nodeCreated': 1649198665840, 'nodeIteration': 'merged', 'path': 'va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz', 'plate': 'DVALABP000450', 'readGroup': 0, 'sample': 'SHIP5142421', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FFASTQ%2FSHIP5142421_0_R1.fastq.gz', 'size': 6188179435, 'storageClass': 'REGIONAL', 'temporaryHold': False, 'timeCreated': '2022-03-24T23:37:45.267Z', 'timeCreatedEpoch': 1648165065.267, 'timeCreatedIso': '2022-03-24T23:37:45.267000+00:00', 'timeStorageClassUpdated': '2022-03-24T23:37:45.267Z', 'timeUpdatedEpoch': 1654214895.713, 'timeUpdatedIso': '2022-06-03T00:08:15.713000+00:00', 'trellisUuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929', 'triggerOperation': 'metadataUpdate', 'updated': '2022-06-03T00:08:15.713Z', 'uri': 'gs://gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz'}}, {'id': 290206, 'labels': ['PersonalisSequencing'], 'properties': {'sample': 'SHIP5142421'}}], 'queryName': 'relateFastqToPersonalisSequencing', 'relationships': [{'end_node': {'id': 288453, 'labels': ['Fastq'], 'properties': {'basename': 'SHIP5142421_0_R1.fastq.gz', 'bucket': 'gbsc-gcp-project-mvp-test-from-personalis', 'componentCount': 1, 'crc32c': '7iQ68Q==', 'customTime': '1970-01-01T00:00:00.000Z', 'dirname': 'va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ', 'etag': 'COqL96n13/YCEGo=', 'eventBasedHold': False, 'extension': 'fastq.gz', 'filetype': 'gz', 'generation': '1648165065180650', 'gitCommitHash': '0e73bb8', 'gitVersionTag': '', 'id': 'gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz/1648165065180650', 'kind': 'storage#object', 'matePair': 1, 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FFASTQ%2FSHIP5142421_0_R1.fastq.gz?generation=1648165065180650&alt=media', 'metadata': "{'node_creation_test': 'April-5-2022-1', 'trellis-uuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929'}", 'metageneration': '106', 'name': 'SHIP5142421_0_R1', 'nodeCreated': 1649198665840, 'nodeIteration': 'merged', 'path': 'va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz', 'plate': 'DVALABP000450', 'readGroup': 0, 'sample': 'SHIP5142421', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FFASTQ%2FSHIP5142421_0_R1.fastq.gz', 'size': 6188179435, 'storageClass': 'REGIONAL', 'temporaryHold': False, 'timeCreated': '2022-03-24T23:37:45.267Z', 'timeCreatedEpoch': 1648165065.267, 'timeCreatedIso': '2022-03-24T23:37:45.267000+00:00', 'timeStorageClassUpdated': '2022-03-24T23:37:45.267Z', 'timeUpdatedEpoch': 1654214895.713, 'timeUpdatedIso': '2022-06-03T00:08:15.713000+00:00', 'trellisUuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929', 'triggerOperation': 'metadataUpdate', 'updated': '2022-06-03T00:08:15.713Z', 'uri': 'gs://gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz'}}, 'id': 288207, 'properties': {}, 'start_node': {'id': 288592, 'labels': ['PersonalisSequencing'], 'properties': {'AlignmentCoverage': '33.7162', 'AssayType': 'WGS', 'BloodType': 'O', 'CellId': '0228928806', 'Concentration': '50', 'Contamination': '0.0001', 'DataQcCode': 'PASS', 'DataQcDate': '02/20/2019', 'Ethnicity': 'EUR', 'Gender': 'M', 'OD260_280': '1.93', 'PlateCoord': 'A01', 'PlateId': 'DVALABP000450', 'PredictedGender': 'M', 'ProtocolId': 'PROTOCOL_7', 'ReceiptCode': 'PASS', 'ReceiptDate': '01/19/2019', 'RequestId': 'SHIP_4719', 'RunDate': '02/19/2019', 'SampleConcordance': '0.977789', 'SampleQcCode': 'PASS', 'SampleQcDate': '02/20/2019', 'ShippingId': 'SHIP5142421', 'Volume': '60', 'basename': 'SHIP5142421.json', 'bucket': 'gbsc-gcp-project-mvp-test-from-personalis', 'contentType': 'application/json', 'crc32c': 'DJlBBw==', 'dirname': 'va_mvp_phase2/DVALABP000450/SHIP5142421', 'etag': 'CNbytYn13/YCEAc=', 'eventBasedHold': False, 'extension': 'json', 'filetype': 'json', 'generation': '1648164997003606', 'gitCommitHash': '627e754', 'gitVersionTag': '', 'id': 'gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/SHIP5142421.json/1648164997003606', 'kind': 'storage#object', 'md5Hash': '7ZhO4UpRyuOFtkAR1DvP5g==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FSHIP5142421.json?generation=1648164997003606&alt=media', 'metadata': "{'gcf-update-metadata': '1138930761694726', 'pbilling-test': 'April-20-2022-5', 'trellis-uuid': 'f1cde3e9-7735-4205-9660-78fc144d7c3a'}", 'metageneration': '7', 'name': 'SHIP5142421', 'nodeCreated': 1650494263007, 'nodeIteration': 'initial', 'path': 'va_mvp_phase2/DVALABP000450/SHIP5142421/SHIP5142421.json', 'plate': 'DVALABP000450', 'sample': 'SHIP5142421', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FSHIP5142421.json', 'size': 686, 'storageClass': 'REGIONAL', 'temporaryHold': False, 'timeCreated': '2022-03-24T23:36:37.012Z', 'timeCreatedEpoch': 1648164997.012, 'timeCreatedIso': '2022-03-24T23:36:37.012000+00:00', 'timeStorageClassUpdated': '2022-03-24T23:36:37.012Z', 'timeUpdatedEpoch': 1650494256.191, 'timeUpdatedIso': '2022-04-20T22:37:36.191000+00:00', 'trellisUuid': 'f1cde3e9-7735-4205-9660-78fc144d7c3a', 'triggerOperation': 'metadataUpdate', 'updated': '2022-04-20T22:37:36.191Z', 'uri': 'gs://gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/SHIP5142421.json'}}, 'type': 'GENERATED'}, {'end_node': {'id': 288453, 'labels': ['Fastq'], 'properties': {'basename': 'SHIP5142421_0_R1.fastq.gz', 'bucket': 'gbsc-gcp-project-mvp-test-from-personalis', 'componentCount': 1, 'crc32c': '7iQ68Q==', 'customTime': '1970-01-01T00:00:00.000Z', 'dirname': 'va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ', 'etag': 'COqL96n13/YCEGo=', 'eventBasedHold': False, 'extension': 'fastq.gz', 'filetype': 'gz', 'generation': '1648165065180650', 'gitCommitHash': '0e73bb8', 'gitVersionTag': '', 'id': 'gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz/1648165065180650', 'kind': 'storage#object', 'matePair': 1, 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FFASTQ%2FSHIP5142421_0_R1.fastq.gz?generation=1648165065180650&alt=media', 'metadata': "{'node_creation_test': 'April-5-2022-1', 'trellis-uuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929'}", 'metageneration': '106', 'name': 'SHIP5142421_0_R1', 'nodeCreated': 1649198665840, 'nodeIteration': 'merged', 'path': 'va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz', 'plate': 'DVALABP000450', 'readGroup': 0, 'sample': 'SHIP5142421', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-test-from-personalis/o/va_mvp_phase2%2FDVALABP000450%2FSHIP5142421%2FFASTQ%2FSHIP5142421_0_R1.fastq.gz', 'size': 6188179435, 'storageClass': 'REGIONAL', 'temporaryHold': False, 'timeCreated': '2022-03-24T23:37:45.267Z', 'timeCreatedEpoch': 1648165065.267, 'timeCreatedIso': '2022-03-24T23:37:45.267000+00:00', 'timeStorageClassUpdated': '2022-03-24T23:37:45.267Z', 'timeUpdatedEpoch': 1654214895.713, 'timeUpdatedIso': '2022-06-03T00:08:15.713000+00:00', 'trellisUuid': 'af7bb5ef-ef48-4447-8c0f-555f234b0929', 'triggerOperation': 'metadataUpdate', 'updated': '2022-06-03T00:08:15.713Z', 'uri': 'gs://gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz'}}, 'id': 286454, 'properties': {}, 'start_node': {'id': 290206, 'labels': ['PersonalisSequencing'], 'properties': {'sample': 'SHIP5142421'}}, 'type': 'GENERATED'}], 'resultSummary': {'counters': {}, 'database': None, 'notifications': None, 'parameters': {'sample': 'SHIP5142421', 'uri': 'gs://gbsc-gcp-project-mvp-test-from-personalis/va_mvp_phase2/DVALABP000450/SHIP5142421/FASTQ/SHIP5142421_0_R1.fastq.gz'}, 'plan': None, 'profile': None, 'query': 'MATCH (seq:PersonalisSequencing), (fastq:Fastq) WHERE fastq.uri=$uri AND seq.sample=fastq.sample MERGE (seq)-[rel:GENERATED]->(fastq) RETURN seq, rel, fastq', 'query_type': 'rw', 'result_available_after': 10, 'result_consumed_after': 0}}

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

		match_pattern = r"Number of nodes \(3\) and relationships \(2\) does not fit a recognized return pattern\."
		with pytest.raises(ValueError, match=match_pattern):
			activated_triggers = controller.evaluate_trigger_conditions(read_response)

		#for trigger, parameters in activated_triggers:
		#	print(f"> Trigger activated: {trigger.name}.")
