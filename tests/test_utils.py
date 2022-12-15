#!/usr/bin/env python3

import re
import pdb

from collections import deque
from unittest import TestCase

import trellisdata as trellis

class TestTaxonomyParser(TestCase):

	def test_read_computer_taxonomy_from_json(cls):

		parser = trellis.utils.TaxonomyParser()
		parser.read_from_json('sample_inputs/flat-taxonomy-data.json')

		node = parser.find_by_name('Firewall')
		lineage = deque(node.path)

		assert len(lineage) == 3

	def test_read_label_taxonomy_from_json(cls):

		parser = trellis.utils.TaxonomyParser()
		parser.read_from_json('sample_inputs/label-taxonomy.json')

		node = parser.find_by_name('Gvcf')
		lineage = deque(node.path)

		assert len(lineage) == 4

	def test_read_taxonomy_from_string(cls):

		taxonomy_string = b'{\n\t"L0": [\n\t\t{ "name": "root" }\n\t],\n\t"L1": [\n\t\t{ "name": "Blob", "parent": "root" },\n\t\t{ "name": "Dstat", "parent": "root" },\n\t\t{ "name": "GcpInstance", "parent": "root" },\n\t\t{ "name": "JobRequest", "parent": "root" },\n\t\t{ "name": "Dstat", "parent": "root" },\n\t\t{ "name": "Person", "parent": "root" },\n\t\t{ "name": "Study", "parent": "root" },\n\t\t{ "name": "BiologicalOme", "parent": "root" },\n\t\t{ "name": "Sample", "parent": "root" },\n\t\t{ "name": "CromwellStep", "parent": "root" },\n\t\t{ "name": "Participant", "parent": "root" }\n\t],\n\t"L2": [\n\t\t{ "name": "Dsub", "parent": "GcpInstance" },\n\t\t{ "name": "CromwellAttempt", "parent": "GcpInstance" },\n\t\t{ "name": "Index", "parent": "Blob" },\n\t\t{ "name": "Microarray", "parent": "Blob" },\n\t\t{ "name": "Vcfstats", "parent": "Blob" },\n\t\t{ "name": "Fastqc", "parent": "Blob" },\n\t\t{ "name": "Flagstat", "parent": "Blob" },\n\t\t{ "name": "Fastq", "parent": "Blob" },\n\t\t{ "name": "Bam", "parent": "Blob" },\n\t\t{ "name": "Vcf", "parent": "Blob" },\n\t\t{ "name": "Checksum", "parent": "Blob" },\n\t\t{ "name": "Log", "parent": "Blob" }, \n\t\t{ "name": "Script", "parent": "Blob" },\n\t\t{ "name": "Json", "parent": "Blob" },\n\t\t{ "name": "Genome", "parent": "BiologicalOme" }\n\t],\n\t"L3": [\n\t\t{ "name": "CromwellWorkflow", "parent": "Dsub" },\n\t\t{ "name": "Cram", "parent": "Bam" },\n\t\t{ "name": "Ubam", "parent": "Bam" },\n\t\t{ "name": "Gvcf", "parent": "Vcf" },\n\t\t{ "name": "PersonalisSequencing", "parent": "Json" }\n\t]\n}'

		parser = trellis.utils.TaxonomyParser()
		parser.read_from_string(taxonomy_string)

		node = parser.find_by_name('Gvcf')
		lineage = deque(node.path)

		assert len(lineage) == 4

class TestGetDatetimeStamp(TestCase):

	def test_get_datetime_stamp(cls):
		stamp = trellis.utils._get_datetime_stamp()

class TestMakeUniqueTaskId(TestCase):

	def test_make_fastqs_task_id(cls):
		node_1 = {'id': 650078, 'labels': ['Fastq'], 'properties': {'basename': 'SHIP123_2_R2.fastq.gz', 'bucket': 'va-big-data-bucket', 'componentCount': 1, 'crc32c': 'QeBQQg==', 'dirname': 'va_mvp_phase2/DVALABP000123/SHIP123/FASTQ', 'etag': 'CLyInPH23/YCEAg=', 'extension': 'fastq.gz', 'filetype': 'gz', 'generation': '1648165483119676', 'gitCommitHash': '80423e1', 'gitVersionTag': '', 'id': 'va-big-data-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R2.fastq.gz/1648165483119676', 'kind': 'storage#object', 'matePair': 2, 'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/va-big-data-bucket/o/va_mvp_phase2%2FDVALABP000123%2FSHIP123%2FFASTQ%2FSHIP123_2_R2.fastq.gz?generation=1648165483119676&alt=media', 'metadata': "{'pbilling': '2022-12-14-try-0', 'trellis-uuid': 'c404391b-6f1d-45b0-a7fe-99677a3543af'}", 'metageneration': '8', 'name': 'SHIP123_2_R2', 'nodeCreated': 1671062439357, 'nodeIteration': 'initial', 'path': 'va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R2.fastq.gz', 'plate': 'DVALABP000123', 'readGroup': 2, 'sample': 'SHIP123', 'selfLink': 'https://www.googleapis.com/storage/v1/b/va-big-data-bucket/o/va_mvp_phase2%2FDVALABP000123%2FSHIP123%2FFASTQ%2FSHIP123_2_R2.fastq.gz', 'size': 6495426765, 'storageClass': 'REGIONAL', 'timeCreated': '2022-03-24T23:44:43.241Z', 'timeCreatedEpoch': 1648165483.241, 'timeCreatedIso': '2022-03-24T23:44:43.241000+00:00', 'timeStorageClassUpdated': '2022-03-24T23:44:43.241Z', 'timeUpdatedEpoch': 1671062435.247, 'timeUpdatedIso': '2022-12-15T00:00:35.247000+00:00', 'trellisUuid': 'c404391b-6f1d-45b0-a7fe-99677a3543af', 'triggerOperation': 'metadataUpdate', 'updated': '2022-12-15T00:00:35.247Z', 'uri': 'gs://va-big-data-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R2.fastq.gz'}}
		node_2 = {'id': 650058, 'labels': ['Fastq'], 'properties': {'basename': 'SHIP123_2_R1.fastq.gz', 'bucket': 'va-big-data-bucket', 'componentCount': 1, 'crc32c': 'J9TZTA==', 'dirname': 'va_mvp_phase2/DVALABP000123/SHIP123/FASTQ', 'etag': 'COuewcv23/YCEAg=', 'extension': 'fastq.gz', 'filetype': 'gz', 'generation': '1648165404036971', 'gitCommitHash': '80423e1', 'gitVersionTag': '', 'id': 'va-big-data-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R1.fastq.gz/1648165404036971', 'kind': 'storage#object', 'matePair': 1, 'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/va-big-data-bucket/o/va_mvp_phase2%2FDVALABP000123%2FSHIP123%2FFASTQ%2FSHIP123_2_R1.fastq.gz?generation=1648165404036971&alt=media', 'metadata': "{'pbilling': '2022-12-14-try-0', 'trellis-uuid': '1f81be9c-9dff-4237-b982-bf8d6d63ebf4'}", 'metageneration': '8', 'name': 'SHIP123_2_R1', 'nodeCreated': 1671062426878, 'nodeIteration': 'initial', 'path': 'va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R1.fastq.gz', 'plate': 'DVALABP000123', 'readGroup': 2, 'sample': 'SHIP123', 'selfLink': 'https://www.googleapis.com/storage/v1/b/va-big-data-bucket/o/va_mvp_phase2%2FDVALABP000123%2FSHIP123%2FFASTQ%2FSHIP123_2_R1.fastq.gz', 'size': 6284308781, 'storageClass': 'REGIONAL', 'timeCreated': '2022-03-24T23:43:24.125Z', 'timeCreatedEpoch': 1648165404.125, 'timeCreatedIso': '2022-03-24T23:43:24.125000+00:00', 'timeStorageClassUpdated': '2022-03-24T23:43:24.125Z', 'timeUpdatedEpoch': 1671062421.942, 'timeUpdatedIso': '2022-12-15T00:00:21.942000+00:00', 'trellisUuid': '1f81be9c-9dff-4237-b982-bf8d6d63ebf4', 'triggerOperation': 'metadataUpdate', 'updated': '2022-12-15T00:00:21.942Z', 'uri': 'gs://va-big-data-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R1.fastq.gz'}}
		nodes = [node_1, node_2]
		task_id, trunc_nodes_hash = trellis.utils.make_unique_task_id(nodes)

		assert re.search(r'\d+-\d+-\d+-5d709493', task_id)
		assert trunc_nodes_hash == '5d709493'

class TestConvertTimestampToRfc3339(TestCase):

	def test_convert_timestamp(cls):
		pass


