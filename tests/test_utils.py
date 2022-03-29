#!/usr/bin/env python3

import pdb

from collections import deque
from unittest import TestCase

import trellisdata as trellis

class TestTaxonomyParser(TestCase):

	def test_read_computer_taxonomy_from_json(cls):

		parser = trellis.utils.TaxonomyParser()
		parser.read_from_json('flat-taxonomy-data.json')

		node = parser.find_by_name('Firewall')
		lineage = deque(node.path)

		assert len(lineage) == 3

	def test_read_label_taxonomy_from_json(cls):

		parser = trellis.utils.TaxonomyParser()
		parser.read_from_json('label-taxonomy.json')

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
