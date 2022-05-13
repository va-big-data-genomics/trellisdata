import pdb
import yaml

from unittest import TestCase

from trellisdata import DatabaseQuery

tbi_query = """
--- !DatabaseQuery
name: relateTbiToGvcf
cypher: 'MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE tbi.id = $tbi_id AND step.wdlCallAlias = "mergevcfs" MERGE (gvcf)-[:HAS_INDEX {ontology: "bioinformatics"}]->(tbi) RETURN gvcf AS node'
required_parameters:
  tbi_id: str
write_transaction: true
split_results: false
publish_to: 
- TOPIC_TEST_FUNCTION
indexes:
  Tbi:
  - id
returns:
  pattern: node
  start: Gvcf
"""

gvcf_query_doc = """
--- !DatabaseQuery
name: relateGvcfToTbi
cypher: 'MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE step.cromwellWorkflowId = $cromwell_id AND step.wdlCallAlias = "mergevcfs" MERGE (gvcf)-[r:HAS_INDEX {ontology: "bioinformatics"}]->(tbi) RETURN gvcf, r, tbi'
required_parameters:
  cromwell_id: str
write_transaction: true
split_results: false
publish_to: 
- TOPIC_TEST_FUNCTION
indexes:
  CromwellStep:
    - cromwellWorkflowId
    - wdlCallAlias
returns:
  pattern: relationship
  start: Gvcf
  relationship: HAS_INDED
  end: Tbi
active: true
"""

tbi_queries = """
--- !DatabaseQuery
name: relateGvcfToTbi
cypher: 'MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE step.cromwellWorkflowId = $cromwell_id AND step.wdlCallAlias = "mergevcfs" MERGE (gvcf)-[r:HAS_INDEX {ontology: "bioinformatics"}]->(tbi) RETURN gvcf, r, tbi'
required_parameters:
  cromwell_id: str
write_transaction: true
split_results: false
publish_to: 
- TOPIC_TEST_FUNCTION
indexes:
  CromwellStep:
    - cromwellWorkflowId
    - wdlCallAlias
returns:
  pattern: relationship
  start: Gvcf
  relationship: HAS_INDED
  end: Tbi
active: true
--- !DatabaseQuery
name: relateGenomeToFastq
cypher: 'MATCH (fastq:Fastq)<-[]-(:PersonalisSequencing)<-[]-(s:Sample)<-[]-(:Person)-[]->(genome:Genome) WHERE s.sample = $sample AND NOT (g)-[:HAS_SEQUENCING_READS]->(f) MERGE (genome)-[rel:HAS_SEQUENCING_READS]->(fastq) RETURN genome, rel, fastq'
required_parameters:
  sample: str
write_transaction: true
split_results: false
publish_to: 
- TOPIC_TRIGGERS
indexes:
  Sample: 
  - sample
returns:
  pattern: relationship
  start: Genome
  relationship: HAS_SEQUENCING_READS
  end: Fastq
active: true
"""

class TestDatabaseQuery(TestCase):

	@classmethod
	def test_create_database_query(cls):
		query = yaml.load(tbi_query, Loader=yaml.FullLoader)

		assert isinstance(query, DatabaseQuery) == True
		assert query.name == "relateTbiToGvcf"
		assert query.required_parameters == {"tbi_id": "str"}

	@classmethod
	def test_create_multiple_queries(cls):
		queries = yaml.load_all(tbi_queries, Loader=yaml.FullLoader)

		for query in queries:
			assert isinstance(query, DatabaseQuery)

	@classmethod
	def test_create_custom_query(cls):
		name = "mergeCustomNode"
		cypher = "MERGE (n:Custom) RETURN n as custom"
		publish_to = "trellis-check-triggers"
		returns = {"custom": "node"}

		custom_query = DatabaseQuery(
            name = name,
            cypher = cypher,
            required_parameters = {},
            write_transaction = True,
            publish_to = publish_to,
            returns = returns,
            active = True,
            split_results = False)

		assert custom_query.name == name
		assert custom_query.cypher == cypher
		assert custom_query.publish_to == publish_to
		assert custom_query.returns == returns

	@classmethod
	def test_load_query_from_file(cls):
		with open("sample_inputs/pilot-db-queries.yaml", "r") as file_handle:
			queries = yaml.load_all(file_handle, Loader=yaml.FullLoader)
			# Convert generator to list
			queries = list(queries)

		assert len(queries) == 8

	@classmethod
	def test_create_custom_query_with_params(cls):
		name = "mergeCustomNode"
		cypher = "MERGE (n:Custom) SET n.cov = $AlignmentCoverage, n.assay = $AssayType, n.blood = $BloodType RETURN n as custom"
		publish_to = "trellis-check-triggers"
		returns = {"custom": "node"}
		
		query_parameters = {'AlignmentCoverage': '33.7162', 'AssayType': 'WGS', 'BloodType': 'O'}

		required_parameters = {}
		for key, value in query_parameters.items():
			required_parameters[key] = type(value).__name__

		custom_query = DatabaseQuery(
			name = name,
			cypher = cypher,
			required_parameters = required_parameters,
			write_transaction = True,
			publish_to = publish_to,
			returns = returns,
			active = True,
			split_results = False)

		assert custom_query.required_parameters['AlignmentCoverage'] == "str"
		assert custom_query.required_parameters['AssayType'] == "str"
		assert custom_query.required_parameters['BloodType'] == "str"

	@classmethod
	def test_create_custom_query_no_returns(cls):
		name = "mergeCustomNode"
		cypher = "MERGE (n:Custom) RETURN n as custom"
		publish_to = "trellis-check-triggers"

		custom_query = DatabaseQuery(
            name = name,
            cypher = cypher,
            required_parameters = {},
            write_transaction = True,
            publish_to = publish_to,
            active = True,
            returns = None,
            split_results = False)

		#pdb.set_trace()

		assert custom_query.name == name
		assert custom_query.cypher == cypher
		assert custom_query.publish_to == publish_to
		assert custom_query.active == True
		assert custom_query.split_results == False

	@classmethod
	def test_write_query_to_file(cls):
		query = yaml.load(tbi_query, Loader=yaml.FullLoader)

		with open("sample_inputs/pilot-write-db-query.yaml", "w") as file_handle:
			yaml.dump(query, file_handle)
		
		with open("sample_inputs/pilot-write-db-query.yaml", "r") as file_handle:	
			queries = yaml.load_all(file_handle, Loader=yaml.FullLoader)
			# Convert generator to list
			queries = list(queries)

		assert len(queries) == 1
		first_query = queries[0]
		
		assert isinstance(first_query, DatabaseQuery) == True
		assert first_query.name == "relateTbiToGvcf"
		assert first_query.required_parameters == {"tbi_id": "str"}

	@classmethod
	def test_append_queries_to_file(cls):
		# Document containing multiple queries to append
		new_queries = yaml.load_all(tbi_queries, Loader=yaml.FullLoader)
		output_doc = "sample_inputs/pilot-write-db-query.yaml"

		# Write a single query to the doc
		#query = yaml.load(tbi_query, Loader=yaml.FullLoader)
		#with open(output_doc, "w") as file_handle:
		#	yaml.dump(query, file_handle)
		
		# Read multiple queries to be written to output
		#with open(query_doc, "r") as file_handle:
		#	new_queries = list(yaml.load_all(file_handle, Loader=yaml.FullLoader))

		# Read the existing queries in the output doc
		with open(output_doc, "r") as file_handle:
			queries = list(yaml.load_all(file_handle, Loader=yaml.FullLoader))
			# Create a list of query names
		
			query_names = [query.name for query in queries]
			for query in new_queries:
				if not query.name in query_names:
					queries.append(query)

		assert len(queries) == 3

		# Write appended list of queries to output doc
		with open(output_doc, "w") as file_handle:
			yaml.dump_all(queries, file_handle)

		# Read the appended queries from the output doc
		with open(output_doc, "r") as file_handle:
			read_queries = list(yaml.load_all(file_handle, Loader=yaml.FullLoader))

		assert len(read_queries) == 3
		#pdb.set_trace()

		for query in read_queries:
			assert isinstance(query, DatabaseQuery) == True

	@classmethod
	def test_equality_of_queries(cls):
		sample_queries = list(yaml.load_all(tbi_queries, Loader=yaml.FullLoader))
		gvcf_query = yaml.load(gvcf_query_doc, Loader=yaml.FullLoader)
		
		relate_gvcf = sample_queries[0]
		relate_fastq = sample_queries[1]

		assert relate_gvcf.active == True
		#pdb.set_trace()
		assert relate_fastq.active == True
		
		assert relate_gvcf == gvcf_query
		assert relate_gvcf != relate_fastq