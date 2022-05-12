import pdb
import yaml

from unittest import TestCase

from trellisdata import DatabaseQuery

tbi_query = """
--- !DatabaseQuery
name: relateTbiToGvcf
query: "MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE tbi.id = $tbi_id AND step.wdlCallAlias = 'mergevcfs' MERGE (gvcf)-[:HAS_INDEX {ontology: 'bioinformatics'}]->(tbi) RETURN gvcf AS node"
parameters:
  tbi_id: str
write_transaction: true
publish_to: test-function
returns:
  gvcf: node
"""

tbi_queries = """
--- !DatabaseQuery
name: relateGvcfToTbi
query: "MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE step.cromwellWorkflowId = $cromwell_id AND step.wdlCallAlias = 'mergevcfs' MERGE (gvcf)-[r:HAS_INDEX {ontology: 'bioinformatics'}]->(tbi) RETURN gvcf, r, tbi"
parameters:
  cromwell_id: str
write_transaction: true
publish_to: test-function
returns:
  gvcf: node
  r: relationship
  tbi: node
--- !DatabaseQuery
name: relateTbiToGvcf
query: "MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE tbi.id = $tbi_id AND step.wdlCallAlias = 'mergevcfs' MERGE (gvcf)-[:HAS_INDEX {ontology: 'bioinformatics'}]->(tbi) RETURN gvcf AS node"
parameters:
  tbi_id: str
write_transaction: true
publish_to:
active: true
returns:
  gvcf: node
"""

class TestDatabaseQuery(TestCase):

	@classmethod
	def test_create_database_query(cls):
		query = yaml.load(tbi_query, Loader=yaml.FullLoader)

		assert isinstance(query, DatabaseQuery) == True
		assert query.name == "relateTbiToGvcf"
		assert query.parameters == {"tbi_id": "str"}

	@classmethod
	def test_create_multiple_queries(cls):
		queries = yaml.load_all(tbi_queries, Loader=yaml.FullLoader)

		for query in queries:
			assert isinstance(query, DatabaseQuery)

	@classmethod
	def test_create_custom_query(cls):
		name = "mergeCustomNode"
		query = "MERGE (n:Custom) RETURN n as custom"
		publish_to = "trellis-check-triggers"
		returns = {"custom": "node"}

		custom_query = DatabaseQuery(
            name = name,
            query = query,
            required_parameters = {},
            write_transaction = True,
            publish_to = publish_to,
            returns = returns)

		assert custom_query.name == name
		assert custom_query.query == query
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
		query = "MERGE (n:Custom) SET n.cov = $AlignmentCoverage, n.assay = $AssayType, n.blood = $BloodType RETURN n as custom"
		publish_to = "trellis-check-triggers"
		returns = {"custom": "node"}
		
		query_parameters = {'AlignmentCoverage': '33.7162', 'AssayType': 'WGS', 'BloodType': 'O'}

		required_parameters = {}
		for key, value in query_parameters.items():
			required_parameters[key] = type(value).__name__

		custom_query = DatabaseQuery(
			name = name,
			query = query,
			required_parameters = required_parameters,
			write_transaction = True,
			publish_to = publish_to,
			returns = returns)

		assert custom_query.required_parameters['AlignmentCoverage'] == "str"
		assert custom_query.required_parameters['AssayType'] == "str"
		assert custom_query.required_parameters['BloodType'] == "str"

	@classmethod
	def test_create_custom_query_no_returns(cls):
		name = "mergeCustomNode"
		query = "MERGE (n:Custom) RETURN n as custom"
		publish_to = "trellis-check-triggers"

		custom_query = DatabaseQuery(
            name = name,
            query = query,
            required_parameters = {},
            write_transaction = True,
            publish_to = publish_to)

		assert custom_query.name == name
		assert custom_query.query == query
		assert custom_query.publish_to == publish_to