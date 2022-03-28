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
            parameters = {},
            write_transaction = True,
            publish_to = publish_to,
            returns = returns)

		assert custom_query.name == name
		assert custom_query.query == query
		assert custom_query.publish_to == publish_to
		assert custom_query.returns == returns

	@classmethod
	def test_load_query_from_file(cls):
		with open("sample-database-query.yaml", "r") as file_handle:
			queries = yaml.load_all(file_handle, Loader=yaml.FullLoader)
			# Convert generator to list
			queries = list(queries)

		assert len(queries) == 1
		query = queries[0]
		assert query.name == 'relateTbiToGvcf'
