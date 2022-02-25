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
"""

tbi_queries = """
--- !DatabaseQuery
name: relateGvcfToTbi
query: "MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE step.cromwellWorkflowId = $cromwell_id AND step.wdlCallAlias = 'mergevcfs' MERGE (gvcf)-[r:HAS_INDEX {ontology: 'bioinformatics'}]->(tbi) RETURN gvcf, r, tbi"
parameters:
  cromwell_id: str
write_transaction: true
--- !DatabaseQuery
name: relateTbiToGvcf
query: "MATCH (gvcf:Gvcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Tbi) WHERE tbi.id = $tbi_id AND step.wdlCallAlias = 'mergevcfs' MERGE (gvcf)-[:HAS_INDEX {ontology: 'bioinformatics'}]->(tbi) RETURN gvcf AS node"
parameters:
  tbi_id: str
write_transaction: true
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
