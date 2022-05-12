import pdb
import yaml
import neo4j

from .database_query import DatabaseQuery
from .database_trigger import DatabaseTrigger

class OperationGrapher:

	def __init__(self, query_document, trigger_document, neo4j_driver):
		self.neo4j_driver = neo4j_driver

		# Read in the queries
		with open(query_document, 'r') as query_file_handle:
			self.queries = list(yaml.load_all(query_file_handle, Loader=yaml.FullLoader))
		# Read in the triggers
		with open(trigger_document, 'r') as trigger_file_handle:
			self.triggers = list(yaml.load_all(trigger_file_handle, Loader=yaml.FullLoader))

	# Create a node for each database query with properties describing outputs
	def add_query_nodes_to_neo4j(self):
		query_nodes = []
		for query in self.queries:

			# Only exists for relationship patterns
			relationship = query.returns.get('relationship')
			end = query.returns.get('end')

			with self.neo4j_driver.session() as session:
				query_node = session.write_transaction(
								self._add_query_node_to_neo4j,
								name = query.name,
								pattern = query.returns['pattern'],
								start = query.returns['start'],
								relationship = relationship,
								end = end)
				query_nodes.append(query_node)
		return query_nodes

	# Create a node for each database trigger with properties describing trigger conditions
	def add_trigger_nodes_to_neo4j(self):
		trigger_nodes = []
		for trigger in self.triggers:

			# Only exists for relationship patterns
			if hasattr(trigger, 'relationship'):
				relationship = trigger.relationship['type']
			else:	
				relationship = None
				
			if hasattr(trigger, 'end'):
				end = trigger.end['label']
			else:
				end = None

			with self.neo4j_driver.session() as session:
				trigger_node = session.write_transaction(
								self._add_trigger_node_to_neo4j,
								name = trigger.name,
								pattern = trigger.pattern,
								start = trigger.start['label'],
								activates_query = trigger.query,
								relationship = relationship,
								end = end)
				trigger_nodes.append(trigger_node)
		return trigger_nodes

	def connect_nodes(self):
			with self.neo4j_driver.session() as session:
				print("Connecting relationship based operations")
				result_summary = session.write_transaction(self._connect_relationship_operations)
				print(result_summary.counters)

				print("Connecting node based operations")
				result_summary = session.write_transaction(self._connect_node_operations)
				print(result_summary.counters)

				print("Connecting triggers to activated queries")
				result_summary = session.write_transaction(self._connect_triggers_to_activated_queries)
				print(result_summary.counters)

	@staticmethod
	def _add_query_node_to_neo4j(tx, name, pattern, start, relationship, end):
		result = tx.run(
						"MERGE (n:Query {name: $name}) "
						"SET n.output_pattern = $pattern, "
						"n.start = $start, " 
						"n.relationship = $relationship, "
						"n.end = $end "
						"RETURN n",
						name = name,
						pattern = pattern,
						start = start,
						relationship = relationship,
						end = end)
		return result.single()[0]

	@staticmethod
	def _add_trigger_node_to_neo4j(tx, name, pattern, start, activates_query, relationship, end):
		result = tx.run(
						"MERGE (t:Trigger {name: $name}) "
						"SET t.pattern = $pattern, "
						"t.start = $start, " 
						"t.relationship = $relationship, "
						"t.end = $end, "
						"t.activates_query = $activates_query "
						"RETURN t",
						name = name,
						pattern = pattern,
						start = start,
						activates_query = activates_query,
						relationship = relationship,
						end = end)
		return result.single()[0]

	# Run query to connect relationship based operations
	@staticmethod
	def _connect_relationship_operations(tx):
		result = tx.run(
						"MATCH (q:Query), (t:Trigger) "
						"WHERE q.output_pattern = 'relationship' "
						"AND t.pattern = 'relationship' "
						"AND q.start = t.start "
						"AND q.relationship = t.relationship "
						"AND q.end = t.end "
						"MERGE (q)-[r:ACTIVATES]->(t) "
						"RETURN r")
		return result.consume()

	# Run query to connect node based operations
	@staticmethod
	def _connect_node_operations(tx):
		result = tx.run(
						"MATCH (q:Query), (t:Trigger) "
						"WHERE q.output_pattern = 'node' "
						"AND t.pattern = 'node' "
						"AND q.start = t.start "
						"MERGE (q)-[r:ACTIVATES]->(t) "
						"RETURN r")
		return result.consume()

	# Run query to connect triggers to activated queries
	@staticmethod
	def _connect_triggers_to_activated_queries(tx):
		result = tx.run(
						"MATCH (t:Trigger), (q:Query) "
						"WHERE t.activates_query = q.name "
						"MERGE (t)-[r:REQUESTS]->(q) "
						"RETURN r")
		return result.consume()

