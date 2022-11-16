import pdb
import yaml
import neo4j

from .database_query import DatabaseQuery
from .database_trigger import DatabaseTrigger

class OperationGrapher:

	def __init__(self, query_document, trigger_document, neo4j_driver):
		self.neo4j_driver = neo4j_driver
		self.queries = []
		self.triggers = []

		# Read in the queries
		with open(query_document, 'r') as query_file_handle:
			self.queries = list(yaml.load_all(query_file_handle, Loader=yaml.FullLoader))
			for query in self.queries:
				# Will raise error on failure
				self._is_query_valid(query)
			
		# Read in the triggers
		with open(trigger_document, 'r') as trigger_file_handle:
			self.triggers = list(yaml.load_all(trigger_file_handle, Loader=yaml.FullLoader))
			for trigger in self.triggers:
				self._is_trigger_valid(trigger)


	# Create a node for each database query with properties describing outputs
	def add_query_nodes_to_neo4j(self):
		query_nodes = []
		for query in self.queries:
			publish_to = query.publish_to

			for pattern in query.returns:
				# Only exists for relationship patterns
				start = pattern['start']
				relationship = pattern.get('relationship')
				end = pattern.get('end')
				
				if relationship and end:
					pattern = 'relationship'
				elif not relationship and not end:
					pattern = 'node'
				else:
					return ValueError("Query return pattern is not valid for {query.name}.")

				with self.neo4j_driver.session() as session:
					query_node = session.write_transaction(
									self._add_query_node_to_neo4j,
									name = query.name,
									pattern = pattern,
									start = start,
									relationship = relationship,
									end = end,
									publish_to = publish_to)
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
				result_summary = session.write_transaction(self._connect_rel_query_results_to_trigger)
				print(result_summary.counters)

				print("Connecting node based operations")
				result_summary = session.write_transaction(self._connect_node_query_results_to_trigger)
				print(result_summary.counters)

				print("Connecting triggers to activated queries")
				result_summary = session.write_transaction(self._connect_triggers_to_activated_queries)
				print(result_summary.counters)

	@staticmethod
	def _is_query_valid(query):
		required_value_fields = {
			"name": str,
			"cypher": str,
			"write_transaction": bool,
			"aggregate_results": bool,
		}
		optional_value_fields = {
			"parameters": dict,
			"publish_to": list,
			"indexes": dict,
			"returns": list
		}

		if not hasattr(query, "name"):
			raise AttributeError("Query is missing name.")

		# Code to handle fields that require values
		for field, value_type in required_value_fields.items():
			if not hasattr(query, field):
				raise AttributeError(f"Query {query.name} is missing '{field}'.")
			value = getattr(query, field)
			if value == None:
				raise ValueError(f"Query {query.name} is missing value for required field '{field}'.")
			if not isinstance(value, value_type):
				raise ValueError(f"Query {query.name} '{field}' is not of type {value_type}.")

		# Code to handle fields that do not require values
		for field, value_type in optional_value_fields.items():
			if not hasattr(query, field):
				continue
			value = getattr(query, field)
			if value == None:
				continue
			if not isinstance(value, value_type):
				raise ValueError(f"Query {query.name} '{field}' is not of type {value_type}.")

	@staticmethod
	def _is_trigger_valid(trigger):
		required_value_fields = {
			"name": str,
			"pattern": str,
			"start": dict,
			"query": str
		}

		optional_value_fields = {
			"relationship": dict,
			"end": dict
		}

		if not hasattr(trigger, "name"):
			raise AttributeError("Trigger is missing name.")

		# Code to handle fields that require values
		for field, value_type in required_value_fields.items():
			if not hasattr(trigger, field):
				raise AttributeError(f"Trigger {trigger.name} is missing '{field}'.")
			value = getattr(trigger, field)
			if value == None:
				raise ValueError(f"Trigger {trigger.name} is missing value for required field '{field}'.")
			if not isinstance(value, value_type):
				raise ValueError(f"Trigger {trigger.name} '{field}' is not of type {value_type}.")

		# Code to handle fields that do not require values
		for field, value_type in optional_value_fields.items():
			if not hasattr(trigger, field):
				continue
			value = getattr(trigger, field)
			if value == None:
				raise ValueError(f"Trigger {trigger.name} is missing value for optional field '{field}'.")
			if not isinstance(value, value_type):
				raise ValueError(f"Trigger {trigger.name} '{field}' is not of type {value_type}.")

	@staticmethod
	def _add_query_node_to_neo4j(tx, name, pattern, start, relationship, end, publish_to):
		result = tx.run(
						"MERGE (n:Query {name: $name}) "
						"SET n.outputPattern = $pattern, "
						"n.start = $start, " 
						"n.relationship = $relationship, "
						"n.end = $end, "
						"n.publishTo = $publish_to "
						"RETURN n",
						name = name,
						pattern = pattern,
						start = start,
						relationship = relationship,
						end = end,
						publish_to = publish_to)
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
	def _connect_rel_query_results_to_trigger(tx):
		result = tx.run(
						"MATCH (q:Query), (t:Trigger) "
						"WHERE q.outputPattern = 'relationship' "
						"AND t.pattern = 'relationship' "
						"AND q.start = t.start "
						"AND q.relationship = t.relationship "
						"AND q.end = t.end "
						"AND 'TOPIC_TRIGGERS' IN q.publishTo "
						"MERGE (q)-[r:ACTIVATES]->(t) "
						"RETURN r")
		return result.consume()

	# Run query to connect node based operations
	@staticmethod
	def _connect_node_query_results_to_trigger(tx):
		result = tx.run(
						"MATCH (q:Query), (t:Trigger) "
						"WHERE q.outputPattern = 'node' "
						"AND t.pattern = 'node' "
						"AND q.start = t.start "
						"AND 'TOPIC_TRIGGERS' IN q.publishTo "
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

