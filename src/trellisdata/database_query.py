import yaml

class DatabaseQuery(yaml.YAMLObject):
	"""A parameterized Neo4j query.

	Use query parameters whenver possible: https://medium.com/neo4j/neo4j-driver-best-practices-dfa70cf5a763

	args:
		name (str): Name used to request the query.
		query (str): Cypher-formatted Neo4j query.
		parameters (dict): Key-value parameters passed to the query.
		write_transaction (bool): Indicates whether this is a read or write transaction.
		active (bool): Use to activate/deactivate the query.
	"""

	# Create objects from YAML: https://pyyaml.org/wiki/PyYAMLDocumentation
	yaml_tag = u'!DatabaseQuery'

	def __init__(
				 self,
				 name,
				 query,
				 write_transaction,
				 publish_to,
				 returns = None,
				 parameters = {},
				 active = True):
		self.name = name
		self.query = query
		self.write_transaction = write_transaction
		self.publish_to = publish_to
		self.returns = returns
		self.parameters = parameters
		self.active = active
