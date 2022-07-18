import yaml
#import ruamel.yaml

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

	# Note: I also looked into ruamel for YAML handling (https://yaml.readthedocs.io/en/latest/)
	#    * safer loading of Python classes
	#    * better handling of read/write cycles
	# But, it had poor documentation and I got pyyaml to work

	# Create objects from YAML: https://pyyaml.org/wiki/PyYAMLDocumentation
	yaml_tag = u'!DatabaseQuery'

	def __init__(
				 self,
				 name,
				 cypher,
				 write_transaction,
				 publish_to,
				 returns,
				 required_parameters,
				 active=True,
				 aggregate_results=False):
		self.name = name
		self.cypher = cypher
		self.write_transaction = write_transaction
		self.publish_to = publish_to
		self.aggregate_results = aggregate_results
		self.returns = returns
		self.required_parameters = required_parameters
		self.active = active

	def __eq__(self, other):
		# https://stackoverflow.com/questions/1227121/compare-object-instances-for-equality-by-their-attributes
		if not isinstance(other, DatabaseQuery):
			# don't attempt to compare against unrelated types
			return NotImplemented

		return(
			   self.name == other.name and
			   self.cypher == other.cypher and
			   self.write_transaction == other.write_transaction and
			   self.publish_to == other.publish_to and
			   self.aggregate_results == other.aggregate_results and
			   self.returns == other.returns and
			   self.required_parameters == other.required_parameters and
			   self.active == other.active)
