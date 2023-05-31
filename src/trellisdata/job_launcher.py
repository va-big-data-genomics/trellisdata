import yaml

class JobLauncher(yaml.YAMLObject):
	yaml_tag = u'!JobLauncher'
	def __init__(
			     self, 
				 name,
				 inputs,
				 virtual_machine,
				 dsub):
		self.name = name
		self.inputs = inputs
		self.virtual_machine = virtual_machine
		self.dsub = dsub

class JobRequest():

	def __init__(task):
	    """ Create a dictionary with all the values required
	        to launch a dsub job for this task. This dictionary will
	        then be transformed into a dictionary with the actual
	        key-value dsub arguments. We generate this intermediate 
	        dictionary in a format that is amenable to adding to 
	        Neo4j to create a node representing this job.


	        Args:
	            task (dict): Task definition loaded from YAML
	            trellis_config (disk): General Trellis configuration data loaded from YAML
	            start_node (dict): Dictionary with node id, labels, & properties (neo4j.Graph.Node)
	            end_node (dict): Dictionary with node id, labels, & properties (neo4j.Graph.Node)
	        Returns:
	            dictionary: Dsub job arguments
	    """
	    #self.job_sources = self._get_job_sources(task, query_response)
	    
	    self.task = task

	    self.job_id, self.trunc_nodes_hash = self._make_unique_job_id()


	    # Generate dsub arguments by mapping task configuration
	    # variables to values defined in the JobRequest instance,
	    # Trellis configuration, or input nodes.
	    self.env_variables = self._get_input_output_values(
	                         	argument_type = "env_variables")
	    self.inputs = self._get_input_output_values(
	                			argument_type = "inputs")
	    self.outputs = self._get_input_output_values(
	    						argument_type = "outputs")
	    self.outputs = self._get_input_output_values(
	    						argument_type = "outputs_recursive")
	    self.dsub_labels = self._get_label_values()

	    # Use camelcase keys for this dict because it will be added to Neo4j
	    # database where camelcase is the standard.
	    # TODO: convert these all to attributes
	    self.name = task.name
	    self.dsub_name = f"{task.dsub_prefix}-{trunc_nodes_hash[0:5]}",
	    self.inputs_hash = trunc_nodes_hash
	    self.input_ids = input_ids
	    self.trellis_task_id = job_id
	    self.provider = provider
	    self.user = trellis_config['DSUB_USER']
	    self.regions = trellis_config['DSUB_REGIONS']
	    self.project = project_id
	    self.network = trellis_config['DSUB_NETWORK']
	    self.subnetwork = trellis_conifg['DSUB_SUBNETWORK']
	    self.min_cores = task.virtual_machine["min_cores"]
	    self.min_ram = task.virtual_machine["min_ram"]
	    self.boot_disk_size = task.virtual_machine["boot_disk_size"]
	    self.image = f"gcr.io/{project_id}/{task.virtual_machine['image']}"
	    self.logging = f"gs://{trellis_config['DSUB_LOG_BUCKET']}/{task.name}/{job_id}/logs"
	    self.disk_size = task.virtual_machine['disk_size']
	    self.preemptible = task.dsub['preemptible']
	    self.command = task.dsub['command']

	def _make_unique_job_id():
	    """ Create pretty-unique hash value based on input node 
	        properties. Ignore labels and node Ids.
	        Inspiration for hashing algorithm: https://www.geeksforgeeks.org/ways-sort-list-dictionaries-values-python-using-lambda-function/

	        Args:
	            nodes (list): List of node dictionaries output from Neo4j Graph
	        
	        Returns:
	            task_id (str): Combination of datetime stamp and nodes hash.
	                Format: '{date}-{time}-{time}-{nodes hash}'
	                Example: '221214-162717-045-5d709493'
	            trunc_nodes_hash (str): Hash value created from 
	                combined properties of all nodes.
	    """
	    nodes_properties = []
	    for node in self.nodes:
	        nodes_properties.append(node['properties'])
	    sorted_nodes = sorted(nodes_properties, key = lambda i: i['id'])
	    nodes_str = json.dumps(sorted_nodes, sort_keys=True, ensure_ascii=True, default=str)
	    nodes_hash = hashlib.sha256(nodes_str.encode('utf-8')).hexdigest()
	    print(nodes_hash)
	    trunc_nodes_hash = str(nodes_hash)[:8]
	    datetime_stamp = _get_datetime_stamp()
	    task_id = f"{datetime_stamp}-{trunc_nodes_hash}"
	    return(task_id, trunc_nodes_hash)

	def _get_input_output_values(self, argument_type):
	    """ Parse job values from the input nodes. Mapping of 
	        node properties to job values is defined in the
	        task configuration document.

	        Args:
	            argument_type (str): Indicate which kind of dsub argument is
	            			being created: ['inputs', 'outputs', 
	            			'env_variables', 'outputs_recursive'].
	        Returns:
	            dictionary: Job arguments
	    """
	    # Used for transforming values from default type (str)
	    supported_value_types = {
	        "int": int,
	        "float": float
	    }
	    supported_argument_types = [
	        'inputs', 
	        'env_variables',
	        'outputs',
	        'outputs_recursive'
	    ]
	    if not argument_type in supported_argument_types:
	        raise ValueError(f"{argument_type} is not a supported type: {supported_argument_types}")
	    
	    task_fields = self.task.dsub[argument_type]

	    # Inputs must provide either a "value" field with
	    # a static value or a "template" and "source" fields
	    # that will be used to generate value at runtime
	    # from source [start,end] values.
	    # Inspiration: https://stackoverflow.com/questions/54351740/how-can-i-use-f-string-with-a-variable-not-with-a-string-literal
	    job_values = {}
	    for key in task_fields:
	    	# Values here represent static values provided by the user
	        value = task_fields[key].get('value')
	        # The alternative to a value is a variable reference that
	        # the Trellis will dynamically fetch the value for, from
	        # the Trellis configuration or provided nodes.
	        if not value:
	            # May 31: Source is now specified in the `template` field
	            #source = task_fields[key]['source']
	            template = task_fields[key]['template']
	            value_type = task_fields[key].get('value_type')
	            
	            # May 31: Need to parse the source from the template 
	            # and then get value.
	            value = template.format(**sources[source])
	            
	            if value_type:
	                if not value_type in supported_value_types.keys():
	                    raise ValueError(f"Type {value_type} not in supported types: {supported_value_types.keys()}")
	                else:
	                    value = supported_value_types[value_type](value)
	        job_values[key] = value
	    return job_values

	def _create_dsub_args(self, trellis_config):
	    """ Convert the job description dictionary into a list
	        of dsub supported arguments.

	    Args:
	        neo4j_job_dict (dict): Event payload.
	    Returns:
	        list: List of "--arg", "value" pairs which will
	            be passed to dsub.
	    """

	    dsub_args = [
	        "--name", self.dsub_name,
	        "--label", f"trellis-id={self.trellis_task_id}",
	        "--label", f"trellis-name={self.name}",
	        "--label", f"wdl-call-alias={self.name}",
	        "--provider", self.provider, 
	        "--user", self.user, 
	        "--regions", self.regions,
	        "--project", self.project,
	        "--min-cores", str(self.minCores), 
	        "--min-ram", str(self.minRam),
	        "--boot-disk-size", str(self.bootDiskSize), 
	        "--image", self.image, 
	        "--logging", self.logging,
	        "--disk-size", self.diskSize,
	        "--command", self.command,
	        "--use-private-address",
	        "--network", self.network,
	        "--subnetwork", self.subnetwork,
	        "--enable-stackdriver-monitoring",
	    ]

	    # Argument lists
	    for key, value in self.inputs.items():
	        dsub_args.extend([
	                          "--input", 
	                          f"{key}={value}"])
	    for key, value in self.environment_variables.items():
	        dsub_args.extend([
	                          "--env",
	                          f"{key}={value}"])
	    for key, value in self.outputs.items():
	        dsub_args.extend([
	                          "--output",
	                          f"{key}={value}"])
	    for key, value in self.dsub_labels.items():
	        dsub_args.extend([
	                          "--label",
	                          f"{key}={value}"])

	    return dsub_args

	def _get_job_inputs(self):
	    # Generate dsub arguments by mapping task configuration
	    # variables to values defined in the JobRequest instance,
	    # Trellis configuration, or input nodes.
	    self.env_variables = self._get_input_output_values(
	                         	argument_type = "env_variables")
	    self.inputs = self._get_input_output_values(
	                			argument_type = "inputs")
	    self.outputs = self._get_input_output_values(
	    						argument_type = "outputs")
	    self.outputs = self._get_input_output_values(
	    						argument_type = "outputs_recursive")
	    self.dsub_labels = self._get_label_values()
		
	def get_job_sources(self, query_response, trellis_config):
		"""Map graph data provided in the query response to
  		   job variable sources defined in the task configuration.
		Arguments:
			query_response (trellis.QueryResponse): Graph data
				returned by the query associated with the job
				trigger.
			trellis_config (Dict): Trellis configuration data.
		"""

		self.job_sources = {
			"trellis" = trellis_config
			"job" = self
		}
	    for source in self.task.sources:
	        if source['graph-status'] == 'independent':
	            # Look for the source in query_response.nodes
	            # Find matching nodes by label
	            nodes = []
	            for node in query_response.nodes:
	                if source['start'] in node.labels:
	                    nodes.append(node)
	            # Check whether cardinality matches
	            if source['cardinality'] == 'one':
	                if len(nodes) != 1:
	                    return ValueError("Expected one node labelled {source['start']} but found {len(nodes)}.")        
	                self.job_sources[source] = nodes[0]
	            elif source['cardinality'] == 'many':
	                self.job_sources[source] = nodes
	            else:
	                return ValueError("Cardinality '{source['cardinality']}' is not supported.")
	        elif source['graph-status'] == 'relationship':
	            # Look for the source in query_response.relationship(s?)
	            # Find matching nodes by label
	            # Find matching relationship by type
	            relationships = []
	            for relationship in query_response.relationships:
	                if (source['start'] in relationship.nodes[0].labels 
	                        and source['relationships'] == relationship.type
	                        and source['end'] in relationships.nodes[1].labels):
	                    relationships.append(relationship)
	            # Check wheter cardinality matches
	            if source['cardinality'] == 'one':
	                if len(relationships) != 1:
	                    return ValueError("Expected one relationship but found {len(relationships)}.")
	                self.job_sources[source] = relationships[0]
	            elif source['cardinality'] == 'many':
	                self.job_sources[source] = relationships
	            else:
	                return ValueError("Cardinality '{source['cardinality']}' is not supported.")

	def configure_dsub_job(self):
		self._get_job_inputs()
		dsub_args = self.-create_dsub_args()
		return dsub_args