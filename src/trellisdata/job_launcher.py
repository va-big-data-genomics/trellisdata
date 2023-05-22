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

	def __init__(task, project_id, provider, trellis_config, start_node, end_node, job_id, input_ids, trunc_nodes_hash):
	    """ Create a dictionary with all the values required
	        to launch a dsub job for this task. This dictionary will
	        then be transformed into a dictionary with the actual
	        key-value dsub arguments. We generate this intermediate 
	        dictionary in a format that is amenable to adding to 
	        Neo4j to create a node representing this job.


	        Args:
	            task (dict): Event payload.
	            provider (str): Indicates which backend should be used by dsub. Reference: https://github.com/DataBiosphere/dsub/blob/main/docs/providers/README.md.
	            start_node (dict): Dictionary with node id, labels, & properties (neo4j.Graph.Node)
	            end_node (dict): Dictionary with node id, labels, & properties (neo4j.Graph.Node)
	            job_id (str):
	            input_ids (list):
	            trunc_nodes_hash (str): 8 character alphanumeric truncated hash value
	        Returns:
	            dictionary: Dsub job arguments
	    """

	    self.env_variables = _get_input_output_values(
	                         	argument_type = "env_variables")
	    self.inputs = _get_input_output_values(
	                			argument_type = "inputs")
	    self.outputs = _get_input_output_values(
	    						argument_type = "outputs")
	    self.outputs = _get_input_output_values(
	    						argument_type = "outputs_recursive")
	    self.dsub_labels = _get_label_values()

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

	
	def _get_input_output_values(self, argument_type):
	    """ Parse job values from the input nodes. Mapping of 
	        node properties to job values is defined in the
	        task configuration document.

	        Args:
	            task (dict): Task template for creating jobs.
	            start_node (dict): Dictionary with node id, labels, & properties (neo4j.Graph.Node)
	            end_node (dict): Dictionary with node id, labels, & properties (neo4j.Graph.Node)
	            params(dict): Template for generating input, output, and
	                environment variables passed to dsub.
	        Returns:
	            dictionary: Dsub job arguments
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
	    
	    sources = {
	        "start": self.start.get('properties'),
	        "end": self.end.get('properties'),
	        "nodes": self.nodes[0].get('properties')
	        "trellis": self.trellis_config
	    }

	    # Inputs must provide either a "value" field with
	    # a static value or a "template" and "source" fields
	    # that will be used to generate value at runtime
	    # from source [start,end] values.
	    # Inspiration: https://stackoverflow.com/questions/54351740/how-can-i-use-f-string-with-a-variable-not-with-a-string-literal
	    job_values = {}
	    for key in task_fields:
	        value = task_fields[key].get('value')
	        if not value:
	            source = task_fields[key]['source']
	            template = task_fields[key]['template']
	            value_type = task_fields[key].get('value_type')
	            value = template.format(**sources[source])
	            
	            if value_type:
	                if not value_type in supported_value_types.keys():
	                    raise ValueError(f"Type {value_type} not in supported types: {supported_value_types.keys()}")
	                else:
	                    value = supported_value_types[value_type](value)
	        job_values[key] = value
	    return job_values

	def create_dsub_job_args(self):
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

class BadJobRequest():

	def __init__(
				 boot_disk_size = 100,
				 disk_size = 100,
				 inputs = {},
				 inputs_recursive = {},
				 trellis_job_id,
				 inputs_hash,
				 outputs = {},
				 environment_vars = {},
				 # Track trellis version. TODO: Should also track task config file version...
				 git_commit_hash,
				 image,
				 input_ids,
				 logging,
				 machine_type,
				 name,
				 network,
				 plate,
				 project,
				 provider,
				 regions,
				 sample,
				 script,
				 subnetwork,
				 user ,
				 zone):

		self.boot_disk_size = boot_disk_size
		self.disk_size = disk_size
		self.inputs = inputs
		self.inputs_recursive = inputs_recursive
		self.trellis_job_id = trellis_job_id
		self.inputs_hash = inputs_hash
		self.outputs = outputs
		self.environment_vars = environment_vars
		self.git_commit_hash = git_commit_hash
		self.image = image
		self.input_ids = input_ids
		self.logging = logging
		self.machine_type = machine_type
		self.name = name
		self.network = network
		self.plate = plate
		self.project = project
		self.provider = provider
		self.regions = regions
		self.sample = sample
		self.script = script
		self.subnetwork = subnetwork
		self.user = user
		self.zone = zone


		self.input_hash = self._get_inputs_hash()
		self.dsub_command = self.get_dsub_cmd()
		
		# Must get Dsub job id before creating
		self.dstat_command = None

	def _get_dsub_command(self):
		dsub_cmd_args = self.dsub_args.copy()
    	dsub_cmd_args.insert(0, "dsub")
    	for arg in dsub_cmd_args:
        	dsub_cmd = " ".join(dsub_cmd_args)
        return dsub_cmd

    def _get_dstat_command(self):
    	if not self.dsub_job_id:
    		raise AttributeError("Attribute 'dsub_job_id' has not been set.")
        dstat_command = (
            "dstat " +
            f"--project {self.project} " +
            f"--provider {self.provider} " +
            f"--jobs '{self.dsub_job_id}' " +
            f"--users '{self.user}' " +
            "--full " +
            "--format json " +
            "--status '*'"
        )
        return dstat_command

    def _get_inputs_hash(self):

    def set_dsub_job_id(self, job_id)
    	self.dsub_job_id = job_id