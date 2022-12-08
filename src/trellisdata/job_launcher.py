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