import yaml

import trellisdata as trellis

from unittest import TestCase

class TestLauncherConfiguration(TestCase):

	def test_load_launcher_document(cls):
		tasks = {}
		launcher_doc = 'sample_inputs/pilot-job-launcher.yaml'
		with open(launcher_doc, 'r') as file_handle:
			task_generator = yaml.load_all(
										   file_handle, 
										   Loader=yaml.FullLoader)
			for task in task_generator:
				tasks[task.name] = task

		task = tasks['fastq-to-ubam']
		assert task.dsub['command']

