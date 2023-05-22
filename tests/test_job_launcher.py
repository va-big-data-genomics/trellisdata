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

class TestJobRequest(TestCase):

	project_id = 'test-project'
	provider = 'test-provider'

	# Set Trellis configuration
	trellis_config = {
		'DSUB_OUT_BUCKET': 'dsub-out-bucket',
		'DSUB_USER': 'trellis',
		'DSUB_REGIONS': "us-west1",
		'DSUB_NETWORK': "trellis",
		'DSUB_SUBNETWORK': "trellis-subnet",
		'DSUB_LOG_BUCKET': "logs-bucket",
	}

	# Load job launcher configuration document
	launcher_document = "sample_inputs/job-launcher.yaml"
	task_generator = yaml.load_all(
						launcher_document,
						Loader=yaml.FullLoader)
    tasks = {}
    for task in task_generator:
        tasks[task.name] = task

	def test_create_job_request(cls):

		# How to handle jobs with multiple node inputs?
		# Database triggers can be activated by a node or
		# two related nodes, but jobs can involve multiple 
		# start or end nodes
		task = cls.tasks['fastq-to-ubam']
		job_request = trellis.JobRequest(
										 task,
										 cls.project_id,
										 cls.provider,
										 cls.trellis_config,
										 start_node,
										 end_node,
										 job_id, 
										 input_ids,
										 trunc_nodes_hash)
