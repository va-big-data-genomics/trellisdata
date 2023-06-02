import pdb
import json
import mock
import yaml
import base64

import trellisdata as trellis

from unittest import TestCase

class TestLauncherConfiguration(TestCase):

    def test_load_launcher_document(cls):
        tasks = {}
        launcher_doc = 'sample_inputs/job-launcher.yaml'
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
    tasks = {}
    launcher_document = "sample_inputs/job-launcher.yaml"
    with open(launcher_document, "r") as file_handle:
        task_generator = yaml.load_all(
            file_handle,
            Loader=yaml.FullLoader)
        for task in task_generator:
            tasks[task.name] = task

    nodes = [
        {
            'id': 157, 
            'labels': ['Fastq'], 
            'properties': {
                'id': 'mvp-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R1.fastq.gz/1582075925383876',
                'name': 'SHIP123_2_R1'
            }
        }, 
        {
            'id': 158, 
            'labels': ['Fastq'], 
            'properties': {
                'id': 'mvp-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R1.fastq.gz/1582075925383876',
                'name': 'SHIP123_2_R2'
            }
        }
    ]

    relationship = {
                    'id': 78, 
                    'start_node': {
                        'id': 157, 
                        'labels': ['Fastq'], 
                        'properties': {
                            'id': 'mvp-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R1.fastq.gz/1582075925383876',
                            'name': 'SHIP123_2_R1'
                        }
                    }, 
                    'end_node': {
                        'id': 158, 
                        'labels': ['Fastq'], 
                        'properties': {
                            'id': 'mvp-bucket/va_mvp_phase2/DVALABP000123/SHIP123/FASTQ/SHIP123_2_R1.fastq.gz/1582075925383876',
                            'name': 'SHIP123_2_R2'
                        }
                    }, 
                    'type': 'HAS_MATE_PAIR', 
                    'properties': {}
    }

    # Load query response
    query_response_file = "sample_inputs/query_response.json"
    with open(query_response_file, "r") as file_handle:
        data = json.load(file_handle)
        data_str = json.dumps(data)
        data_utf8 = data_str.encode('utf-8')
        event = {'data': base64.b64encode(data_utf8)}

    mock_context = mock.Mock()
    mock_context.event_id = '617187464135194'
    mock_context.timestamp = '2019-07-15T22:09:03.761Z'

    query_response = trellis.QueryResponseReader(
                        mock_context,
                        event)

    # Load Trellis config
    trellis_config_file = "sample_inputs/trellis-config.yaml"
    with open(trellis_config_file, "r") as file_handle:
        trellis_config = yaml.safe_load(trellis_config_file)

    def test_create_job_request(cls):

        # How to handle jobs with multiple node inputs?
        # Database triggers can be activated by a node or
        # two related nodes, but jobs can involve multiple 
        # start or end nodes
        task = cls.tasks['fastq-to-ubam']
        job_request = trellis.JobRequest(
                                         task = task,
                                         nodes = cls.nodes)
        assert(job_request.trunc_nodes_hash == 'f0f19705')

        #job_request.get_job_sources(
        #                            query_response = query_response, 
        #                            trellis_config = trellis_config)
        # Map variables with templated definitions defined in the 
        # task configuration to actual values derived from the 
        # graph data in the query response and Trellis configuration
        # data.
        #job_request._get_job_inputs()
        #dsub_args = job_request._get_dsub_args(trellis_config)

    def test_get_job_sources(cls):
        task = cls.tasks['fastq-to-ubam']
        job_request = trellis.JobRequest(
                                         task = task,
                                         nodes = cls.nodes)
        job_sources = job_request.get_job_sources(
            query_response = cls.query_response, 
            trellis_config = cls.trellis_config)
        pdb.set_trace()