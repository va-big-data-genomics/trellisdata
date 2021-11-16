import json
import neo4j
import base64

class TrellisMessage(object):

    def __init__(self, *, message_kind=None, sender=None, seed_id=None, previous_event_id=None):
        self.message_kind = message_kind
        self.sender = sender
        self.seed_id = seed_id
        self.previous_event_id = previous_event_id

        self.context = None
        self.header = None
        self.body = None

    def parse_pubsub_message(self, event, context):
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)

        # Not sure what cases caused me to add this to db-query,
        # but keeping it in because I don't see any harm
        if type(data) == str:
            logging.warn("Message data not correctly loaded as JSON. " +
                         "Used eval to convert from string.")
            data = eval(data)
        
        self.context = context
        self.header = data['header']
        self.body = data['body']
        self.sender = self.header['sender']
        
        # The event_id is created by Pub/Sub or the message broker
        # so only get it when parsing messages
        self.event_id = int(context.event_id)
        self.seed_id = int(self.header.get('seedId'))

        
        # If no seed specified, assume this is the seed event
        if not self.seed_id:
            self.seed_id = self.event_id

    def format_json_message(self):
        raise NotImplementedError


class QueryRequest(TrellisMessage):

    def __init__(self,
                 * , # Begin keyword-only arguments
                 sender=None, 
                 seed_id=None, 
                 previous_event_id=None,
                 query_name=None,
                 query_parameters={}, 
                 write_transaction = False, 
                 results_mode = "data", 
                 split_results = True, 
                 publish_results_to = []):
        
        super().__init__(
                         message_kind="queryRequest",
                         sender=sender, 
                         seed_id=seed_id, 
                         previous_event_id=previous_event_id)
        
        self.query_name = query_name
        self.query_parameters = query_parameters
        self.write_transaction = write_transaction
        self.results_mode = results_mode
        self.split_results = split_results
        self.publish_results_to = publish_results_to

    def parse_pubsub_message(self, event, context):
        super().parse_pubsub_message(event, context)

        if self.header['messageKind'] != "queryRequest":
            return ValueError

        # Required fields
        self.previous_event_id = int(self.header['previousEventId'])
        self.query_name = self.body['queryName']
        self.write_transaction = self.body['writeTransaction']
        self.results_mode = self.body['resultsMode']
        self.split_results = self.body['splitResults']

        # Optional fields
        self.query_parameters = self.body.get('queryParameters')
        self.publish_results_to = self.body.get('publishResultsTo')

        if not isinstance(self.publish_results_to, list):
            return ValueError

    def format_json_message(self):
        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    # Trigger name should be named the same as the database query it trigger
                    "queryName": self.query_name,
                    "queryParameters": self.query_parameters,
                    "writeTransaction": self.write_transaction,
                    "resultsMode": self.results_mode,
                    "splitResults": self.split_results,
                    "publishResultsTo": self.publish_results_to
           }
        }
        return message


class QueryResponse(TrellisMessage):

    def __init__(self,
                 sender,
                 seed_id,
                 previous_event_id,
                 *, # Begin keyword-only arguments
                 query_name,
                 graph,
                 result_summary):

        if not isinstance(graph, neo4j.graph.Graph):
            return TypeError
        if not isinstance(result_summary, neo4j.ResultSummary):
            return TypeError

        super().__init__(
                         message_kind = "queryResponse",
                         sender = sender,
                         seed_id = seed_id,
                         previous_event_id = previous_event_id)

        self.query_name = query_name
        self.graph = graph
        self.result_summary = result_summary

        self.nodes = []
        for node in self.graph.nodes:
            self.nodes.append(node)

        self.relationships = []
        for relationship in self.graph.relationships:
            self.relationships.append(relationship)

    def parse_pubsub_message(self, event, context):
        super().parse_pubsub_message(event, context)

        if self.header['messageKind'] != "queryResponse":
            return ValueError

        # Required fields
        self.previous_event_id = int(self.header['previousEventId'])
        self.query_name = self.body['queryName']

        # Optional fields
        self.query_parameters = self.body.get('queryParameters')
        self.nodes = self.body.get('nodes')
        self.relationships = self.body.get('relationships')
        self.result_summary = self.body.get('resultSummary')

    def format_json_message(self):

        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    "queryName": self.query_name,
                    "nodes": [self._get_node_dict(node) for node in self.nodes],
                    "relationships": [self._get_relationship_dict(rel) for rel in self.relationships],
                    "resultSummary": self._get_result_summary_dict(self.result_summary)
           }
        }
        return message

    def format_json_message_iter(self):
        for entity in self.nodes + self.relationships:
            yield self._format_json_message_single_result(entity)

    def _format_json_message_single_result(self, entity):
        result_nodes = None
        result_relationship = None

        if isinstance(entity, neo4j.graph.Node):
            node = [entity]
            relationship = []
        elif isinstance(entity, neo4j.graph.Relationship):
            node = []
            relationship = [entity]

        result_summary_dict = self._get_result_summary_dict(self.result_summary)

        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    "queryName": self.query_name,
                    "nodes": node,
                    "relationships": relationship,
                    "resultSummary": self._get_result_summary_dict(self.result_summary)
           }
        }
        return message

    def _get_result_summary_dict(self, result_summary):
        # Create a copy of the dict so that metadata and server
        # elements are preserved in self.result_summary.
        # ResultSummary source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/work/summary.py
        summary_dict = dict(self.result_summary.__dict__)
        # The metadata field contains the dictionary of information 
        # that is parsed into attributes of the ResultSummary object.
        del summary_dict['metadata']
        # The server value is an instance of the Server class and
        # its attributes contain more classes which would require
        # parsing to text and I'm not that concerned with the
        # information anyway.
        del summary_dict['server']

        summary_dict['counters'] = summary_dict['counters'].__dict__

        return summary_dict

    def _get_relationship_dict(self, relationship):
        """Convert neo4j.graph.Relationship object into dictionary.

        neo4j.graph.Relationship source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/graph/__init__.py

        Args: 
            relationship (neo4j.graph.Relationship): Object with relationship metadata.

        Returns: 
            relationship_dict (dict): Dictionary of metadata stored in Relationship object.
        """

        relationship_dict = {
            "id": relationship.id,
            "start_node": self._get_node_dict(relationship.start_node),
            "end_node": self._get_node_dict(relationship.end_node),
            "type": relationship.type,
            "properties": dict(relationship.items())
        }
        return relationship_dict

    def _get_node_dict(self, node):
        """Convert neo4j.graph.Node object into a dictionary.

        neo4j.graph.Node source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/graph/__init__.py

        Args:
            node (neo4j.graph.Node): Object with node metadata.

        Returns:
            node_dict (dict): Dictionary of metadata stored in the Node object.
        """

        node_dict = {
            "id": node.id,
            "labels": list(node.labels),
            "properties": dict(node.items())

        }
        return node_dict


class JobLauncherResponse(TrellisMessage):

    def __init__(self,
                 sender,
                 seed_id,
                 previous_event_id,
                 *,
                 job_properties):
        
        super().__init__(
                         message_kind = "jobLauncherResponse",
                         sender = sender,
                         seed_id = seed_id,
                         previous_event_id = previous_event_id)
        
        self.job_properties = job_properties

    def parse_pubsub_message(self, event, context):
        super().parse_pubsub_message(event, context)

        if self.header['messageKind'] != "jobLauncherResponse":
            return ValueError

        # Required fields
        self.job_properties = self.body['jobProperties']
    
    def format_json_message(self):
        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    "jobProperties": self.job_properties
           }
        }
        return message
