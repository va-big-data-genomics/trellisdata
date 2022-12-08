import json
import neo4j
import base64


class MessageWriter(object):

    def __init__(
                 self,
                 sender,
                 seed_id,
                 previous_event_id):

        self.message_kind = "unspecifiedWriter"
        self.sender = sender
        self.seed_id = seed_id
        self.previous_event_id = previous_event_id

    def format_json_header(self):

        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           }
        }
        return message


class QueryRequestWriter(MessageWriter):

    def __init__(
                 self,
                 *, # Begin keyword-only arguments
                 sender,
                 seed_id,
                 previous_event_id,
                 query_name,
                 query_parameters,
                 custom=False,
                 cypher=None,
                 write_transaction=False,
                 aggregate_results=False,
                 publish_to=None,
                 returns={}):

        super().__init__(
                         sender,
                         seed_id,
                         previous_event_id)

        self.message_kind = "queryRequest"
        self.query_name = query_name
        self.query_parameters = query_parameters
        self.custom = custom
        self.cypher = cypher
        self.write_transaction = write_transaction
        self.aggregate_results = aggregate_results
        self.publish_to = publish_to
        self.returns = returns

    def format_json_message(self):
        message = super().format_json_header()
        body = {
                "body": {
                    "queryName": self.query_name,
                    "queryParameters": self.query_parameters,
                    # Adding support for custom queries
                    "custom": self.custom
                }
        }
        message.update(body)

        if self.custom:
            custom_fields = {
                "cypher": self.cypher,
                "writeTransaction": self.write_transaction,
                "aggregateResults": self.aggregate_results,
                "publishTo": self.publish_to,
                "returns": self.returns
            }
            message['body'].update(custom_fields)
        return message


class QueryResponseWriter(MessageWriter):

    def __init__(
                 self,
                 *,
                 sender,
                 seed_id,
                 previous_event_id,
                 query_name,
                 result_summary,
                 graph,
                 job_request=None):

        super().__init__(
                         sender,
                         seed_id,
                         previous_event_id)

        self.message_kind = "queryResponse"
        self.query_name = query_name
        self.result_summary = result_summary
        self.graph = graph
        self.nodes = []
        self.relationships = []
        self.supported_patterns = ['node', 'relationship']
        self.job_request = job_request

        if self.graph:
            for node in self.graph.nodes:
                self.nodes.append(node)

            for relationship in self.graph.relationships:
                self.relationships.append(relationship)

        if self.relationships:
            self.pattern = "relationship"
        else:
            self.pattern = "node"

    def return_json_with_all_nodes(self):
        if self.pattern == 'relationship':
            raise ValueError("Cannot use this method to publish relationships. Use 'generate_separate_entity_jsons()' instead.")

        message = super().format_json_header()

        body = {
           "body": {
                    "queryName": self.query_name,
                    "jobRequest": self.job_request,
                    "nodes": [self._get_node_dict(node) for node in self.nodes],
                    # Note: Each relationship triple should be delivered separately using 
                    # the generate_individual_jsons() method.
                    "relationship": {},
                    #"relationships": [self._get_relationship_dict(rel) for rel in self.relationships],
                    "resultSummary": self._get_result_summary_dict(self.result_summary)
           }
        }
        message.update(body)
        return message

    def generate_separate_entity_jsons(self):
        summary_dict = self._get_result_summary_dict(self.result_summary)

        if self.pattern == "node":
            for node in self.nodes:
                message = super().format_json_header()
                body = {
                        "body": {
                            "queryName": self.query_name,
                            "jobRequest": self.job_request,
                            "nodes": [self._get_node_dict(node)],
                            "relationship": {},
                            "resultSummary": summary_dict
                        }
                }
                message.update(body)
                yield message
        elif self.pattern == "relationship":
            for relationship in self.relationships:
                message = super().format_json_header()
                body = {
                        "body": {
                            "queryName": self.query_name,
                            "jobRequest": self.job_request,
                            "nodes": [],
                            "relationship": self._get_relationship_dict(relationship),
                            "resultSummary": summary_dict
                        }
                }
                message.update(body)
                yield message
        else:
            raise ValueError(f"Pattern '{self.pattern}' not in supported patterns: {self.supported_patterns}.")

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


class JobCreatedWriter(MessageWriter):
    def __init__(
                 self,
                 *,
                 sender,
                 seed_id,
                 previous_event_id,
                 job_dict):

        self.message_kind = 'jobCreated'

        super().__init__(
                         sender,
                         seed_id,
                         previous_event_id)

        self.job_dict = job_dict

    def format_json_message(self):
        message = super().format_json_header()

        body = {
           "body": {
                    "jobDict": self.job_dict
            }
        }
        message.update(body)
        return message


class MessageReader(object):

    def __init__(
                 self, 
                 context, 
                 event):

        self.context = context

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)

        # Not sure what cases caused me to add this to db-query,
        # but keeping it in because I don't see any harm
        if type(data) == str:
            logging.warn("#> Message data not correctly loaded as JSON. " +
                         "Used eval to convert from string.")
            data = eval(data)

        seed_id = int(data['header'].get('seedId'))
        previous_event_id = int(data['header'].get('previousEventId'))
        if not previous_event_id:
            previous_event_id = seed_id

        self.context = context
        self.message_kind = data['header']['messageKind']
        self.header = data['header']
        self.body = data['body']
        self.sender = data['header']['sender']
        self.event_id = context.event_id
        self.seed_id = seed_id
        self.previous_event_id = previous_event_id


class QueryRequestReader(MessageReader):

    def __init__(
                 self,
                 context,
                 event):

        super().__init__(context, event)

        if self.message_kind != 'queryRequest':
            return ValueError

        self.query_name = self.body['queryName']
        self.query_parameters = self.body['queryParameters']
        self.custom = bool(self.body['custom'])

        # Should only be populated for custom queries
        if self.custom:
            self.cypher = self.body['cypher']
            self.write_transaction = bool(self.body['writeTransaction'])
            self.aggregate_results = bool(self.body['aggregateResults'])
            self.publish_to = self.body.get('publishTo')
            self.returns = self.body.get('returns')


class QueryResponseReader(MessageReader):

    def __init__(
                 self,
                 context,
                 event):

        super().__init__(context, event)
        
        if self.message_kind != 'queryResponse':
            return ValueError

        self.query_name = self.body['queryName']
        self.result_summary = self.body['resultSummary']
        self.nodes = self.body['nodes']
        self.relationship = self.body['relationship']
        self.job_request = self.body['jobRequest']


class JobCreatedReader(MessageReader):
    def __init__(
                 self,
                 context,
                 event):

        super().__init__(context, event)

        if self.message_kind != 'jobCreated':
            return ValueError("Message is not of type 'jobCreated'.")

        self.job_dict = self.body['jobDict']


"""
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
"""