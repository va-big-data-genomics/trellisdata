import json
import neo4j
import base64

class DatabaseTrigger:
    """ A Trellis-formatted database query request.

    Use this class to generate query requests to be delivered to the Trellis
    'db-query' function.

    Attributes:
        name (str): Name of the trigger and corresponding database query. This name should
            be used to look up the query in the query config file.
        required_node_labels (list): These labels must be a subset of the node labels
            to activate trigger.
        recipient_topic (str): Name of the Trellis configuration key corresponding
            to the message broker topic the trigger message should be delivered to.
            In this most cases this will be the database function ('DB_QUERY_TOPIC').
        function_name (str): Name of the serverless function that is checking the 
            triggers.
        env_vars (dict): Trellis and computing environment configuration values.
        required_header_labels (list): Required labels from the incoming message header
            for activating the trigger.
        required_node_properties (dict): Node key:value properties that are required
            for activation. If the value is None only the presence of the property will
            be checked.
        required_body_properties (dict): Key:value properties that must be present in
            the incoming message body to activate trigger. If the value is None only 
            the presence of the property will be checked.
        required_node_property_types (dict): Specifies the type of value (str, int, etc.)
            that is required for node key:value properties.
        required_trellis_properties (dict): Key:value properties that must be present in
            the Trellis configuration provided in the env_vars.
        banned_node_labels (list): If a node has any of these labels it will not
            activate the trigger.
        results_mode (str): Determine whether to return query results or statistics. 
            Supported values : ["data", "stats"].
        node_required (bool): Indicates whether node metadata must be provided to activate
            the trigger. Node metadata is not required if trigger activation is being
            requested (e.g. Cloud Scheduler) rather than being event-driven.
        publish_results_to (str): Specify the message broker topic that the results of the 
            database query should be sent to.
        split_results (bool): Indicate whether to deliver all query results in a single message
            or split them into separate messages.
        write_transaction (bool): Determine whether to use the read or write Neo4j driver method.
        query_variables (dict): Dictionary where the key is the database trigger query
            variable name and the value is the key for the node property that will be passed
            as the variable value.
    """

    def __init__(
                 self, 
                 name,
                 required_node_labels,
                 recipient_topic,
                 function_name,
                 env_vars,
                 required_header_labels = [],
                 required_node_properties = {},
                 required_body_properties = {},
                 required_node_property_types = {},
                 required_trellis_properties = {},
                 banned_node_labels = [],
                 results_mode = "data",
                 node_required = True,
                 publish_results_to = None,
                 split_results = True,
                 write_transaction = False,
                 query_variables = {}):

        self.name = name
        self.required_node_labels = required_node_labels
        self.results_mode = results_mode
        self.recipient_topic = recipient_topic
        self.function_name = function_name
        self.env_vars = env_vars

        self.required_header_labels = required_header_labels
        self.required_node_properties = required_node_properties
        self.required_body_properties = required_body_properties
        self.required_node_property_types = required_node_property_types
        self.required_trellis_properties = required_trellis_properties
        self.banned_node_labels = banned_node_labels

        self.node_required = node_required
        self.publish_results_to = publish_results_to
        self.split_results = split_results
        self.write_transaction = write_transaction
        self.query_variables = query_variables

    def check_header_labels(self, header):
        # Check label conditions
        label_conditions = [
            set(self.required_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in label_conditions:
            if condition:
                continue
            else:
                return False
        return True

    def check_node_labels(self, node):
        # Check label conditions
        label_conditions = [
            # Check that node matches metadata criteria:
            set(self.required_node_labels).issubset(set(node.get('labels'))),
        ]

        for condition in label_conditions:
            if condition:
                continue
            else:
                return False
        return True

    def check_node_properties(self, node):
        # Check presence and and values of node properties
        for key, value in self.required_node_properties.items():
            if value:
                if node.get(key) == value:
                    continue
                else:
                    return False
            else:
                if node.get(key):
                    continue
                else:
                    return False
        return True

    def check_banned_node_labels(self, node):
        for banned_label in self.banned_node_labels:
            if banned_label in node['labels']:
                return False
        return True

    def check_node_property_instance_types(self, node):
        # Check node property instance types
        for property_name, property_type in self.required_node_property_types.items():
            if isinstance(node.get(property_name), property_type):
                continue
            else:
                return False
        return True

    def check_body_properties(self, body):
        # Check presence and and values of message body properties
        for key, value in self.required_body_properties.items():
            if value:
                if body.get(key) == value:
                    continue
                else:
                    return False
            else:
                if body.get(key):
                    continue
                else:
                    return False
        return True

    def check_trellis_configuration(self, env_vars):
        for key, value in self.required_trellis_properties.items():
            if env_vars.get(key) == value:
                continue
            else:
                return False
        return True

    def check_all_conditions(self, header, body, node):

        results = []
        results.append(self.check_header_labels(header))
        results.append(self.check_node_labels(node))
        results.append(self.check_node_properties(node))
        results.append(self.check_node_property_instance_types(node))
        results.append(self.check_body_properties(body))
        results.append(self.check_banned_node_labels(node))
        if False in results:
            return False
        else:
            return True

    def format_json_messages(self, header, body, node, context):

        query_parameters = _get_query_parameters()

        # Instead of directly creating the message, I should provide
        # the inputs to the TrellisMessage class and create it 
        # from there
        message = {
           "header": {
                      "messageKind": "queryRequest",
                      "sender": self.function_name,
                      "seedId": header["seedId"],
                      "previousEventId": context.event_id,
           },
           "body": {
                    # Trigger name should be named the same as the database query it trigger
                    "triggerName": self.name,
                    "queryParameters": query_parameters,
                    "writeTransaction": self.write_transaction,
                    "resultsMode": self.results_mode,
                    "splitResults": self.split_results,
                    "publishResultsTo": self.publish_results_to
           }
        }
        return([(self.recipient_topic, message)])

    def require_node_has_property(self, key, value=None):
        self.node_required_property[key] = value

    def require_body_has_property(self, key, value=None):
        self.body_required_property[key] = value

    def require_node_property_type(self, key, value):
        # Require value of a key-value pair to be an instance of a particular type
        self.node_required_property_type[key] = value

    def require_trellis_config(self, key, value):
        # Get keys or key-value pairs from the Trellis config file
        self.trellis_required_property[key] = value

    def do_not_allow_node_labels(self, labels):
        ## TODO: Check that node of the banned labels are in the required labels
        self.banned_node_labels.extend(labels)

    def _get_query_parameters(self):
        query_parameters = {}
        for variable_name, node_key in self.query_variables.items():
            query_parameters[variable_name] = node[node_key]
        return(query_parameters)