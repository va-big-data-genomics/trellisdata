import json
import yaml
import neo4j
import base64
import logging

class TriggerController:

    def __init__(self, trigger_document):
        """Manage database trigger activation and get query parameters.

        Inspired by docs: https://pyyaml.org/wiki/PyYAMLDocumentation

        args:
            trigger_document (str): YAML formatted str with tagged !DatabaseTrigger documents
        """
        triggers = yaml.load_all(trigger_document, Loader=yaml.FullLoader)

        self.node_triggers = {}
        self.relationship_triggers = {}
        self.supported_patterns = ["node", "relationship"]

        # Separate triggers by type
        for trigger in triggers:

            self._validate_trigger_content(trigger)

            if trigger.pattern == 'node':
                if trigger.start['label'] in self.node_triggers.keys():
                    self.node_triggers[trigger.start['label']].append(trigger)
                else:
                    self.node_triggers[trigger.start['label']] = [trigger]
            elif trigger.pattern == 'relationship':
                if trigger.start['label'] in self.relationship_triggers.keys():
                    self.relationship_triggers[trigger.start['label']].append(trigger)
                else:
                    self.relationship_triggers[trigger.start['label']] = [trigger]
            else:
                raise ValueError(f"{trigger.pattern} is not a supported trigger pattern.")

    def evaluate_trigger_conditions(self, query_response):
        """
        args:
            query_response (trellisdata.QueryResponseReader)
        """

        response_pattern = self._determine_result_pattern(query_response)

        if response_pattern == 'node':
            print("Evaluating node triggers.")
            activated_triggers = self._evaluate_node_triggers(query_response)
        elif response_pattern == 'relationship':
            print("Evaluating relationship triggers.")
            activated_triggers = self._evaluate_relationship_triggers(query_response)
        else:
            raise ValueError(f"Response pattern '{response_pattern}' is not a supported trigger pattern.")
        return activated_triggers

    def _determine_result_pattern(self, query_response):
        len_nodes = len(query_response.nodes)
        len_relationships = len(query_response.relationship)

        #if len_nodes == 1 and len_relationships == 0:
        if len_nodes == 1 and not query_response.relationship:
            return "node"
        elif len_nodes == 0 and query_response.relationship:
        #elif len_nodes == 2 and len_relationships == 1:
            return "relationship"
        else:
            raise ValueError(f"Number of nodes ({len_nodes}) and relationships ({len_relationships}) " +
                              "does not fit a recognized return pattern.")

    def _evaluate_node_triggers(self, query_response):
        """
        When evaluating node triggers, the only criterion is 
        whether the node label matches the trigger label.
        """
        node = query_response.nodes[0]
        node_labels = node['labels']

        # New logic to handle multiple labels
        relevant_triggers = []
        for label in node_labels:
            node_triggers = self.node_triggers.get(label)
            if node_triggers:
                relevant_triggers.extend(node_triggers)
        # Remove duplicates
        relevant_triggers = set(relevant_triggers)
        #relevant_triggers = self.node_triggers[node_label] # Old single-label code

        activated_triggers = []
        for trigger in relevant_triggers:
            node_properties = node['properties']

            query_parameters = self._get_node_trigger_parameters(
                                    trigger,
                                    node_properties)
            trigger_tuple = (trigger, query_parameters)
            activated_triggers.append(trigger_tuple)
        return activated_triggers

    def _evaluate_relationship_triggers(self, query_response):
        """
        When evaluating relationships triggers, the criteria are...
        """
        
        # Not relevant anymore; only (1) relationship can be passed and
        # presence of relationship is checked upstream.
        """
        if len(query_response.relationship) == 0:
            return None
        elif len(query_response.relationship) > 1:
            raise ValueError(
                "Multiple relationships provided, " +
                "only single relationship patterns supported.")
        """

        relationship = query_response.relationship
        start_labels = relationship['start_node']['labels']

        # Get relevant triggers
        #relevant_triggers = self.relationship_triggers[start_label]
        #candidate_triggers = []
        for label in start_labels:
            start_triggers = self.relationship_triggers.get(label)
            #if start_triggers:
            #    candidate_triggers.extend(start_triggers)
        candidate_triggers = set(start_triggers)

        trigger_names = [trigger.name for trigger in candidate_triggers]
        logging.info(f"Triggers that match start node '{start_labels}': {trigger_names}.")

        end_labels = relationship['end_node']['labels']
        relationship_type = relationship['type']

        activated_triggers = []
        for trigger in candidate_triggers:
            #if (start_label == trigger.start['label'] # This is redundant
            #        and end_label == trigger.end['label'] 
            #        and relationship_type == trigger.relationship['type']):
            if (
                trigger.relationship['type'] == relationship_type and
                trigger.end['label'] in end_labels):

                start_properties = relationship['start_node']['properties']
                end_properties = relationship['end_node']['properties']
                rel_properties = relationship['properties']

                # TODO: Get query parameters for relationship triggers
                parameters = self._get_relationship_trigger_parameters(
                    trigger,
                    start_properties,
                    end_properties,
                    rel_properties)
                trigger_tuple = (trigger, parameters)
                activated_triggers.append(trigger_tuple)
                logging.info(f"Trigger {trigger.name} activated.")
            else:
                logging.info(f"{trigger.name} not activated. " +
                    f"Triple relationship {relationship_type} does not match {trigger.relationship['type']} " +
                    f"or trigger label '{trigger.end['label']}' not in end node labels: {end_labels}.")
        return activated_triggers

    def _get_relationship_trigger_parameters(
                                             self,
                                             trigger,
                                             start_properties,
                                             end_properties,
                                             rel_properties):

        start_parameters = trigger.start.get('properties')
        end_parameters = trigger.end.get('properties')
        rel_parameters = trigger.relationship.get('properties')

        parameters = {}
        if start_parameters:
            parameters = self._get_parameter_values(start_parameters, start_properties, parameters, trigger.start['label'])
        if end_parameters:
            parameters = self._get_parameter_values(end_parameters, end_properties, parameters, trigger.end['label'])
        if rel_parameters:
            parameters = self._get_parameter_values(rel_parameters, rel_properites, parameters, trigger.relationship['type'])
        return parameters

    def _get_node_trigger_parameters(
                                     self,
                                     trigger,
                                     node_properties):
        node_parameters = trigger.start.get('properties')

        parameters = {}
        if node_parameters:
            parameters = self._get_parameter_values(
                                        node_parameters,
                                        node_properties,
                                        parameters,
                                        trigger.start['label'])
        return parameters

    def _get_parameter_values(self, parameter_mapping, properties, parameters, entity_label):
        # Parameters describe values that must be provided to the activated query.
        # Properties come from database response entities (node or relationship).
        for property_name, parameter_name in parameter_mapping.items():
            try:
                parameters[parameter_name] = properties[property_name]
            except KeyError:
                raise KeyError(f"{entity_label} is missing property {property_name}.")
        return parameters

    def _validate_trigger_content(self, trigger):
        if not hasattr(trigger, "name"):
            raise AttributeError(f"Trigger is not named.")

        logging.info(f"Validating trigger: {trigger.name}.")
        
        if not hasattr(trigger, "pattern"):
            raise AttributeError(f"Trigger is missing an activation pattern.")
    
        if not trigger.pattern in self.supported_patterns:
            raise ValueError(f"Trigger pattern '{trigger.pattern}' not in supported patterns: {self.supported_patterns}.")
        
        try:
            label = trigger.start['label']
            if not label:
                raise ValueError(f"Trigger start node missing label.")
        except:
            raise ValueError(f"Trigger start node missing label.")

        if not hasattr(trigger, "query"):
            raise AttributeError(f"Trigger is missing query.")

        if hasattr(trigger, "relationship") or hasattr(trigger, "end"):
            try:
                rel_type = trigger.relationship["type"]
                if not rel_type:
                    raise ValueError(f"Trigger relationship type missing.")
            except:
                raise ValueError(f"Trigger relationship type missing.")

            try:
                label = trigger.end["label"]
                if not label:
                    raise ValueError(f"Trigger end node missing label.")
            except:
                raise ValueError(f"Trigger end node missing label.")


class DatabaseTrigger(yaml.YAMLObject):
    """
    Inspired by docs: https://pyyaml.org/wiki/PyYAMLDocumentation
    """
    yaml_tag = u'!DatabaseTrigger'

    def __init__(
                 self, 
                 pattern, 
                 start, 
                 query, 
                 end = 0, 
                 relationship = 0):
        self.pattern = pattern
        self.start = start
        self.query = query

        self.end = end
        self.relationship = relationship