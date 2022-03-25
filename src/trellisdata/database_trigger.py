import json
import yaml
import neo4j
import base64

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

        # Separate triggers by type
        for trigger in triggers:
            if trigger.type == 'node':
                if trigger.start['label'] in self.node_triggers.keys():
                    self.node_triggers[trigger.start['label']].append(trigger)
                else:
                    self.node_triggers[trigger.start['label']] = [trigger]
            elif trigger.type == 'relationship':
                if trigger.start['label'] in self.relationship_triggers.keys():
                    self.relationship_triggers[trigger.start['label']].append(trigger)
                else:
                    self.relationship_triggers[trigger.start['label']] = [trigger]
            else:
                return ValueError(f"{trigger.type} is not a supported trigger type.")

    def evaluate_trigger_conditions(self, query_response):
        """

        args:
            query_response (trellisdata.QueryResponseReader)
        """

        trigger_type = self._determine_trigger_type(query_response)

        if trigger_type == 'node':
            activated_triggers = self._evaluate_node_triggers(query_response)
        elif trigger_type == 'relationship':
            activated_triggers = self._evaluate_relationship_triggers(query_response)
        else:
            return ValueError()
        return activated_triggers

    def _determine_trigger_type(self, query_response):
        len_nodes = len(query_response.nodes)
        len_relationships = len(query_response.relationships)

        if len_nodes == 1 and len_relationships == 0:
            return "node"
        elif len_nodes == 2 and len_relationships == 1:
            return "relationship"
        else:
            return ValueError()

    def _evaluate_node_triggers(self, query_response):
        node = query_response.nodes[0]
        node_label = node['labels']

        relevant_triggers = self.node_triggers[node_label]

        activated_triggers = []
        for trigger in relevant_triggers:
            node_properties = node['properties']

            query_parameters = _get_node_trigger_parameters(
                                                            trigger,
                                                            node_properties)
            trigger_tuple = (trigger, query_parameters)
            activated_triggers = append(trigger_tuple)
        return activated_triggers

    def _evaluate_relationship_triggers(self, query_response):
        relationship = query_response.relationships[0]

        start_labels = relationship['start_node']['labels']
        if len(start_labels) > 1:
            # TODO: raise warning
            pass
        start_label = start_labels[0]

        # Get relevant triggers
        relevant_triggers = self.relationship_triggers[start_label]

        end_labels = relationship['end_node']['labels']
        if len(end_labels) > 1:
            # TODO: raise warning
            pass
        end_label = end_labels[0]

        relationship_type = relationship['type']

        activated_triggers = []
        for trigger in relevant_triggers:
            if (start_label == trigger.start['label'] 
                    and end_label == trigger.end['label'] 
                    and relationship_type == trigger.relationship['type']):

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
            parameters = self._get_parameter_values(start_parameters, start_properties, parameters)
        if end_parameters:
            parameters = self._get_parameter_values(end_parameters, end_properties, parameters)
        if rel_parameters:
            parameters = self._get_parameter_values(rel_parameters, rel_properites, parameters)
        return parameters

    def _get_node_trigger_parameters(
                                     self,
                                     trigger,
                                     node_properties):
        node_parameters = trigger.start.get('properties')

        parameters = {}
        if start_parameters:
            parameters = self._get_parameter_values(start_parameters, start_properties, parameters)
        return parameters

    def _get_parameter_values(self, parameter_mapping, properties, parameters):
        for property_name, parameter_name in parameter_mapping.items():
            parameters[parameter_name] = properties[property_name]
        return parameters


class DatabaseTrigger(yaml.YAMLObject):
    """
    Inspired by docs: https://pyyaml.org/wiki/PyYAMLDocumentation
    """
    yaml_tag = u'!DatabaseTrigger'

    def __init__(
                 self, 
                 trigger_type, 
                 start, 
                 query, 
                 end=None, 
                 relationship=None):
        self.trigger_type = trigger_type
        self.start = start
        self.query = query

        self.end = end
        self.relationship = relationship