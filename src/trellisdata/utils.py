import json
import pytz

from anytree import Node, RenderTree
from anytree.search import find

from datetime import datetime

class TaxonomyParser:
    """
    This class is a wrapper on a Tree class from the anytree library to hold
    different data hierarchies read from a JSON representation

    Source: https://towardsdatascience.com/represent-hierarchical-data-in-python-cd36ada5c71a
    """

    def __init__(self, level_prefix = "L"):
        self.prefix = level_prefix
        self.nodes = {}
        self.root_key = None

    def find_by_name(self, name) -> Node:
        """
        Retrieve a node by its unique identifier name
        """
        root = self.nodes[self.root_key]
        node = find(root, lambda node: node.name == name)
        return node
   
    def read_from_json(self, fname):
        """
        Read the taxonomy from a JSON file given as input
        """
        
        self.nodes = {}
        try:
            with open(fname, "r") as f:
                data = json.load(f)
                n_levels = len(list(data.keys()))

                # read the root node
                root = data[f"{self.prefix}0"][0]
                name = root["name"]
                _ = root.pop("name")
                
                self.nodes[name] = Node(name, **root)
                self.root_key = name

                # populate the tree
                for k in range(1, n_levels):
                    
                    key = f"{self.prefix}{k}"
                    nodes = data[key]

                    for n in nodes:
                        try:
                            assert "name" in n
                            name = n["name"]
                            _ = n.pop("name")
                            parent = n["parent"]
                            _ = n.pop("parent")
                            
                            self.nodes[name] = Node(
                                name,
                                parent=self.nodes[parent],
                                **n
                            )
                        except AssertionError:
                            print(f"Malformed node representation: {n}")
                        except KeyError:
                            print(f"Detected a dangling node: {n['name']}")

        except (FileNotFoundError, KeyError):
            raise Exception("Not existent or malformed input JSON file")

    def read_from_string(self, data_str):
        """
        Read the taxonomy from a JSON file given as input
        """
        
        self.nodes = {}
        try:
            data = json.loads(data_str)
            n_levels = len(list(data.keys()))

            # read the root node
            root = data[f"{self.prefix}0"][0]
            name = root["name"]
            _ = root.pop("name")
            
            self.nodes[name] = Node(name, **root)
            self.root_key = name

            # populate the tree
            for k in range(1, n_levels):
                
                key = f"{self.prefix}{k}"
                nodes = data[key]

                for n in nodes:
                    try:
                        assert "name" in n
                        name = n["name"]
                        _ = n.pop("name")
                        parent = n["parent"]
                        _ = n.pop("parent")
                        
                        self.nodes[name] = Node(
                            name,
                            parent=self.nodes[parent],
                            **n
                        )
                    except AssertionError:
                        print(f"Malformed node representation: {n}")
                    except KeyError:
                        print(f"Detected a dangling node: {n['name']}")

        except (KeyError):
            raise Exception("Malformed input JSON string")


class Struct:
    # https://stackoverflow.com/questions/6866600/how-to-parse-read-a-yaml-file-into-a-python-object
    def __init__(self, **entries):
        self.__dict__.update(entries)


def make_unique_task_id(nodes, datetime_stamp):
    # Create pretty-unique hash value based on input nodes
    # https://www.geeksforgeeks.org/ways-sort-list-dictionaries-values-python-using-lambda-function/
    sorted_nodes = sorted(nodes, key = lambda i: i['id'])
    nodes_str = json.dumps(sorted_nodes, sort_keys=True, ensure_ascii=True, default=str)
    nodes_hash = hashlib.sha256(nodes_str.encode('utf-8')).hexdigest()
    print(nodes_hash)
    trunc_nodes_hash = str(nodes_hash)[:8]
    task_id = f"{datetime_stamp}-{trunc_nodes_hash}"
    return(task_id, trunc_nodes_hash)

def publish_to_pubsub_topic(publisher, project_id,  topic, message):
    """Convert dictionary to JSON and publish to Pub/Sub topic.

    Args:
        publisher (pubsub.PublisherClient): Pub/Sub client
        project_id (str): Google Cloud Project ID
        topic (str): Pub/Sub topic name
        message (dict): Dictionary with header and body fields.

    Returns:
        result (???)
    """

    topic_path = publisher.topic_path(project_id, topic)
    # https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable/36142844#36142844
    json_message = json.dumps(message, indent=4, sort_keys=True, default=str).encode('utf-8')
    result = publisher.publish(topic_path, data=json_message).result()
    return result

def publish_str_to_topic(publisher, project_id, topic, str_data):
    topic_path = publisher.topic_path(project_id, topic)
    message = str_data.encode('utf-8')
    result = publisher.publish(topic_path, data=message).result()
    return result

def convert_timestamp_to_rfc_3339(timestamp):
    # Load time in RFC 3339 format
    # Description of RFC 3339: http://henry.precheur.org/python/rfc3339.html
    # Pub/Sub message example: https://cloud.google.com/functions/docs/writing/background#functions-writing-background-hello-pubsub-python
    try:
        rfc3339_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError as exception:
        try:
            rfc3339_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
        except:
            return ValueError
    except:
        return ValueError
    return rfc3339_time

def get_seconds_from_epoch(datetime_obj):
    """Get datetime as total seconds from epoch.

    Provides datetime in easily sortable format

    Args:
        datetime_obj (datetime): Datetime.
    Returns:
        (float): Seconds from epoch
    """
    from_epoch = datetime_obj - datetime(1970, 1, 1, tzinfo=pytz.UTC)
    from_epoch_seconds = from_epoch.total_seconds()
    return from_epoch_seconds