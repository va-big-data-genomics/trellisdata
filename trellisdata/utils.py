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
    result = PUBLISHER.publish(topic_path, data=json_message).result()
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