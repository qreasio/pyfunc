import os
import json
from azure.servicebus.control_client import ServiceBusService, Message, Topic, Rule, DEFAULT_RULE_NAME

service_namespace = os.environ.get('SERVICE_NAMESPACE')
shared_access_key_name = os.environ.get('SHARED_ACCESS_KEY_NAME')
shared_access_key_value = os.environ.get('SHARED_ACCESS_KEY_VALUE')


bus_service = ServiceBusService(
    service_namespace=service_namespace,
    shared_access_key_name=shared_access_key_name,
    shared_access_key_value=shared_access_key_value)

message_obj = {
    "Message": "start_crawler",
    "MessageAttributes": {
        "job_id": {
            "Value": 2
        },
        "tenant_id": {
            "Value": 3
        }
    }
}
topic_name = os.environ.get('EDGE_TOPIC_NAME')
msg = Message(json.dumps(message_obj),
                custom_properties={'messageposition': 0})
bus_service.send_topic_message(topic_name, msg)