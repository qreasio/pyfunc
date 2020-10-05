import azure.functions as func
import json
import logging

WORKER_NAME_MAP = {
    'start_crawler': {
        'type': 'statemachine',
        'name': 'STATE_MACHINE_ARM'
    }
}

def main(msg: func.ServiceBusMessage):
    message = msg.get_body().decode('utf-8')
    body = json.loads(message)
    action = body['Message']
    params = body['MessageAttributes']
    worker = WORKER_NAME_MAP[action]
    worker_type = worker['type']
    worker_name = worker['name']
    input = {k: params[k]['Value'] for k in params.keys()}
    job_id = input['job_id']
    tenant_id = input['tenant_id']

    result = json.dumps({
        'message_id': msg.message_id,
        'body': msg.get_body().decode('utf-8'),
        'worker_type': worker_type,
        'worker_name': worker_name,
        'job_id': job_id,
        'tenant_id': tenant_id,
        'content_type': msg.content_type,
        'expiration_time': msg.expiration_time,
        'label': msg.label,
        'partition_key': msg.partition_key,
        'reply_to': msg.reply_to,
        'reply_to_session_id': msg.reply_to_session_id,
        'scheduled_enqueue_time': msg.scheduled_enqueue_time,
        'session_id': msg.session_id,
        'time_to_live': msg.time_to_live,
        'to': msg.to,
        'user_properties': msg.user_properties,
    })

    logging.info(result)