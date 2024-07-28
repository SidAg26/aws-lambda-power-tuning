import boto3
import re
import os
import time
import json

def lambda_handler(event, context):
    lambda_arn = event['lambdaARN']
    
    # TODO: Fetch the last streamed log request id
    start_time = os.environ.get('startTime', int(time.time() * 1000) - 900000)
    start_time = int(start_time) if isinstance(start_time, str) else start_time

    end_time = os.environ.get('endTime', int(time.time() * 1000))
    end_time = int(end_time) if isinstance(end_time, str) else end_time


    # Create a CloudWatch Logs client
    client = cloudwatch_client_from_arn(lambda_arn)
    
    # Get the list of log events from Lambda Insights
    parsed_events, last_event_time = extract_lambda_insights(client, lambda_arn, end_time, start_time)
    
    _dynamo_table = boto3.resource('dynamodb').Table('function_logs')
    process_logs_in_batch(parsed_events, _dynamo_table, 20)

    return "Successfully processed logs"

def cloudwatch_client_from_arn(lambda_arn):
    region = lambda_arn.split(":")[3]
    return boto3.client('logs', region_name=region)

def extract_lambda_insights(client, lambda_arn, end_time, start_time):
    # Define the log group and log stream names
    log_group_name = '/aws/lambda-insights'
    last_event_time = [0]

    processed_events = {}

    log_stream_name = []
    log_streams = []
    next_token = None
    while True:
        if next_token:
            response = client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LastEventTime',
            descending=True,
            nextToken=next_token
            )
        else:
            response = client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LastEventTime',
            descending=True
            )

        log_streams.extend(response['logStreams'])
        next_token = response.get('nextToken')
        if not next_token:
            break
    for log_stream in log_streams:
        # print("first condition: ", log_stream['firstEventTimestamp'] >= start_time)
        if log_stream['logStreamName'].startswith(f'{lambda_arn.split(":")[-1]}/') and \
            log_stream['firstEventTimestamp'] >= start_time:
            log_stream_name.append(log_stream['logStreamName'])
            last_event_time.append(log_stream['lastIngestionTime'])    
            # print(log_stream)
            
    # print(log_stream_name)
    for lg in log_stream_name:
    # Get the log events from the log group and log stream
        response = client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=lg,
            startTime=start_time
        )

        # Process the log events
        for event in response['events']:
            # Parse the JSON message
            try:
                log_message = json.loads(event['message'])
                request_id = log_message['request_id']

                processed_events[request_id] = {
                    'cpu_user_time': log_message['cpu_user_time'],
                    'cpu_system_time': log_message['cpu_system_time'],
                    'insights_duration': log_message['duration'],
                    'memory_utilisation': log_message['memory_utilization'],
                    'billed_duration': log_message['billed_duration'],
                    'billed_mb_ms': log_message['billed_mb_ms'],
                    'function_name': log_message['function_name'],
                    'cold_start': log_message['cold_start'],
                    'insight_init_duration': log_message['init_duration'] if 'init_duration' in log_message else 0,
                    'used_memory_max': log_message['used_memory_max'],
                    'total_memory': log_message['total_memory'],
                    'total_network': log_message['total_network'],
                    'tmp_free': log_message['tmp_free'],
                    'tmp_used': log_message['tmp_used'],
                    'tx_bytes': log_message['tx_bytes'],
                    'fd_max': log_message['fd_max'],
                    'rx_bytes': log_message['rx_bytes'],
                    'request_id': log_message['request_id'],
                    'agent_memory_avg': log_message['agent_memory_avg'],
                    'threads_max': log_message['threads_max'],
                    'tmp_max': log_message['tmp_max'],
                    'agent_memory_max': log_message['agent_memory_max'],
                    'fd_use': log_message['fd_use'],
                    'version': log_message['version'],
                    'shutdown': log_message['shutdown'] if 'shutdown' in log_message else 0,
                    'shutdown_reason': log_message['shutdown_reason'] if 'shutdown_reason' in log_message else 0
                }
            except Exception as e:
                print("Error parsing log message:", e)

    if log_stream_name is not []:
        last_event_time = [max(last_event_time)]
        # print(last_event_time)
    return processed_events, last_event_time

def process_logs_in_batch(log_events, table, batch_size):
    if not log_events:
        return
    batch = dict(list(log_events.items())[:batch_size])
    for request_id, log in batch.items():
        try:
            # Check if the log value is a dictionary
            if isinstance(log, dict):
                # Remove request_id from the log
                del log['request_id']
                # Initialize parts of the update expression
                update_expression_parts = []
                expression_attribute_values = {}
                expression_attribute_names = {}

                # Construct the update expression and attribute values
                for key, value in log.items():
                    column_name = key
                    placeholder = f':{column_name}'
                    update_expression_parts.append(f'#{column_name} = {placeholder}')
                    expression_attribute_values[placeholder] = value
                    expression_attribute_names[f'#{column_name}'] = key

                # Join the parts to form the complete update expression
                update_expression = 'SET ' + ', '.join(update_expression_parts)

                # Correctly pass the constructed ExpressionAttributeNames
                table.update_item(
                    Key={'request_id': request_id},
                    UpdateExpression=update_expression,
                    ExpressionAttributeNames=expression_attribute_names,
                    ExpressionAttributeValues=expression_attribute_values
                )
            else:
                print(f"Invalid log format for request_id: {request_id}")
        except Exception as e:
            print(f"Failed to update item in DynamoDB: {e}")
            raise e
    # Recursively process the remaining events
    remaining_events = dict(list(log_events.items())[batch_size:])
    if remaining_events:
        process_logs_in_batch(remaining_events, table, batch_size)