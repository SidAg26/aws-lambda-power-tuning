import boto3
import re
import time
import json

def lambda_handler(event, context):
    lambda_arn = event['lambdaARN']
    last_event_time = None
    # TODO: Fetch the last streamed log request id
    end_time = int(time.time() * 1000)  # Current time in milliseconds
    # start_time = end_time - 900000  # 15 minutes ago in milliseconds
    start_time = event['startTime'] if 'startTime' in event else end_time - 900000
    # Create a CloudWatch Logs client
    client = cloudwatch_client_from_arn(lambda_arn)


    # Get the list of log groups
    log_groups = client.describe_log_groups()
    log_group_names = [group['logGroupName'] for group 
                       in log_groups['logGroups'] 
                       if group['logGroupName'].endswith('/aws/lambda/' \
                                                         + lambda_arn.split(':')[-1])]
    
    # Get the list of log streams
    log_streams = []
    for log_group_name in log_group_names:
        log_streams += client.describe_log_streams(logGroupName=log_group_name,
                                                   orderBy='LastEventTime',
                                                   descending=True)['logStreams']


    # Get the list of log events
    log_events = []
    for log_stream in log_streams:
        response = client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream['logStreamName'],
            startTime=start_time,
            endTime=end_time
        )
        log_events += response['events']
    
    # Parse the log events
    parsed_events = {}
    for log in log_events:
        try:
            match = re.search(r'^REPORT RequestId:', log['message'])
            if match is not None:
                request_id = extract_request_id(log['message'])
                parsed_events[request_id] = {
                    'duration': extract_duration(log['message']),
                    'init_duration': extract_init_duration(log['message']),
                    'memory_size': extract_memory_size(log['message']),
                    'memory_used': extract_memory_used(log['message'])
                }
        except:
            continue

    # Get the list of log events from Lambda Insights
    parsed_events, last_event_time = extract_lambda_insights(client, lambda_arn, 
                                                             parsed_events, end_time, start_time)
    # parsed_events += lambda_insights
    print(parsed_events)
    return parsed_events

def cloudwatch_client_from_arn(lambda_arn):
    region = lambda_arn.split(":")[3]
    return boto3.client('logs', region_name=region)

def extract_request_id(log):
    regex = r'^REPORT RequestId:\s+([a-f0-9-]+)'
    match = re.search(regex, log)
    if match:
        return match.group(1)
    return None

def extract_duration(log):
    regex = r'Billed Duration: (\d+) ms'
    match = re.search(regex, log)
    if match:
        return int(match.group(1))
    return None

def extract_init_duration(log):
    regex = r'Init Duration: (\d+)'
    match = re.search(regex, log)
    if match:
        return int(match.group(1))
    return None

def extract_memory_size(log):
    regex = r'Memory Size: (\d+) MB'
    match = re.search(regex, log)
    if match:
        return int(match.group(1))
    return None

def extract_memory_used(log):
    regex = r'Memory Used: (\d+) MB'
    match = re.search(regex, log)
    if match:
        return int(match.group(1))
    return None

def extract_lambda_insights(client, lambda_arn, parsed_events, end_time, start_time):
    # Define the log group and log stream names
    log_group_name = '/aws/lambda-insights'
    last_event_time = [0]
    # Define start and end times for the logs (example: last 24 hours)
    # end_time = int(time.time() * 1000)  # Current time in milliseconds
    # start_time = end_time - 86400000  # 24 hours ago in milliseconds
    processed_events = parsed_events

    log_stream_name = []
    while len(log_stream_name) == 0:
        response = client.describe_log_streams(
        logGroupName=log_group_name,
        orderBy='LastEventTime',
        descending=True
        )

        log_streams = response['logStreams']
        
        for log_stream in log_streams:
            if log_stream['logStreamName'].startswith(f'{lambda_arn.split(":")[-1]}/') and \
                end_time >= log_stream['lastIngestionTime'] >= start_time:
                log_stream_name.append(log_stream['logStreamName'])
                last_event_time.append(log_stream['lastIngestionTime'])            
        time.sleep(10)


    print(log_stream_name)
    for lg in log_stream_name:
    # Get the log events from the log group and log stream
        response = client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=lg,
            startTime=start_time,
            endTime=end_time
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
                    'duration': log_message['duration'],
                    'memory_utilisation': log_message['memory_utilization'],
                    'billed_duration': log_message['billed_duration'],
                    'billed_mb_ms': log_message['billed_mb_ms'],
                    'function_name': log_message['function_name'],
                    'cold_start': log_message['cold_start'],
                    'init_duration': log_message['init_duration'] if 'init_duration' in log_message else 0,
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
                    'version': log_message['version']
                }
            except Exception as e:
                print("Error parsing log message:", e)

    if log_stream_name is not []:
        last_event_time = [max(last_event_time)]
        # print(last_event_time)
    
    return processed_events, last_event_time