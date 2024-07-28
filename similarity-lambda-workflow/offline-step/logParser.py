import boto3
import re
import time

def lambda_handler(event, context):
    lambda_arn = event['lambdaARN']
    last_event_time = None
    insight_function_name = 'getLogInsights' # ENTER THE LogInsight FUNCTION NAME
    executor_function_name = 'executeFunctions' # ENTER THE EXECUTOR FUNCTION NAME

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
            match = r'Error|'\
                    r'Exception|'\
                    r'error'
            match = re.search(match, log['message'])
            if match is not None:
                request_id = extract_request_id(log['message'])
                parsed_events[request_id] = {
                    'function_error': extract_function_error(log['message'])
                }
        except:
            continue
    
    # Get the payload value from the executor lambda function
    filter_pattern = 'PAYLOAD'
    # Retrieve log events based on the filter pattern
    response = client.filter_log_events(
        logGroupName=f'/aws/lambda/{executor_function_name}', # ENTER THE EXECUTOR FUNCTION LOG GROUP
        startTime=start_time,
        endTime=end_time,
        filterPattern=filter_pattern
    )

    # Parse the log events
    for event in response['events']:
        request_id = extract_request_id_from_payload(event['message'])
        if request_id is not None:
            parsed_events[request_id] = {
                'payload': extract_payload_value(event['message'])
            }
    # print(parsed_events)
    _dynamodb = dynamodb_client_from_arn(lambda_arn)
    _table = _dynamodb.Table('function_logs') # ENTER THE DYNAMODB TABLE NAME
    process_logs_in_batch(parsed_events, _table, 10)

    # Update the environment variables
    set_enviroment_variables(start_time, end_time, lambda_arn, insight_function_name)
    return end_time

def set_enviroment_variables(start_time, end_time, lambda_arn, function_name):
    _client = boto3.client('lambda', region_name=lambda_arn.split(":")[3])
    new_env = { 
        'startTime': str(start_time),
        'endTime': str(end_time)
    }
    try:
        response = _client.update_function_configuration(
            FunctionName=function_name,
            Environment={
                'Variables': new_env
            }
        )
    except Exception as e:
        print(f"Failed to update environment variables: {e}")
        raise e

    


def process_logs_in_batch(log_events, table, batch_size):
    if not log_events:
        return
    batch = dict(list(log_events.items())[:batch_size])
    try:
        with table.batch_writer() as batch_writer:
            for request_id, log in batch.items():
                batch_writer.put_item(
                    Item={
                        'request_id': request_id,
                        **log
                    }
                )
    except Exception as e:
        print(f"Failed to write to DynamoDB: {e}")
        raise e    
    # Recursively process the remaining events
    remaining_events = dict(list(log_events.items())[batch_size:])
    if remaining_events:
        process_logs_in_batch(remaining_events, table, batch_size)


def cloudwatch_client_from_arn(lambda_arn):
    region = lambda_arn.split(":")[3]
    return boto3.client('logs', region_name=region)

def dynamodb_client_from_arn(lambda_arn):
    region = lambda_arn.split(":")[3]
    return boto3.resource('dynamodb', region_name=region)

def extract_function_error(log):
    regex = r'Error: (?P<Error>.*)|'\
            r'Exception: (?P<Exception>.*)|'\
            r'error: (?P<error>.*)'
    match = re.search(regex, log)
    if match.group('Error'):
        return match.group('Error')
    elif match.group('Exception'):
        return match.group('Exception')
    elif match.group('error'):
        return match.group('error')
    return None

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

def extract_payload_value(log):
    regex = r'Input:\t(.*)\n'
    match = re.search(regex, log)
    if match:
        return match.group(1)
    return None

def extract_request_id_from_payload(log):
    regex = r'RequestId:\t([a-f0-9-]+)\t'
    match = re.search(regex, log)
    if match:
        return match.group(1)
    return None