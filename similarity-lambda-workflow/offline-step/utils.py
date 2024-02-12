import os
import json
import boto3
import time
import math
import re
import base64
import numpy as np
from botocore.exceptions import NoCredentialsError
from urllib.parse import urlparse, urlencode

# AWS Lambda client
lambda_client = boto3.client('lambda')

# S3 client
s3_client = boto3.client('s3')

def step_functions_cost(n_power):
    return round(step_functions_base_cost() * (6 + n_power), 5)

def step_functions_base_cost():
    prices = json.loads(os.environ['sfCosts'])
    return base_cost_for_region(prices, os.environ['AWS_REGION'])

def lambda_base_cost(region, architecture):
    prices = json.loads(os.environ['baseCosts'])
    price_map = prices[architecture]
    if not price_map:
        raise ValueError('Unsupported architecture: ' + architecture)
    return base_cost_for_region(price_map, region)

def all_power_values():
    increment = 64
    power_values = []
    for value in range(128, 3009, increment):
        power_values.append(value)
    return power_values

def get_lambda_alias(lambda_arn, alias):
    print('Checking alias ', alias)
    params = {
        'FunctionName': lambda_arn,
        'Name': alias,
    }
    try:
        response = lambda_client.get_alias(**params)
        return response
    except lambda_client.exceptions.ResourceNotFoundException:
        return None

def verify_alias(lambda_arn, alias):
    try:
        lambda_client.get_alias(FunctionName=lambda_arn, Name=alias)
        return True
    except lambda_client.exceptions.ResourceNotFoundException:
        print('OK, even if missing alias')
        return False
    except Exception as error:
        print('Error during alias check:')
        raise error

def create_power_configuration(lambda_arn, value, alias):
    try:
        set_lambda_power(lambda_arn, value)
        wait_for_function_update(lambda_arn)
        response = lambda_client.publish_version(FunctionName=lambda_arn)
        version = response['Version']
        alias_exists = verify_alias(lambda_arn, alias)
        if alias_exists:
            update_lambda_alias(lambda_arn, alias, version)
        else:
            create_lambda_alias(lambda_arn, alias, version)
    except Exception as error:
        if 'Alias already exists' in str(error):
            print('OK, even if: ', error)
        else:
            print('Error during config creation for value ' + str(value))
            raise error

def lambda_client_from_arn(lambda_arn):
    region = lambda_arn.split(":")[3]
    return boto3.client('lambda', region_name=region)

def wait_for_function_update(lambda_arn):
    print('Waiting for update to complete')
    while True:
        lambda_client = lambda_client_from_arn(lambda_arn)
        response = lambda_client.get_function(FunctionName=lambda_arn)
        if response['Configuration']['LastUpdateStatus'] == 'Successful':
            break
        time.sleep(1)

def wait_for_alias_active(lambda_arn, alias):
    print(f'Waiting for alias {alias} to be active')
    while True:
        lambda_client = lambda_client_from_arn(lambda_arn)
        response = lambda_client.get_alias(FunctionName=lambda_arn, Name=alias)
        if response['AliasArn']:
            break
        time.sleep(10 * 90)

def get_lambda_power(lambda_arn):
    print('Getting current power value')
    lambda_client = lambda_client_from_arn(lambda_arn)
    response = lambda_client.get_function(FunctionName=lambda_arn, Qualifier='$LATEST')
    return response['Configuration']['MemorySize']

def get_lambda_config(lambda_arn, alias):
    print(f'Getting current function config for alias {alias}')
    lambda_client = lambda_client_from_arn(lambda_arn)
    response = lambda_client.get_function(FunctionName=lambda_arn, Qualifier=alias)
    return response['Configuration']

# def get_lambda_config(lambda_arn, alias):
#     print(f'Getting current function config for alias {alias}')
#     lambda_client = lambda_client_from_arn(lambda_arn)
#     response = lambda_client.get_function_configuration(FunctionName=lambda_arn, Qualifier=alias)
#     architecture = response.get('Architectures', ['x86_64'])[0]
#     is_pending = response.get('State', '') == 'Pending'
#     return {'architecture': architecture, 'is_pending': is_pending}

def set_lambda_power(lambda_arn, value):
    print(f'Setting power to {value}')
    lambda_client = lambda_client_from_arn(lambda_arn)
    lambda_client.update_function_configuration(FunctionName=lambda_arn, MemorySize=int(value))

def publish_lambda_version(lambda_arn):
    print('Publishing new version')
    lambda_client = lambda_client_from_arn(lambda_arn)
    response = lambda_client.publish_version(FunctionName=lambda_arn)
    return response['Version']


def delete_lambda_version(lambda_arn, version):
    print('Deleting version ', version)
    lambda_client = lambda_client_from_arn(lambda_arn)
    lambda_client.delete_function(FunctionName=lambda_arn, Qualifier=version)

def create_lambda_alias(lambda_arn, alias, version):
    print('Creating Alias ', alias)
    lambda_client = lambda_client_from_arn(lambda_arn)
    lambda_client.create_alias(FunctionName=lambda_arn, FunctionVersion=version, Name=alias)

def update_lambda_alias(lambda_arn, alias, version):
    print('Updating Alias ', alias)
    lambda_client = lambda_client_from_arn(lambda_arn)
    lambda_client.update_alias(FunctionName=lambda_arn, FunctionVersion=version, Name=alias)

def delete_lambda_alias(lambda_arn, alias):
    print('Deleting alias ', alias)
    lambda_client = lambda_client_from_arn(lambda_arn)
    lambda_client.delete_alias(FunctionName=lambda_arn, Name=alias)

async def invoke_lambda_processor(processor_arn, payload, pre_or_post='Pre', disable_payload_logs=False):
    processor_data = await invoke_lambda(processor_arn, None, payload, disable_payload_logs)
    if 'FunctionError' in processor_data:
        error_message = f"{pre_or_post}Processor {processor_arn} failed with error {processor_data['Payload']}"
        if not disable_payload_logs:
            error_message += f" and payload {json.dumps(payload)}"
        raise Exception(error_message)
    return processor_data['Payload']

async def invoke_lambda_with_processors(lambda_arn, alias, payload, pre_arn, post_arn, disable_payload_logs):
    actual_payload = payload  # might change based on pre-processor

    # first invoke pre-processor, if provided
    if pre_arn:
        print('Invoking pre-processor')
        # overwrite payload with pre-processor's output (only if not empty)
        pre_processor_output = await invoke_lambda_processor(pre_arn, payload, 'Pre', disable_payload_logs)
        if pre_processor_output:
            actual_payload = pre_processor_output

    # invoke function to be power-tuned
    invocation_results = await invoke_lambda(lambda_arn, alias, actual_payload, disable_payload_logs)

    # then invoke post-processor, if provided
    if post_arn:
        print('Invoking post-processor')
        # note: invocation may have failed (invocation_results.FunctionError)
        await invoke_lambda_processor(post_arn, invocation_results['Payload'], 'Post', disable_payload_logs)

    return {
        'actualPayload': actual_payload,
        'invocationResults': invocation_results,
    }


def invoke_lambda(lambda_arn, alias, payload, disable_payload_logs):
    print('Invoking function')
    lambda_client = lambda_client_from_arn(lambda_arn)
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType='RequestResponse',
        LogType='Tail' if not disable_payload_logs else 'None',
        Payload=json.dumps(payload),
        Qualifier=alias
    )
    return {
        'StatusCode': response['StatusCode'],
        'FunctionError': response.get('FunctionError'),
        'LogResult': response.get('LogResult'),
        'Payload': response['Payload'].read().decode('utf-8')
    }

# Compute total cost
def compute_total_cost(min_cost, min_ram, value, durations):
    if not durations:
        return 0

    # compute corresponding cost for each duration
    costs = [compute_price(min_cost, min_ram, value, duration) for duration in durations]

    # sum all together
    return sum(costs)

# Compute average duration
def compute_average_duration(durations, discard_top_bottom):
    if not durations:
        return 0

    # a percentage of durations will be discarded (trimmed mean)
    to_be_discarded = int(len(durations) * discard_top_bottom)

    if discard_top_bottom > 0 and to_be_discarded == 0:
        # not an error, but worth logging
        # this happens when you have less than 5 invocations
        # (only happens if dryrun or in tests)
        print('not enough results to discard')

    new_n = len(durations) - 2 * to_be_discarded

    # compute trimmed mean (discard a percentage of low/high values)
    durations.sort() # sort numerically
    average_duration = sum(durations[to_be_discarded:-to_be_discarded if to_be_discarded > 0 else len(durations)]) / new_n

    return average_duration

# Extract duration (in ms) from a given Lambda's CloudWatch log.
def extract_duration(log):
    regex = r'\tBilled Duration: (\d+) ms'
    match = re.search(regex, log)
    if match:
        return int(match.group(1))
    return None

def range(n):
    return [i for i in range(n)]

def convert_payload(payload):
    def is_json_string(s):
        if not isinstance(s, str):
            return False

        try:
            json.loads(s)
        except ValueError:
            return False

        return True

    if payload is not None and not is_json_string(payload):
        print('Converting payload to JSON string from ', type(payload))
        payload = json.dumps(payload)

    return payload

def compute_price(min_cost, min_ram, value, duration):
    return math.ceil(duration) * min_cost * (value / min_ram)

def parse_log_and_extract_durations(data):
    return [extract_duration(base64.b64decode(log['LogResult']).decode('utf-8')) for log in data]
    
def generate_payloads(num, payload_input):
    if isinstance(payload_input, list):
        # if array, generate a list of payloads based on weights

        # fail if empty list or missing weight/payload
        if len(payload_input) == 0 or any('weight' not in p or 'payload' not in p for p in payload_input):
            raise ValueError('Invalid weighted payload structure')

        if num < len(payload_input):
            raise ValueError(f'You have {len(payload_input)} payloads and only "num"={num}. Please increase "num".')

        # we use relative weights (not %), so here we compute the total weight
        total = sum(p['weight'] for p in payload_input)

        # generate an array of num items (to be filled)
        payloads = list(range(num))

        # iterate over weighted payloads and fill the array based on relative weight
        done = 0
        for i in range(len(payload_input)):
            p = payload_input[i]
            how_many = math.floor(p['weight'] * num / total)
            if how_many < 1:
                raise ValueError('Invalid payload weight (num is too small)')

            # make sure the last item fills the remaining gap
            if i == len(payload_input) - 1:
                how_many = num - done

            # finally fill the list with howMany items
            for j in range(done, done + how_many):
                payloads[j] = convert_payload(p['payload'])
            done += how_many

        return payloads
    else:
        # if not an array, always use the same payload (still generate a list)
        payloads = [convert_payload(payload_input) for _ in range(num)]
        return payloads

def fetch_payload_from_s3(s3_path):
    print('Fetch payload from S3', s3_path)

    if not isinstance(s3_path, str) or 's3://' not in s3_path:
        raise ValueError('Invalid S3 path, not a string in the format s3://BUCKET/KEY')

    uri = urlparse(s3_path)
    bucket = uri.netloc
    key = uri.path.lstrip('/')

    if not bucket or not key:
        raise ValueError(f'Invalid S3 path: "{s3_path}" (bucket: {bucket}, key: {key})')

    data = _fetch_s3_object(bucket, key)

    try:
        # try to parse into JSON object
        return json.loads(data)
    except json.JSONDecodeError:
        # otherwise return as is
        return data

def _fetch_s3_object(bucket, key):
    s3_client = boto3.client('s3')

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        return data
    except s3_client.exceptions.NoSuchKey:
        raise ValueError(f'No such key: s3://{bucket}/{key}')
    except s3_client.exceptions.NoS3Configuration:
        raise ValueError(
            f'Permission denied when trying to read s3://{bucket}/{key}. ' +
            'You might need to re-deploy the app with the correct payloadS3Bucket parameter.'
        )
def sort_and_encode_stats(stats, base_url):
    stats.sort(key=lambda p: p['power'])

    sizes = [p['power'] for p in stats]
    times = [p['duration'] for p in stats]
    costs = [p['cost'] for p in stats]

    hash = ';'.join([
        urlencode(sizes, np.int16),
        urlencode(times),
        urlencode(costs),
    ])

    if 'AWS_REGION' in os.environ and os.environ['AWS_REGION'].startswith('cn-'):
        base_url += "?currency=CNY"

    return base_url + '#' + hash

def base_cost_for_region(price_map, region):
    if region in price_map:
        return price_map[region]
    print(f'{region} not found in base price map, using default: {price_map["default"]}')
    return price_map['default']

def sleep(sleep_between_runs_ms):
    time.sleep(sleep_between_runs_ms / 1000.0)