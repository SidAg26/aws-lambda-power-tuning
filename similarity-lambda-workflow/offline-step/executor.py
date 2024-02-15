import os
import utils
import concurrent.futures
import asyncio

minRAM = int(os.getenv('minRAM', '128'))  # default to 128MB

def lambda_handler(event, context):
    # read input from event
    # data = extract_data_from_input(event)
    lambdaARN = event['lambdaARN']
    value = event['value'] if 'value' in event else None # this is the input value
    # if payloadS3 is present, it will be used to fetch the payload
    value = event['payloads3'] if 'payloadS3' in event else value 
    num = event['num']
    powerValues = event['powerValues']
    dryRun = event['dryRun'] if 'dryRun' in event else False
    # default to enable parallel invocation
    enableParallel = event['enableParallel'] if 'enableParallel' in event else True
    disablePayloadLogs = event['disablePayloadLogs'] if 'disablePayloadLogs' in event else False
    sleepBetweenRunsMs = event['sleepBetweenRunsMs'] if 'sleepBetweenRunsMs' in event else 0
    architecture = event['architectures'] if 'architectures' in event else "x86_64"
    isPending = event['isPending'] if 'isPending' in event else False
    # payload = data['payload']
    # preProcessorARN = data['preProcessorARN']
    # postProcessorARN = data['postProcessorARN']
    # discardTopBottom = data['discardTopBottom']
    
    

    validate_input(lambdaARN, value, num, powerValues)  # may throw

    # force only 1 execution if dryRun
    if dryRun:
        print('[Dry-run] forcing num=1')
        num = 1

    # generate Lambda aliases from all powerValues
    lambdaAlias = ['RAM' + str(i) for i in powerValues]
    results = None

    # # fetch architectures from Lambda
    # config = [utils.get_lambda_config(lambdaARN, alias) for alias in lambdaAlias]
    # architecture = [config[i]['architecture'] for i in range(len(config))]
    # isPending = [config[i]['is_pending'] for i in range(len(config))]
    print(f'Detected architecture type: {architecture}, isPending: {isPending}')

    # # pre-generate an array of N payloads
    # payloads = utils.generate_payloads(num, payload)
    # run_input = {
    #     'num': num,
    #     'lambdaARN': lambdaARN,
    #     'lambdaAlias': lambdaAlias,
    #     'payloads': payloads,
    #     'preARN': preProcessorARN,
    #     'postARN': postProcessorARN,
    #     'sleepBetweenRunsMs': sleepBetweenRunsMs,
    #     'disablePayloadLogs': disablePayloadLogs,
    # }

    # wait if the function/alias state is Pending
    if False in isPending:
        isPending = [utils.wait_for_alias_active(lambdaARN, alias) for alias in lambdaAlias]
    # print('Alias active')

    if enableParallel:
        results = run_in_parallel(num, lambdaARN, lambdaAlias, value, disablePayloadLogs)
    else:
        results = run_in_series(num, lambdaARN, lambdaAlias, value, sleepBetweenRunsMs, disablePayloadLogs)
    # for input=value results=[{value, alias1}, {value, alias2}...]
    # put the results in DynamoDB table to access later and create similarity model
        
    # get base cost for Lambda
    base_cost = utils.lambda_base_cost(utils.region_from_arn(lambdaARN), architecture[0])
    # base_cost= {'AWS-Lambda-Duration': 0.0000166667, 'AWS-Lambda-Requests': 0.20}

    # return results
    return compute_statistics(base_cost=base_cost, results=results, input_value=value)


def validate_input(lambdaARN, value, num, powerValues):
    if not lambdaARN:
        raise ValueError('Missing or empty lambdaARN')
    if not value or not isinstance(value, (int, float)):
        raise ValueError('Invalid value: ' + str(value))
    if not num or not isinstance(num, int):
        raise ValueError('Invalid num: ' + str(num))
    if not powerValues or not isinstance(powerValues, list):
        raise ValueError('Invalid powerValues: ' + str(powerValues))
    
# def extract_payload_value(input):
#     if 'payloadS3' in input:
#         return utils.fetch_payload_from_s3(input['payloadS3'])  # might throw if access denied or 404
#     elif 'value' in input:
#         return input['value']
#     return None

def extract_discard_top_bottom_value(event):
    # extract discardTopBottom used to trim values from average duration
    discard_top_bottom = event.get('discardTopBottom', 0.2)
    # discardTopBottom must be between 0 and 0.4
    return min(max(discard_top_bottom, 0.0), 0.4)

# def extract_sleep_time(event):
#     sleep_between_runs_ms = event.get('sleepBetweenRunsMs', 0)
#     if not isinstance(sleep_between_runs_ms, int):
#         sleep_between_runs_ms = 0
#     else:
#         sleep_between_runs_ms = int(sleep_between_runs_ms)
#     return sleep_between_runs_ms

# def extract_data_from_input(event):
#     # input = event['input']  # original state machine input
#     payload = extract_payload_value(input)
#     discard_top_bottom = extract_discard_top_bottom_value(input)
#     sleep_between_runs_ms = extract_sleep_time(input)
#     return {
#         'value': int(event['value']),
#         'lambdaARN': input['lambdaARN'],
#         'num': int(input['num']),
#         'powerValues': input['powerValues'],
#         'enableParallel': bool(input.get('parallelInvocation', False)),
#         'payload': payload,
#         'dryRun': input.get('dryRun', False),
#     }

def run_in_parallel(num, lambdaARN, lambdaAlias, payloads, disablePayloadLogs):
    results = []
    for _ in range(0, num):
        # Use a ThreadPoolExecutor to run all invocations in parallel ...
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(invoke_lambda, lambdaARN, alias, payloads, disablePayloadLogs) for alias in lambdaAlias]
        # ... and wait for results
        # Collect the responses from each Lambda function invocation
        # responses = [future.result() for future in futures]
        for future in futures:
            results.append(future.result())
        # results.append(responses)
    return results

def invoke_lambda(lambdaARN, lambdaAlias, payloads, disablePayloadLogs):
    # results = []
    result = utils.invoke_lambda_with_processors(lambdaARN, lambdaAlias, payloads, disablePayloadLogs)
    actual_payload = result['actualPayload']
    invocation_results = result['invocationResults']
    function_error = True if invocation_results['FunctionError'] is not None else False
    # invocation errors return 200 and contain FunctionError and Payload
    if function_error:
        error_message = f"Invocation error (running in parallel): {invocation_results['Payload']}"
        if not disablePayloadLogs:
            error_message += f" with payload {actual_payload}"
        raise Exception(error_message)
    # results.append(invocation_results)
    # print('Invocation results: ', invocation_results)
    return invocation_results

def run_in_series(num, lambdaARN, lambdaAlias, payloads, sleep_between_runs_ms, disablePayloadLogs):
    results = []
    for i in range(num):
        # run invocations in series
        invocation_results, actual_payload = utils.invoke_lambda_with_processors(lambdaARN, lambdaAlias, payloads, disablePayloadLogs)
        # invocation errors return 200 and contain FunctionError and Payload
        if 'FunctionError' in invocation_results:
            error_message = f"Invocation error (running in series): {invocation_results['Payload']}"
            if not disablePayloadLogs:
                error_message += f" with payload {actual_payload}"
            raise Exception(error_message)
        if sleep_between_runs_ms > 0:
            asyncio.sleep(sleep_between_runs_ms / 1000)  # asyncio.sleep expects seconds, so we convert ms to s
        results.append(invocation_results)
    return results

def compute_statistics(base_cost, results, input_value, discard_top_bottom=0):
    # use results (which include logs) to compute average duration ...
    log_results = utils.parse_logs(results) # this list has all the 'powerValues' for an input 'value' for 'num' times

    average_duration = utils.compute_average_duration(log_results, discard_top_bottom) # returns average duration at powerValues
    print('Average duration: ', average_duration)

    # ... and overall statistics
    average_price = {}
    for key in average_duration.keys():
        print(f'PowerValue: {key}, AverageDuration: {average_duration[key]}')
        average_price[key] = utils.compute_price(base_cost, minRAM, int(key), average_duration[key])

    # .. and total cost (exact $)
    total_cost = utils.compute_total_cost(base_cost, minRAM, log_results)

    stats = {
        'averagePrice': average_price,
        'averageDuration': average_duration,
        'totalCost': total_cost,
        'value': input_value,
    }

    print('Stats: ', stats)
    return stats
