# import asyncio
from botocore.exceptions import ClientError
import utils
import concurrent.futures

def lambda_handler(event, context):
    lambda_arn = event['lambdaARN']
    power_value = event['powerValues'] 
    validate_input(lambda_arn, power_value)
    lambda_alias = [f'RAM{power}' for power in power_value] # using Map item selector

    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(cleanup_on_error, lambda_arn, alias) for alias in lambda_alias]
    for future in futures:
        results.append(future.result())
    
    return results

def cleanup_on_error(lambda_arn, alias):
    try:
        # check if it exists and fetch version ID
        function_version = utils.get_lambda_alias(lambda_arn, alias)['FunctionVersion']
        # delete both alias and version (could be done in parallel!)
        utils.delete_lambda_alias(lambda_arn, alias)
        utils.delete_lambda_version(lambda_arn, function_version)
        return 'OK'
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            print('OK, even if version/alias was not found')
            print(error)
        else:
            print(error)
            raise error

def validate_input(lambda_arn, power_value):
    if not lambda_arn:
        raise ValueError('Missing or empty lambdaARN')
    if not power_value:
        raise ValueError('Missing or empty power values')
