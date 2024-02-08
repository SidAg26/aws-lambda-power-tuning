import asyncio
from botocore.exceptions import ClientError
import utils

def lambda_handler(event, context):
    lambda_arn = event['lambdaARN']
    power_values = event['powerValues']

    validate_input(lambda_arn, power_values)

    tasks = [cleanup(lambda_arn, f'RAM{value}') for value in power_values]

    # run everything in parallel and wait until completed
    asyncio.run(asyncio.wait(tasks))

    return 'OK'

def validate_input(lambda_arn, power_values):
    if not lambda_arn:
        raise ValueError('Missing or empty lambdaARN')
    if not power_values:
        raise ValueError('Missing or empty power values')

async def cleanup(lambda_arn, alias):
    try:
        # check if it exists and fetch version ID
        function_version = await utils.get_lambda_alias(lambda_arn, alias)
        # delete both alias and version (could be done in parallel!)
        await utils.delete_lambda_alias(lambda_arn, alias)
        await utils.delete_lambda_version(lambda_arn, function_version)
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            print('OK, even if version/alias was not found')
            print(error)
        else:
            print(error)
            raise error