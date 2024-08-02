import utils
lambda_arn = "arn:aws:lambda:ap-southeast-2:030103857128:function:workbench-chameleon"
power_value = [
    128,
    256,
    384,
    512,
    640,
    768,
    896,
    1024,
    1152,
    1280,
    1408,
    1536,
    1664,
    1792,
    1920,
    2048,
    2176,
    2304,
    2432,
    2560,
    2688,
    2816,
    2944,
    3008
]
lambda_client = utils.lambda_client_from_arn(lambda_arn)
for al in power_value:
    alias = f'RAM{al}' # using Map item selector

    params = {
    'FunctionName': lambda_arn,
    'Name': alias,
    }
    

    try:
        response = lambda_client.get_alias(**params)
        # check if it exists and fetch version ID
        function_version = response['FunctionVersion']
        print(function_version)
        if function_version:
            print(function_version)
            # function_version = function_version['FunctionVersion']
            # delete both alias and version (could be done in parallel!)
            try:
                utils.delete_lambda_alias(lambda_arn, alias)
            except Exception as error:
                print(error)
                continue
            try:
                utils.delete_lambda_version(lambda_arn, function_version)
            except Exception as error:
                print(error)
                continue
        else:
            continue
    except Exception as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            print('OK, even if version/alias was not found')
            print(error)
        else:
            print(error)
            raise error