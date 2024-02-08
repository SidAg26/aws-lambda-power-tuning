# import os
import utils

# default_power_values = os.getenv('defaultPowerValues').split(',')
default_power_values = ['128', '256', '512', '1024', '1536', '3008']    
default_sla_value = 1000 # ms

def lambda_handler(event, context):
    lambda_arn = event['lambdaARN']
    num = event['num']
    sla = extract_sla_value(event)
    power_values = extract_power_values(event)

    validate_input(lambda_arn, num)  # may throw

    # fetch initial $LATEST value so we can reset it later
    initial_power = utils.get_lambda_power(lambda_arn)

    # reminder: configuration updates must run sequentially
    # (otherwise you get a ResourceConflictException)
    for value in power_values:
        alias = 'RAM' + str(value)
        utils.create_power_configuration(lambda_arn, value, alias)

    utils.set_lambda_power(lambda_arn, initial_power)

    return {"powerValues": power_values, "sla": sla}

def extract_sla_value(event):
    sla = event.get('sla')  # could be undefined

    # use default value (defined at deploy-time) if not provided
    if not sla:
        sla = default_sla_value
    else:
        sla = sla.get('value')

    return sla

def extract_power_values(event):
    power_values = event.get('powerValues')  # could be undefined

    # auto-generate all possible values if ALL
    if power_values == 'ALL':
        power_values = utils.all_power_values()

    # use default list of values (defined at deploy-time) if not provided
    if not power_values or len(power_values) == 0:
        power_values = default_power_values

    return power_values

def validate_input(lambda_arn, num):
    if not lambda_arn:
        raise ValueError('Missing or empty lambdaARN')
    if not num or num < 5:
        raise ValueError('Missing num or num below 5')