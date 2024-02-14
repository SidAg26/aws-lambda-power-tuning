import json
import boto3
import utils

def lambda_handler(event, context):
    event = event['Payload']
    lambdaARN = event['lambdaARN']
    powerValues = event['powerValues']
    
    lambdaAlias = ['RAM' + str(i) for i in powerValues]
    
    # fetch architectures from Lambda
    config = [utils.get_lambda_config(lambdaARN, alias) for alias in lambdaAlias]
    architecture = [config[i]['architecture'] for i in range(len(config))]
    isPending = [config[i]['is_pending'] for i in range(len(config))]
    
    return { 'architectures': architecture, 'isPending': isPending }