import boto3

client = boto3.client('pricing', region_name='us-east-1')
res = client.get_attribute_values(
ServiceCode='AWSLambda',
AttributeName='productFamily',
MaxResults=12
)